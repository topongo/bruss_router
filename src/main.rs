#![feature(iter_intersperse)]

mod osrm;

use std::{collections::{HashMap, HashSet}, future::Future, sync::Arc, hash::Hash};
use futures::{StreamExt, TryStreamExt};
use osrm::OsrmResponse;

use reqwest::RequestBuilder;
use mongodb::bson::doc;
use mongodb::options::FindOptions;
use tt::{RequestOptions, StopTime, TTStop, TTTrip, TripQuery, AreaType};
use chrono::{Local, Duration};
use bruss_config::BrussConfig;
use lazy_static::lazy_static;
use bruss_data::{FromTT, Stop, Coords, Segment, Path, Trip, BrussType, AreaHelper, StopPair, sequence_hash, Route};
use log::{info,debug,warn};

lazy_static! {
    pub static ref CONFIGS: BrussConfig = BrussConfig::from_file("bruss.toml").expect("cannot load configs");
}

fn url_builder(start: &Coords, end: &Coords) -> String {
    format!(
        "http://{}:{}/{}/{};{}?geometries=polyline&overview=false&steps=true&alternatives=false",
        CONFIGS.routing.host,
        CONFIGS.routing.port.unwrap_or(80),
        CONFIGS.routing.url,
        start.to_osrm_query(),
        end.to_osrm_query()
    )
}


async fn calculate_geometry(s1: Stop, s2: Stop, client: Option<reqwest::Client>) -> Result<Vec<Coords>, reqwest::Error> {
    let client = client.unwrap_or(reqwest::Client::new());
    let res = client.get(url_builder(&s1.position, &s2.position))
        .send()
        .await?
        .json::<OsrmResponse>()
        // .text()
        .await?;

    Ok(res.flatten())
    // println!("{}", res);
    // todo!();
}

fn segment_to_geojson(input: &Segment) -> String {
    serde_json::to_string(&input.coords)
        // won't fail
        .unwrap()
}

fn path_to_geojson(input: &Path, segments: HashMap<(StopPair, AreaType), Segment>) -> String {
    serde_json::to_string(&input.segments().iter()
            .flat_map(|s| &segments.get(&(*s, input.ty)).unwrap().coords)
            .collect::<Vec<&Coords>>()
        )
        .unwrap()
}

#[tokio::main]
async fn main() -> Result<(), Box<(dyn std::error::Error + 'static)>> {
    env_logger::init(); 

    // create tt client
    info!("getting tt client");
    let tt_client = CONFIGS.tt.client();

    let time = (Local::now() + Duration::hours(5) + Duration::days(1)).naive_local();
    // let mut ts = tt_client
    //     .request_opt::<TTTrip, TripQuery>(Some(RequestOptions::new().query(TripQuery { 
    //         ty: AreaType::U,
    //         // 5/: 535
    //         // 3: 396
    //         route_id: 408,
    //         limit: 100,
    //         time 
    //     })))
    //     .await?;
    //
    // // todo!();

    // connect to db
    info!("connecting to mongodb database");
    let db = mongodb::Client::with_options(CONFIGS.db.gen_mongodb_options())?.database(CONFIGS.db.get_db());
   
    let mut stops = AreaHelper::new();
    for s in tt_client
        .request::<TTStop>()
        .await?
    {
        stops.insert(Stop::from_tt(s));
    }

    info!("getting cached segments...");
    let seg_c = db.collection::<Segment>("segments");
    let segs: HashMap<(u16, u16), Vec<Coords>> = seg_c.find(doc!{}, None).await?
        .map(|r| r.map(|s| ((s.from, s.to), s.coords)))
        .try_collect().await.unwrap();

    // println!("{}", serde_json::to_string(&segs.values().collect::<Vec<&Vec<Coords>>>()).unwrap());
    info!("done! there are {} segments cached", segs.len());

    info!("getting cached paths...");
    let path_c = Path::get_coll(&db);
    // let paths: HashMap<Uuid, Path> = path_c.find(doc!{ }, None).await?
    let paths: Vec<Path> = path_c.find(doc!{ }, None).await?
        // .map(|r| r.map(|p| (p.id, p)))
        .try_collect().await?;
    info!("done! there are {} paths cached", paths.len());

    let paths_in_db: HashSet<String> = paths.iter()
        .map(|p| sequence_hash(p.ty, &p.sequence))
        .collect();

    let trips_in_db: HashSet<String> = db.collection::<String>("trips").find(
        doc!{},
        Some(FindOptions::builder().projection(doc!{"id": 1, "_id": 0}).build())
    ).await?
        .try_collect()
        .await?;


    let routes: Vec<Route> = Route::get_coll(&db)
        .find(doc!{}, None)
        .await?
        .try_collect()
        .await?;

    info!("getting trips from tt...");
    let time = (Local::now() + Duration::hours(5) + Duration::days(1)).naive_local();
    debug!("using start time: {}", time);
    let mut trips_tt: Vec<TTTrip> = Vec::new(); 
    for r in routes {
        info!("getting route {}", r.id);
        let mut ts = tt_client
            .request_opt::<TTTrip, TripQuery>(Some(RequestOptions::new().query(TripQuery { 
                ty: AreaType::U,
                // 5/: 535
                // 3: 396
                route_id: r.id,
                limit: 100,
                time 
            })))
            .await?;
        info!("got {} trips for route {:?}", ts.len(), r.id);
        trips_tt.append(&mut ts);
    }
    info!("done! got {} trips", trips_tt.len());

    info!("converting trips into bruss type...");
    let mut trips = Vec::<Trip>::new();
    for t in trips_tt {
        trips.push(Trip::from_tt(t));
    }
    info!("done!");
    

    let mut segments_fut = HashMap::<(StopPair, AreaType), _>::new();
    let mut paths_pending = HashMap::<String, (AreaType, Vec<StopPair>)>::new();

    for t in trips {
        if !trips_in_db.contains(&t.id) {
            let stops_inarea = stops.get(t.ty);
            // info!("trip {} is not in database", t.id);
            // trip is not in database, processing it.
            let p_hash = sequence_hash(t.ty, &t.path);
            debug!("\tpath sequence: {:?}", t.path);
            info!("path hash is: {}", p_hash);
            if !paths_pending.contains_key(&p_hash) && !paths_in_db.contains(&p_hash) {
                info!("path of trip {} is not in database, importing it...", t.id);
                let mut path_sequence = vec![];
                // let path_sequence = &mut paths_pending.get_mut(&p_hash).unwrap().1;

                if t.path.len() == 0 {
                    panic!("path length is 0")
                }
                // let prev_n = -1;
                let mut prev = &t.path[0];
                for s in t.path.iter().skip(1) {
                    let key = (*prev, *s);
                    path_sequence.push(key);
                    let key_a = (key, t.ty);
                    let p_s = stops_inarea.get(&prev).unwrap().clone();
                    let c_s = stops_inarea.get(&s).unwrap().clone();
                    debug!("distance between stops_inarea: {}", p_s.position - c_s.position);
                    let names = (p_s.name.clone(), c_s.name.clone());
                    if segs.contains_key(&key) {
                        info!("\tsegment in db, skipping    {:?}", names);
                    } else if segments_fut.contains_key(&key_a) {
                        info!("\tsegment pending, skipping  {:?}", names);
                    } else {
                        let p_s = stops_inarea.get(&prev).unwrap().clone();
                        let c_s = stops_inarea.get(&s).unwrap().clone();
                        info!("\tunknown segment, appending {:?}", names);
                        segments_fut.insert(
                            key_a,
                            calculate_geometry(p_s, c_s, None)
                        );
                    }
                    prev = s;
                }
                paths_pending.insert(p_hash.clone(), (t.ty, path_sequence));
            }
        }
    }
    info!("discovered {} paths from tt", paths_pending.len()); 
    let mut handles = HashMap::with_capacity(segments_fut.len());

    for (k, fut) in segments_fut {
        handles.insert(k, tokio::spawn(fut));
    }

    info!("fetching {} segments from routing server...", handles.len());
    let mut segments = HashMap::<(StopPair, AreaType), Segment>::new();
    for ((s, t), handle) in handles {
        segments.insert((s, t), Segment::new(s.0, s.1, t, handle.await.unwrap().unwrap()));
    }

    if segments.len() > 0 {
        info!("inserting {} segments info db...", segments.len());
        seg_c.insert_many(segments.values(), None).await.expect("cannot insert new segments into database");
    } else {
        info!("no segments to insert");
    }

    let mut paths_pending_db = Vec::<Path>::new();
    for (_, (ty, seg)) in paths_pending {
        let p = Path::new_from_segments(seg, ty);
        paths_pending_db.push(p);
    }

    if paths_pending_db.len() > 0 {
        info!("inserting {} paths into db...", paths_pending_db.len());
        let res = path_c.insert_many(paths_pending_db, None)
            .await
            .expect("cannot insert new paths into database");
        debug!("result of insertion: {:?}", res);
    } else {
        info!("no paths to insert");
    }

    Ok(())
}
