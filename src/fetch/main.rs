#![feature(iter_intersperse)]

use std::collections::{HashMap, HashSet};
use bruss_config::CONFIGS;
use futures::{StreamExt, TryStreamExt};

use mongodb::bson::doc;
use mongodb::options::FindOptions;
use tt::{AreaType, RequestOptions, TTRoute, TTTrip, TripQuery, TTStop};
use chrono::{Local, NaiveTime};
use bruss_data::{FromTT, Segment, Path, Trip, BrussType, sequence_hash, Route, Stop};
use log::{info,debug,warn,error};
use serde::Deserialize;

#[derive(Deserialize)]
struct IdDocument<T> {
    id: T
}

#[tokio::main]
async fn main() -> Result<(), Box<(dyn std::error::Error + 'static)>> {
    env_logger::init(); 

    info!("getting tt client");
    let tt_client = CONFIGS.tt.client();

    info!("connecting to mongodb database");
    let db = mongodb::Client::with_options(CONFIGS.db.gen_mongodb_options())?.database(CONFIGS.db.get_db());

    info!("getting stop ids...");
    let stop_ids: HashSet<u16> = Stop::get_coll(&db)
        .find(doc!{}, None)
        .await?
        .map(|r| r.map(|s| s.id))
        .try_collect()
        .await?;
    info!("done! got {} stops", stop_ids.len());

    info!("getting stops from tt...");
    let stops_tt: Vec<TTStop> = tt_client.request().await?;
    info!("done! got {} stops", stops_tt.len());
    let mut stops_missing: Vec<Stop> = Vec::new();
    for s in stops_tt {
        debug!("{:?}", s);
        if !stop_ids.contains(&s.id) {
            stops_missing.push(Stop::from_tt(s));
        }
    }
    if stops_missing.len() > 0 {
        info!("inserting {} missing stops in db...", stops_missing.len());
        Stop::get_coll(&db).insert_many(stops_missing, None).await?;
        info!("done!");
    }

    info!("getting path ids...");
    let path_ids: HashSet<String> = db.collection::<IdDocument<String>>("paths")
        .find(doc!{}, FindOptions::builder().projection(doc!{"id": 1, "_id": 0}).build())
        .await?
        .map(|r| r.map(|d| d.id))
        .try_collect()
        .await?;

    info!("getting segment ids...");
    let segment_ids: HashSet<(AreaType, u16, u16)> = Segment::get_coll(&db)
        .find(doc!{}, None)
        .await?
        .map(|r| r.map(|s| (s.ty, s.from, s.to)))
        .try_collect()
        .await?;
    info!("done! got {} segments", segment_ids.len());

    info!("getting trip ids...");
    let trip_ids: HashSet<String> = db.collection::<IdDocument<String>>("trips")
        .find(doc!{}, FindOptions::builder().projection(doc!{"id": 1, "_id": 0}).build())
        .await?
        .map(|r| r.map(|d| d.id))
        .try_collect()
        .await?;

    info!("done! got {} trips", trip_ids.len());

    let routes_c = Route::get_coll(&db);
    info!("getting routes from db...");
    let routes = if routes_c.count_documents(doc!{}, None).await.unwrap() == 0 {
        warn!("no routes in db, getting them from tt...");
        let routes_tt = tt_client.request::<TTRoute>().await.unwrap();
        info!("got {} routes from tt", routes_tt.len());
        let mut routes_bruss = HashMap::with_capacity(routes_tt.len());
        for rt in routes_tt {
            routes_bruss.insert(rt.id, Route::from_tt(rt));
        }
        if routes_bruss.len() > 0 {
            info!("inserting fetched routes into db");
            routes_c.insert_many(routes_bruss.values(), None).await.expect("couln't insert routes into database");
            info!("done!");
        }
        routes_bruss
    } else {
        routes_c 
            .find(doc!{}, None)
            .await?
            .map(|r| r.map(|ro| (ro.id, ro)))
            .try_collect()
            .await?
    };
    info!("done, got {} from database/tt", routes.len());

    let mut paths_missing: HashMap<String, Path> = HashMap::new();

    info!("getting trips from database...");
    let mut trips: Vec<Trip> = Trip::get_coll(&db)
        .find(doc!{}, None)
        .await?
        .try_collect()
        .await?;
    info!("done! got {} trips from db", trips.len());

    if CONFIGS.routing.get_trips {
        info!("getting trips from tt...");
        let time = Local::now().date_naive().and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap());
        debug!("using start time: {}", time);
        let mut trips_tt: Vec<TTTrip> = Vec::new(); 
        for (n, r) in routes.values().enumerate() {
            info!("getting route {} ({:?}) [{:3}/{:3}]...", r.id, r.area_ty, n, routes.len());
            let mut ts = match tt_client
                .request_opt::<TTTrip, TripQuery>(Some(RequestOptions::new().query(TripQuery { 
                    ty: r.area_ty,
                    // 5/: 535
                    // 3: 396
                    route_id: r.id,
                    limit: 1024,
                    time 
                })))
                .await
            {
                Ok(v) => v,
                Err(e) => { 
                    if CONFIGS.routing.exit_on_err {
                        return Err(Box::new(e) as Box<dyn std::error::Error>)
                    } else {
                        error!("skipped route {} ({:?}): {:?}", r.id, r.area_ty, e) 
                    }
                    continue
                }
            };
            info!("got {} trips for route {}", ts.len(), r.id);
            trips_tt.append(&mut ts);
        }
        info!("done! got {} trips from tt", trips_tt.len());

        info!("converting trips into bruss type...");
        for t in trips_tt {
            let seq: Vec<u16> = t.stop_times.iter().map(|st| st.stop).collect();
            let route = routes.get(&t.route).unwrap();
            let h = sequence_hash(t.ty, &seq);
            if !path_ids.contains(&h) && !paths_missing.contains_key(&h) {
                paths_missing.insert(h.clone(), Path::new(seq, t.ty, route.into()));
            }
            if !trip_ids.contains(&t.id) {
                trips.push(Trip::from_tt(t));
            }
        }

        if trips.len() > 0 {
            info!("inserting {} missing trips...", trips.len());
            Trip::get_coll(&db).insert_many(
                trips.iter()
                    .filter(|t| !trip_ids.contains(&t.id))
                , None).await?;
            info!("done!");
        }
        info!("done! converted {} missing trips, collected {} missing paths", trips.len(), paths_missing.len());
    } 

    if paths_missing.len() > 0 {
        info!("inserting {} missing paths...", paths_missing.len());
        Path::get_coll(&db).insert_many(paths_missing.values(), None).await?;
        info!("done!")
    }

    Ok(())
}
