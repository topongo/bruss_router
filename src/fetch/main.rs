#![feature(iter_intersperse)]

use std::collections::{HashMap, HashSet};
use bruss_config::CONFIGS;
use futures::{StreamExt, TryStreamExt};

use mongodb::bson::doc;
use mongodb::options::FindOptions;
use tt::{AreaType, RequestOptions, TTArea, TTRoute, TTStop, TTTrip, TripQuery};
use chrono::{Local, NaiveTime};
use bruss_data::{sequence_hash, Area, BrussType, FromTT, Path, Route, Segment, Stop, Trip};
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

    info!("getting area ids...");
    let area_ids: HashSet<u16> = Area::get_coll(&db)
        .find(doc!{}, None)
        .await?
        .map(|r| r.map(|a| a.id))
        .try_collect()
        .await?;
    info!("done! got {} areas", area_ids.len());

    info!("getting areas from tt...");
    let areas_tt: Vec<TTArea> = tt_client.request().await?;
    info!("done! got {} areas", areas_tt.len());
    let mut areas_missing: Vec<Area> = Vec::new();
    for s in areas_tt {
        debug!("{:?}", s);
        if !area_ids.contains(&s.id) {
            areas_missing.push(Area::from_tt(s));
        }
    }
    if !areas_missing.is_empty() {
        info!("inserting {} missing areas in db...", areas_missing.len());
        Area::get_coll(&db).insert_many(areas_missing, None).await?;
        info!("done!");
    }


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
    if !stops_missing.is_empty() {
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
    let trips: HashMap<String, Trip> = Trip::get_coll(&db)
        .find(doc!{}, None)
        .await?
        .map(|r| r.map(|d| (d.id.clone(), d)))
        .try_collect()
        .await?;

    info!("done! got {} trips", trips.len());

    let routes_c = Route::get_coll(&db);
    info!("getting routes...");
    debug!("there are {} routes in db", routes_c.count_documents(doc!{}, None).await.unwrap());
    let routes = if routes_c.count_documents(doc!{}, None).await.unwrap() == 0 {
        warn!("no routes in db, getting them from tt...");
        let routes_tt = tt_client.request::<TTRoute>().await.unwrap();
        info!("got {} routes from tt", routes_tt.len());
        let mut routes_bruss = HashMap::with_capacity(routes_tt.len());
        for rt in routes_tt {
            routes_bruss.insert(rt.id, Route::from_tt(rt));
        }
        if !routes_bruss.is_empty() {
            info!("inserting fetched routes into db");
            routes_c.insert_many(routes_bruss.values(), None).await.expect("couln't insert routes into database");
            info!("done!");
        }
        routes_bruss
    } else {
        info!("getting routes from db...");
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
    let mut trips_tt: Vec<Trip> = Trip::get_coll(&db)
        .find(doc!{}, None)
        .await?
        .try_collect()
        .await?;
    info!("done! got {} trips from db", trips_tt.len());

    if CONFIGS.routing.get_trips {
        info!("getting trips from tt...");
        let time = Local::now().date_naive().and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap());
        debug!("using start time: {}", time);
        let mut trips_tt_inner: Vec<TTTrip> = Vec::new(); 
        for (n, r) in routes.values().enumerate() {
            info!("getting route {} ({:?}) [{:3}/{:3}]...", r.id, r.area_ty, n, routes.len());
            let mut ts = match tt_client
                .request_opt::<TTTrip, TripQuery>(Some(RequestOptions::new("/trips_new").query(TripQuery { 
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
            trips_tt_inner.append(&mut ts);
        }
        info!("done! got {} trips from tt", trips_tt_inner.len());

        info!("converting trips into bruss type...");
        for t in trips_tt_inner {
            let seq: Vec<u16> = t.stop_times.iter().map(|st| st.stop).collect();
            let route = routes.get(&t.route).unwrap();
            let h = sequence_hash(t.ty, &seq);
            if !path_ids.contains(&h) && !paths_missing.contains_key(&h) {
                paths_missing.insert(h.clone(), Path::new(seq, t.ty, route.into()));
            }
            if !trips.contains_key(&t.id) {
                trips_tt.push(Trip::from_tt(t));
            }
        }

        info!("done! converted {} trips, collected {} missing paths", trips.len(), paths_missing.len());
    } 

    if !paths_missing.is_empty() {
        info!("inserting {} missing paths...", paths_missing.len());
        Path::get_coll(&db).insert_many(paths_missing.values(), None).await?;
        info!("done!")
    }

    if CONFIGS.routing.get_trips {
        let missing_trips = trips_tt.iter().filter(|t| !trips.contains_key(&t.id)).collect::<Vec<_>>();
        if !missing_trips.is_empty() {
            info!("inserting {} missing trips...", missing_trips.len());
            Trip::get_coll(&db).insert_many(missing_trips, None).await?;
        }
        let _ = if CONFIGS.routing.deep_trip_check {
            info!("checking for differences in trips...");
            let mut c = 0;
            for t in &trips_tt {
                if !trips.contains_key(&t.id) {
                    // we just inserted this trip, skip it
                    continue
                }
                // we are sure that the trip is in the database, cause we skipped the ones that
                // aren't
                if trips.get(&t.id).unwrap().deep_cmp(&t) {
                    debug!("trip {} is the same on db and on tt", t.id);
                } else {
                    warn!("trip {} is different on db and on tt", t.id);
                    c += 1;
                }
            }
            if c > 0 {
                warn!("{} trips are different on db and on tt", c);
            } else {
                info!("all trips are the same on db and on tt");
            }
        // } else {
        //     trips_tt.iter().filter(|t| !trips.contains_key(&t.id))
        };

        if trips_tt.iter().filter(|t| !trips.contains_key(&t.id)).count() > 0 {
            info!("inserting {} missing trips...", trips_tt.len());
            Trip::get_coll(&db).insert_many(
                trips_tt.iter()
                    .filter(|t| !trips.contains_key(&t.id))
                , None).await?;
            info!("done!");
        } else {
            info!("no trips inserted")
        }
    }



    Ok(())
}
