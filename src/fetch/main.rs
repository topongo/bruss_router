#![feature(iter_intersperse)]

use std::collections::{HashMap, HashSet};
use futures::{StreamExt, TryStreamExt};

use mongodb::bson::doc;
use mongodb::options::FindOptions;
use tt::{AreaType, RequestOptions, TTRoute, TTTrip, TripQuery};
use chrono::{Local, Duration};
use bruss_data::{FromTT, Segment, Path, Trip, BrussType, sequence_hash, Route};
use log::{info,debug,warn};
use bruss_router::CONFIGS;

#[tokio::main]
async fn main() -> Result<(), Box<(dyn std::error::Error + 'static)>> {
    env_logger::init(); 

    info!("getting tt client");
    let tt_client = CONFIGS.tt.client();

    info!("connecting to mongodb database");
    let db = mongodb::Client::with_options(CONFIGS.db.gen_mongodb_options())?.database(CONFIGS.db.get_db());

    info!("getting path ids...");
    let path_ids: HashSet<String> = db.collection::<String>("paths")
        .find(doc!{}, FindOptions::builder().projection(doc!{"id": 1, "_id": 0}).build())
        .await?
        .try_collect()
        .await?;
    info!("done! got {} paths", path_ids.len());

    info!("getting segment ids...");
    let segment_ids: HashSet<(AreaType, u16, u16)> = Segment::get_coll(&db)
        .find(doc!{}, None)
        .await?
        .map(|r| r.map(|s| (s.ty, s.from, s.to)))
        .try_collect()
        .await?;
    info!("done! got {} segments", segment_ids.len());

    info!("getting trip ids...");
    let trip_ids: HashSet<String> = db.collection::<String>("trips")
        .find(doc!{}, FindOptions::builder().projection(doc!{"id": 1, "_id": 0}).build())
        .await?
        .try_collect()
        .await?;
    info!("done! got {} segments", trip_ids.len());

    let routes_c = Route::get_coll(&db);
    info!("getting routes from db...");
    let routes = if routes_c.count_documents(doc!{}, None).await.unwrap() == 0 {
        warn!("no routes in db, getting them from tt...");
        let routes_tt = tt_client.request::<TTRoute>().await.unwrap();
        info!("got {} routes from tt", routes_tt.len());
        let mut routes_bruss = Vec::with_capacity(routes_tt.len());
        for rt in routes_tt {
            routes_bruss.push(Route::from_tt(rt));
        }
        info!("inserting fetched routes into db");
        routes_c.insert_many(&routes_bruss, None).await.expect("couln't insert routes into database");
        info!("done!");
        routes_bruss
    } else {
        routes_c 
            .find(doc!{}, None)
            .await?
            .try_collect()
            .await?
    };
    info!("done, got {} from database/tt", routes.len());

    info!("getting trips from tt...");
    let time = (Local::now() + Duration::hours(5) + Duration::days(1)).naive_local();
    debug!("using start time: {}", time);
    let mut trips_tt: Vec<TTTrip> = Vec::new(); 
    for r in routes {
        info!("getting route {}...", r.id);
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
        info!("got {} trips for route {}", ts.len(), r.id);
        trips_tt.append(&mut ts);
    }
    info!("done! got {} trips from tt", trips_tt.len());

    let mut paths_missing: HashMap<String, Path> = HashMap::new();
    info!("converting trips into bruss type...");
    let mut trips = Vec::<Trip>::new();
    for t in trips_tt {
        let seq: Vec<u16> = t.stop_times.iter().map(|st| st.stop).collect();
        let h = sequence_hash(t.ty, &seq);
        if !path_ids.contains(&h) && paths_missing.contains_key(&h) {
            paths_missing.insert(h.clone(), Path::new(seq, t.ty));
        }
        if !trip_ids.contains(&t.id) {
            trips.push(Trip::from_tt(t));
        }
    }
    info!("done! converted {} missing trips, collected {} missing paths", trips.len(), paths_missing.len());

    info!("inserting missing trips...");
    Trip::get_coll(&db).insert_many(trips, None).await?;
    info!("done");

    info!("inserting missing paths...");
    Path::get_coll(&db).insert_many(paths_missing.values(), None).await?;

    Ok(())
}
