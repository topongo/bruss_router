#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use bruss_data::{BrussType, Coords, Path, Segment, StopPair, AreaHelper, Stop};
use log::{debug, info};
use tokio::task::JoinSet;
use tt::AreaType;
use mongodb::bson::doc;
use futures::{StreamExt,TryStreamExt};
use bruss_router::{CONFIGS, osrm::calculate_geometry};

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

    info!("connecting to database...");
    let db = mongodb::Client::with_options(CONFIGS.db.gen_mongodb_options()).expect("error creating mongodb client").database(CONFIGS.db.get_db());

    info!("getting paths...");
    let paths: HashMap<String, Path> = Path::get_coll(&db)
        .find(doc!{}, None)
        .await?
        .map(|r| r.map(|p| (p.id.clone(), p)))
        .try_collect()
        .await?;
    info!("done! got {} paths", paths.len());

    info!("getting segments...");
    let segments: HashSet<(AreaType, StopPair)> = Segment::get_coll(&db)
        .find(doc!{}, None)
        .await?
        .map(|r| r.map(|s| (s.ty, (s.from, s.to))))
        .try_collect()
        .await?;
    info!("done! got {} segments", segments.len());

    info!("getting stops from db...");
    let mut stops: AreaHelper<Stop> = AreaHelper::new();
    let mut cursor = Stop::get_coll(&db)
        .find(doc!{}, None)
        .await?;
    while let Some(s) = cursor.try_next().await? {
        stops.insert(s);
    }
    info!("done! got {} stops from db", stops.len());
   
    info!("checking for missing segments...");
    let mut missing_segments: HashSet<(AreaType, StopPair)> = HashSet::new();
    for p in paths.values() {
        for s in p.segments() {
            if !segments.contains(&(p.ty, s)) {
                debug!("missing segment: {:?}", (p.ty, s));
                missing_segments.insert((p.ty, s));
            }
        }
    }
    info!("done! there are {} missing segments", missing_segments.len());

    let mut tasks: JoinSet<_> = JoinSet::new();

    for (t, sp) in missing_segments {
        let s0 = stops.get(t).get(&sp.0).unwrap().clone();
        let s1 = stops.get(t).get(&sp.1).unwrap().clone();
        tasks.spawn(calculate_geometry(t, s0, s1, None));
    }
    info!("started {} tasks for geometry processing", tasks.len());

    info!("gathering tasks...");
    let mut pending_segments = Vec::<Segment>::new();
    while let Some(uuh) = tasks.join_next().await {
        let ((t, (s0, s1)), c) = uuh??;
        pending_segments.push(Segment::new(s0, s1, t, c));
    }
    info!("done!");

    if pending_segments.len() > 0 {
        info!("inserting {} missing segments into db...", pending_segments.len());
        Segment::get_coll(&db)
            .insert_many(pending_segments, None)
            .await?;
        info!("done!")
    }

    Ok(())
}

