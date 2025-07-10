#![allow(dead_code)]

use std::{collections::{HashMap, HashSet}, sync::Arc};
use bruss_data::{AreaHelper, BrussType, Coords, Path, RoutingType, Segment, Stop, StopPair};
use log::{debug, info};
use tokio::task::JoinSet;
use tt::AreaType;
use mongodb::bson::doc;
use futures::{StreamExt,TryStreamExt};
use bruss_router::{CONFIGS, osrm::calculate_geometry};

static MAX_PARALLEL_REQUESTS: usize = 64;

fn segment_to_geojson(input: &Segment) -> String {
    serde_json::to_string(&input.geometry)
        // won't fail
        .unwrap()
}

fn path_to_geojson(input: &Path, segments: HashMap<(StopPair, AreaType), Segment>) -> String {
    serde_json::to_string(&input.segments().iter()
            .flat_map(|s| &segments.get(&(*s, input.ty)).unwrap().geometry)
            .collect::<Vec<&Coords>>()
        )
        .unwrap()
}

#[tokio::main]
async fn main() -> Result<(), Box<(dyn std::error::Error + 'static)>> {
    env_logger::init();
    debug!("current configuration: {:?}", CONFIGS.routing);

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
    let mut missing_segments: HashSet<(AreaType, (StopPair, RoutingType))> = HashSet::new();
    for p in paths.values() {
        for s in p.segments() {
            if !segments.contains(&(p.ty, s)) {
                debug!("missing segment: {:?}", (p.ty, s));
                if CONFIGS.routing.skip_routing_types.contains(&p.rty) {
                    continue
                }
                missing_segments.insert((p.ty, (s, p.rty)));
            }
        }
    }
    info!("done! there are {} missing segments", missing_segments.len());
    
    let missing_segments_count = missing_segments.len();

    let mut tasks: JoinSet<_> = JoinSet::new();

    let cli = Arc::new(reqwest::Client::new());
    let mut pending_segments = Vec::<Segment>::new();
    for (t, (sp, ty)) in missing_segments {
        let s0 = stops.get(t).get(&sp.0).unwrap().clone();
        let s1 = stops.get(t).get(&sp.1).unwrap().clone();
        tasks.spawn(calculate_geometry(t, s0, s1, cli.clone(), ty));
        while tasks.len() > MAX_PARALLEL_REQUESTS {
            if let Some(uuh) = tasks.join_next().await {
                debug!("waiting task {}/{}...", pending_segments.len(), missing_segments_count);
                let ((t, (s0, s1)), c) = uuh??;
                pending_segments.push(Segment::new(s0, s1, t, c));
           }
        }
    }
    debug!("waiting final tasks...");
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

#[tokio::test]
async fn measure_total_segment_length() {
    let db = mongodb::Client::with_options(CONFIGS.db.gen_mongodb_options()).expect("error creating mongodb client").database(CONFIGS.db.get_db());
    let segments: Vec<Segment> = Segment::get_coll(&db)
        .find(doc!{}, None)
        .await
        .expect("error getting segments")
        .map(|r| r.expect("error reading segment"))
        .collect()
        .await;

    let mut lengths: HashMap<AreaType, f64> = HashMap::from([
        (AreaType::E, 0.0),
        (AreaType::U, 0.0),
    ]);

    for s in segments {
        let length = s.geometry
            .iter()
            .zip(s.geometry.iter().skip(1))
            .fold(0.0, |acc, (a, b)| {
                acc + (a - b)
            });
        lengths.get_mut(&s.ty).map(|l| *l += length).expect("error updating length");
    }

    println!("segment lengths by area: {:#?}", lengths);
    println!("total segment length: {} km", lengths.values().sum::<f64>() / 1000.0);
}
