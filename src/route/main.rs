use std::collections::{HashMap, HashSet};
use bruss_data::{BrussType, Coords, Path, Segment, Stop, StopPair};
use log::{debug, info};
use tt::AreaType;
use mongodb::bson::doc;
use futures::{StreamExt,TryStreamExt};
use bruss_router::osrm::OsrmResponse;
use bruss_router::CONFIGS;

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

    Ok(())
}

