#![feature(iter_intersperse)]

mod osrm;

use std::{collections::{HashMap, HashSet}, future::Future, sync::Arc};
use osrm::OsrmResponse;

use reqwest::{Client, RequestBuilder};
use tt::{RequestOptions, StopTime, TTStop, TTTrip, TripQuery};
use chrono::Local;
use bruss_config::BrussConfig;
use lazy_static::lazy_static;
use bruss_data::{FromTT, Stop, Coords};

lazy_static! {
    pub static ref CONFIGS: BrussConfig = BrussConfig::from_file("../bruss.toml").expect("cannot load configs");
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


async fn calculate_geometry(s1: Stop, s2: Stop, client: Option<Client>) -> Result<Vec<Coords>, reqwest::Error> {
    let client = client.unwrap_or(Client::new());
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

fn vec_to_geojson(input: &Vec<Coords>) -> String {
    serde_json::to_string(
        &input.iter()
            .map(|c| {
                (c.lng, c.lat)
            })
            .collect::<Vec<(f64, f64)>>()
        )
        .unwrap()
}

#[tokio::main]
async fn main() {
    let tt_client = CONFIGS.tt.client();

    let trips: Vec<TTTrip> = tt_client.request_opt(Some(RequestOptions::new().query(TripQuery { ty: tt::AreaType::U, route_id: 535, limit: 1, time: Local::now().naive_local() }))).await.unwrap();

    let mut stops: HashMap<u16, Stop> = HashMap::new();
    for s in tt_client
        .request::<TTStop>()
        .await
        .unwrap()
    {
        stops.insert(s.id, Stop::from_tt(s));
    }

    let mut segments = HashMap::<(u16, u16), Vec<Coords>>::new();
    // let mut segments_fut_keys = HashSet::<(u16, u16)>::new();
    // let mut segments_fut = Vec::new();
    let mut segments_fut = HashMap::<(u16, u16), _>::new();
    for t in trips {
        if t.stop_times.len() > 0 {
            // TODO: use this to assert the fact that stops are in order.
            let prev_n = -1;
            let mut prev = &t.stop_times[0];
            for st in t.stop_times.iter().skip(1) {
                let key = (prev.stop, st.stop);
                #[cfg(debug_assertions)]
                if segments.contains_key(&key) {
                    // check if data in database is correct
                    // let geom = calculate_geometry(stops.get(&prev.stop).unwrap(), stops.get(&st.stop).unwrap(), None).await.unwrap();
                    // assert!(geom.iter()
                        // .eq(segments.get(&key).unwrap().iter()));
                }
                if !segments_fut.contains_key(&key) {
                    let prev_stop = stops.get(&prev.stop).unwrap().clone();
                    let stop = stops.get(&st.stop).unwrap().clone();
                    segments_fut.insert(
                        key,
                        calculate_geometry(prev_stop, stop, None)
                    );
                } else {
                }
                prev = st;
            }
        }
    }

    let mut handles = HashMap::with_capacity(segments_fut.len());

    for (k, fut) in segments_fut {
        handles.insert(k, tokio::spawn(fut));
    }

    for (k, handle) in handles {
        segments.insert(k, handle.await.unwrap().unwrap());
    }
    // let res = futures::future::join_all(segments_fut.values());

    for (k, seg) in segments {
        let s1 = stops.get(&k.0).unwrap();
        let s2 = stops.get(&k.1).unwrap();
        println!("{} -> {}:\n{}\n\n", s1.name, s2.name, vec_to_geojson(&seg));
    }
    // println!("{:?}", res.flatten());

    // print!("https://map.project-osrm.org/?z=17&center=46.070927%2C11.127037");
    // for t in &trips[0].stop_times {
    //     let s = stops.get(&t.stop).unwrap();
    //     print!("&loc={}%2C{}", s.lat, s.lng);
    // }
    // println!("&hl=en&alt=0&srv=0");
}
