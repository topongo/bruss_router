#![feature(iter_intersperse)]

mod osrm;

use std::collections::HashMap;
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

fn url_builder(start: Coords, end: Coords) -> String {
    format!(
        "http://{}:{}/{}/{};{}?geometries=polyline&overview=false&steps=true&alternatives=false",
        CONFIGS.routing.host,
        CONFIGS.routing.port.unwrap_or(80),
        CONFIGS.routing.url,
        start.to_osrm_query(),
        end.to_osrm_query()
    )
}


async fn calculate_geometry(s1: Stop, s2: Stop, client: Option<Client>) -> Result<((u16, u16), Vec<Coords>), reqwest::Error> {
    let prev: &StopTime;
    let client = client.unwrap_or(Client::new());
    client.get(url_builder(s1.position, s2.position))
        .send()
        .await?
        .json::<OsrmResponse>()
        .await?;

    todo!();
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

    // println!("{}", res);
    println!("{:?}", res.flatten());

    // print!("https://map.project-osrm.org/?z=17&center=46.070927%2C11.127037");
    // for t in &trips[0].stop_times {
    //     let s = stops.get(&t.stop).unwrap();
    //     print!("&loc={}%2C{}", s.lat, s.lng);
    // }
    // println!("&hl=en&alt=0&srv=0");
}
