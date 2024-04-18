use std::sync::Arc;

use bruss_data::{Coords, RoutingType, Stop, StopPair};
use polyline::decode_polyline;
use serde::Deserialize;
use tt::AreaType;
use crate::CONFIGS;

#[derive(Deserialize, Debug)]
pub struct OsrmResponse {
    routes: Vec<Route>
}

#[derive(Deserialize, Debug)]
struct Route {
    legs: Vec<Leg>
}

#[derive(Deserialize, Debug)]
struct Leg {
    steps: Vec<Step>
}

#[derive(Deserialize, Debug)]
struct Step {
    geometry: String,
}

impl Step {
    pub fn get_coords(&self) -> Vec<Coords> {
        let line = decode_polyline(&self.geometry, 5).unwrap();
        let mut o = Vec::new();
        if line.lines().len() == 0 {
            return o;
        }
        for (n, l) in line.lines().enumerate() {
            if n == 0 { 
                o.push(Coords::new(l.start.y, l.start.x));
            }
            o.push(Coords::new(l.end.y, l.end.x));
        }
        o
    }
}

impl OsrmResponse {
    pub fn flatten(self) -> Vec<Coords> {
        let mut o = Vec::new();
        assert!(self.routes.len() == 1);
        for r in self.routes {
            assert!(r.legs.len() == 1);
            for l in r.legs {
                // assert!(l.steps.len() == 1);
                for s in l.steps {
                    o.append(&mut s.get_coords());
                }
            }
        }
        o
    }
}

pub fn url_builder(start: &Coords, end: &Coords, ty: RoutingType) -> String {
    format!(
        "http://{}:{}/{}/{};{}?geometries=polyline&overview=false&steps=true&alternatives=false",
        CONFIGS.routing.host,
        CONFIGS.routing.port.unwrap_or(80),
        match ty {
            RoutingType::Bus => &CONFIGS.routing.url_bus,
            RoutingType::Railway => &CONFIGS.routing.url_rail,
            _ => panic!()
        },
        start.to_osrm_query(),
        end.to_osrm_query()
    )
}

pub async fn calculate_geometry(ty: AreaType, s1: Stop, s2: Stop, client: Arc<reqwest::Client>, r_ty: RoutingType) -> Result<((AreaType, StopPair), Vec<Coords>), reqwest::Error> {
    if r_ty == RoutingType::Cableway {
        return Ok(((ty, (s1.id, s2.id)), vec![s1.position, s2.position]))
    }
    let res = client.get(url_builder(&s1.position, &s2.position, r_ty))
        .send()
        .await?
        .json::<OsrmResponse>()
        // .text()
        .await?;

    Ok(((ty, (s1.id, s2.id)), res.flatten()))
    // println!("{}", res);
    // todo!();
}


