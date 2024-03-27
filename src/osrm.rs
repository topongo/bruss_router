use std::collections::HashMap;

use bruss_data::Coords;
use polyline::decode_polyline;
use serde::{Deserialize,Serialize};
use tt::TTStop;

#[derive(Deserialize, Debug)]
pub(crate) struct OsrmResponse {
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
        assert_eq!(self.routes.len(), 1);
        for r in self.routes {
            assert_eq!(r.legs.len(), 1);
            for l in r.legs {
                for s in l.steps { 
                    o.insert(&mut s.get_coords());
                }
            }
        }
        o
    }
}


