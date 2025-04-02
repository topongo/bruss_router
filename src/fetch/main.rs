#![feature(iter_intersperse)]

use std::{collections::{HashMap, HashSet}, sync::Arc};
use async_stream::try_stream;
use bruss_config::CONFIGS;
use futures::{Stream, StreamExt, TryStreamExt};

use mongodb::bson::doc;
use mongodb::options::FindOptions;
use tokio::{sync::{Mutex, Semaphore}, task::JoinHandle};
use tt::{AreaType, RequestOptions, TTArea, TTClient, TTResult, TTRoute, TTStop, TTTrip, TripQuery};
use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use chrono_tz::Europe::Rome;
use bruss_data::{sequence_hash, Area, BrussType, FromTT, Path, Route, Schedule, Segment, Stop, Trip};
use log::{info,debug,warn};
use serde::Deserialize;

#[derive(Deserialize)]
struct IdDocument<T> {
    id: T
}

type DownloadJob = JoinHandle<TTResult<(Vec<TTTrip>, NaiveDateTime)>>;

struct ParallelDownloader {
    client: Arc<TTClient>,
    jobs: Arc<Mutex<Vec<DownloadJob>>>,
    semaphore: Arc<Semaphore>,
}

impl ParallelDownloader {
    fn new(client: Arc<TTClient>) -> Self {
        Self {
            client,
            jobs: Arc::new(Mutex::new(Vec::new())),
            semaphore: Arc::new(Semaphore::new(CONFIGS.routing.parallel_downloads.unwrap_or(5))),
        }
    }

    async fn request(&self, ty: AreaType, route_id: u16, time: NaiveDateTime) {
        let sem = self.semaphore.clone();
        let client = self.client.clone();
        let requests = self.jobs.clone();
        let h = tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            client.request_opt::<TTTrip, TripQuery>(Some(RequestOptions::new("/trips_new").query(TripQuery {
                ty,
                route_id,
                limit: 1 << 10,
                time
            }))).await.map(|ts| (ts, time))
        });
        requests.lock().await.push(h);
    }

    fn wait(self) -> impl Stream<Item = TTResult<(Vec<TTTrip>, NaiveDateTime)>> {
        try_stream! {
            let Self { jobs, .. } = self;

            let len = jobs.lock().await.len();
            for (n, h) in jobs.lock().await.drain(..).enumerate() {
                debug!("waiting for job {}/{}...", n, len);
                yield h.await.unwrap()?;
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<(dyn std::error::Error + 'static)>> {
    env_logger::init(); 

    info!("getting tt client");
    let tt_client = Arc::new(CONFIGS.tt.client());

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

    info!("getting trips from database...");
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

    info!("getting schedules from db...");
    let sched: HashSet<Schedule> = Schedule::get_coll(&db)
        .find(doc!{}, None)
        .await?
        .try_collect()
        .await?;
    info!("done! got {} schedules", sched.len());

    let mut paths_missing: HashMap<String, Path> = HashMap::new();
    let mut trips_missing: HashMap<String, Trip> = HashMap::new();
    let mut sched_missing: HashSet<Schedule> = HashSet::new();

    // // get a specific route to perform first pass
    // let r = routes.values().find(|r| r.code == "7").unwrap();
    // info!("getting holidays using route {}...", r.id);
    // let mut holidays = HashSet::new();
    // let mut time = Local::now().date_naive().and_time(NaiveTime::from_hms_opt(3, 0, 0).unwrap());
    // let mut hold = vec![];
    // loop {
    //     let ts = tt_client
    //         .request_opt::<TTTrip, TripQuery>(Some(RequestOptions::new("/trips_new").query(TripQuery { 
    //             ty: r.area_ty,
    //             route_id: r.id,
    //             limit: 1,
    //             time 
    //         })))
    //         .await?;
    //     // 7 doens't pass on sundays and holidays, so if we get an empty schedule on a non-sunday,
    //     //   then it's a holiday
    //     if ts.is_empty() {
    //         // this is an holiday or we reached the end of the schedule
    //         if time.weekday().number_from_monday() != 7 {
    //             hold.push(time.date());
    //             debug!("{:?} is a holiday", time.date());
    //         }
    //         if hold.len() >= 5 {
    //             // TODO: solve this edge case succession of days: normal day, normal day, holiday, end of schedule, end of schedule
    //             //       we should check if the last day is a holiday or is the end of the schedule
    //             debug!("5 days without schedule, stopping and discarding {} trips on hold", hold.len());
    //             break
    //         }
    //     } else {
    //         while let Some(h) = hold.pop() {
    //             holidays.insert(h);
    //         }
    //     }
    //     time += TimeDelta::days(1);
    // }
    // 
    // info!("collected {} holidays", holidays.len());
    
    let bounds = (
        NaiveDate::from_ymd_opt(2024, 9, 9).unwrap(),
        NaiveDate::from_ymd_opt(2025, 6, 12).unwrap(),
    );

    if CONFIGS.routing.get_trips {
        let mut tot_routes = 0;
        info!("getting trips from tt...");
        let tt_client = Arc::new(CONFIGS.tt.client());
        let dl = ParallelDownloader::new(Arc::clone(&tt_client));
        for (n, r) in routes
            .values()
            // uncomment to get only a specific route
            // .filter(|r| r.code == "7" && r.area == 23)
            .enumerate()
            // .filter(|(n, _)| *n < 200)
        {
            tot_routes += 1;
            let mut time = bounds.0.and_time(NaiveTime::from_hms_opt(4, 0, 0).unwrap());
            debug!("using start time: {}", time);
            while time.date() <= bounds.1 {
                info!("appending download of route {} ({:?}) [{:3}/{:3}] for {}...", r.id, r.area_ty, n, routes.len(), time.date());

                dl.request(r.area_ty, r.id, time).await;

                time += Duration::days(1);
            } 
        }

        let st = dl.wait();
        tokio::pin!(st);
        let mut route_prev = -1;
        let mut route_c = -1;
        while let Some(ts) = st.next().await {
            let (ts, time) = ts?;
            // if num_req >= 1000 {
            //     info!("telemetry for tt client: {}", tt_client.print_telemetry().await);
            //     panic!();
            // }
            for t in ts {
                if route_prev != t.route as i32 {
                    route_prev = t.route as i32;
                    route_c += 1;
                }
                info!("processing trip {} of route {} [{:3}/{:3}] on day {}", t.id, t.route, route_c, tot_routes, time.date());
                // insert missing path
                if !trips.contains_key(&t.id) && !trips_missing.contains_key(&t.id) {
                    let seq: Vec<u16> = t.stop_times.iter().map(|st| st.stop).collect();
                    let h = sequence_hash(t.ty, &seq);
                    if !path_ids.contains(&h) && !paths_missing.contains_key(&h) {
                        paths_missing.insert(h.clone(), Path::new(seq, t.ty, routes.get(&t.route).unwrap().into()));
                    }
                }
                // debug!("got trip: {:?}", t);
                let (t, departure) = Trip::from_tt(t);
                debug!("departure timedelta from midnight: {} hours", departure.num_hours());
                // insert missing schedules
                let departure_midnight_naive = time.date().and_time(NaiveTime::MIN);
                debug!("departure day, midnight, naive time: {}", departure_midnight_naive);
                let departure_midnight_rome = Rome.from_local_datetime(&departure_midnight_naive).unwrap();
                debug!("departure day, midnight, rome time: {}", departure_midnight_rome);
                let departure_rome = departure_midnight_rome + departure;
                debug!("departure day, departure time, rome time: {}", departure_rome);
                let departure_utc = departure_rome.with_timezone(&Utc);
                debug!("departure day, departure time, utc time: {}", departure_utc);

                let sch = Schedule::from_trip(&t, departure_utc);
                debug!("got schedule: {:?}", sch);
                if !sched.contains(&sch) && !sched_missing.contains(&sch) {
                    sched_missing.insert(sch);
                }
                // insert missing trip
                if !trips.contains_key(&t.id) && !trips_missing.contains_key(&t.id) {
                    trips_missing.insert(t.id.clone(), t);
                }
            }
        }

        info!("done! got {} missing trips, {} missing paths and {} missing schedules", trips_missing.len(), paths_missing.len(), sched_missing.len());
        info!("tt client telemetry: {}", tt_client.print_telemetry().await);
    }

    if !paths_missing.is_empty() {
        info!("inserting {} missing paths...", paths_missing.len());
        Path::get_coll(&db).insert_many(paths_missing.values(), None).await?;
        info!("done!")
    }

    if !sched_missing.is_empty() {
        info!("inserting {} missing schedules...", sched_missing.len());
        Schedule::get_coll(&db).insert_many(
            sched_missing,
            None
        ).await?;
        info!("done!")
    }

    if CONFIGS.routing.get_trips && !trips_missing.is_empty() {
        info!("inserting {} missing trips...", trips_missing.len());
        Trip::get_coll(&db).insert_many(trips_missing.values(), None).await?;
        // let _ = if CONFIGS.routing.deep_trip_check {
        //     info!("checking for differences in trips...");
        //     let mut c = 0;
        //     for t in &trips_tt {
        //         if !trips.contains_key(&t.id) {
        //             // we just inserted this trip, skip it
        //             continue
        //         }
        //         // we are sure that the trip is in the database, cause we skipped the ones that
        //         // aren't
        //         if trips.get(&t.id).unwrap().deep_cmp(&t) {
        //             debug!("trip {} is the same on db and on tt", t.id);
        //         } else {
        //             warn!("trip {} is different on db and on tt", t.id);
        //             c += 1;
        //         }
        //     }
        //     if c > 0 {
        //         warn!("{} trips are different on db and on tt", c);
        //     } else {
        //         info!("all trips are the same on db and on tt");
        //     }
        // } else {
        //     trips_tt.iter().filter(|t| !trips.contains_key(&t.id))
        // };

        // if trips_tt.iter().filter(|t| !trips.contains_key(&t.id)).count() > 0 {
        //     info!("inserting {} missing trips...", trips_tt.len());
        //     Trip::get_coll(&db).insert_many(
        //         trips_tt.iter()
        //             .filter(|t| !trips.contains_key(&t.id))
        //         , None).await?;
        //     info!("done!");
        // } else {
        //     info!("no trips inserted")
        // }
    }



    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{collections::{HashMap, HashSet}, io::Write, path::Path, time::Instant};

    use bruss_config::CONFIGS;
    use bruss_data::{Direction, FromTT, Route, Stop, Trip};
    use bson::{doc, Document};
    use chrono::{Datelike, Local, NaiveDate, NaiveTime, TimeDelta, Utc};
    use futures::{StreamExt, TryStreamExt};
    use tt::{AreaType, RequestOptions, TTRoute, TTStop, TTTrip, TripQuery};

    async fn get_routes() -> Vec<Route> {
        let c = CONFIGS.tt.client();
        c.request::<TTRoute>()
            .await
            .unwrap()
            .into_iter()
            .map(Route::from_tt)
            .collect::<Vec<_>>()
    }

    async fn get_stops() -> Vec<Stop> {
        let c = CONFIGS.tt.client();
        c.request::<TTStop>()
            .await
            .unwrap()
            .into_iter()
            .map(Stop::from_tt)
            .collect::<Vec<_>>()
    }


    const WEEK_REPR: [&str; 7] = ["mo", "tu", "we", "th", "fr", "sa", "su"];

    #[tokio::test]
    async fn test_weekday_consistency() {
        let r = get_routes().await;
        let today = Local::now().date_naive();


        // let bounds = if today <= NaiveDate::from_ymd_opt(today.year(), 6, 8).unwrap() {
        //     (NaiveDate::from_ymd_opt(today.year() - 1, 9, 16).unwrap(), NaiveDate::from_ymd_opt(today.year(), 6, 4).unwrap())
        // } else if today <= NaiveDate::from_ymd_opt(today.year(), 9, 12).unwrap() {
        //     (NaiveDate::from_ymd_opt(today.year(), 6, 12).unwrap(), NaiveDate::from_ymd_opt(today.year(), 9, 8).unwrap())
        // } else {
        //     (NaiveDate::from_ymd_opt(today.year(), 9, 15).unwrap(), NaiveDate::from_ymd_opt(today.year() + 1, 6, 4).unwrap())
        // };
        let bounds = (
            NaiveDate::from_ymd_opt(2025, 3, 17).unwrap(),
            NaiveDate::from_ymd_opt(2025, 6, 30).unwrap(),
        );
        let bound_weeks = (bounds.1 - bounds.0).num_days() / 7;
        println!("bounds: {:?} weeks: {}", bounds, bound_weeks);

        // collect trips from this week (assuming the current week of execution doens't include execptions)
        let c = CONFIGS.tt.client();
        
        let start_week_now = today - TimeDelta::days(today.weekday().number_from_monday() as i64) - TimeDelta::days(14);
        let mut sample = HashMap::new();

        let r = r.iter().find(|r| r.code == "3" && r.area == 23).unwrap();


        // println!("{:?}", c
        //     .request_opt::<TTTrip, TripQuery>(Some(RequestOptions::new("/trips_new").query(TripQuery { 
        //         ty: r.area_ty,
        //         route_id: r.id,
        //         limit: 1 << 10,
        //         time: NaiveDate::from_ymd_opt(2025, 3, 16).unwrap().and_time(NaiveTime::from_hms_opt(4, 0, 0).unwrap())
        //     })))
        //     .await
        //     .unwrap()
        // );

        let mut weekday_trips: HashMap<i64, HashMap<String, u16>> = HashMap::new();
        let mut global_trips: HashMap<String, u16> = HashMap::new();
        let mut trips_in_weekdays = HashMap::<String, HashSet<u16>>::new();
        let mut trips_in_weeknums = HashMap::<String, HashSet<u16>>::new();
        let mut trip_hours = HashMap::<String, TimeDelta>::new();

        for w in 0..7 {
            let d = start_week_now + TimeDelta::days(w);
            let trips = c.request_opt::<TTTrip, TripQuery>(Some(RequestOptions::new("/trips_new").query(TripQuery { 
                ty: r.area_ty,
                route_id: r.id,
                limit: 1 << 10,
                time: d.and_time(NaiveTime::from_hms_opt(4, 0, 0).unwrap())
            })))
                .await
                .unwrap()
                .into_iter()
                .map(Trip::from_tt)
                .map(|(t, _)| t)
                .collect::<Vec<_>>();
            sample.insert(w, trips);
        }

        let mut trips_per_week: HashMap<_, Vec<Vec<_>>> = (0..7).map(|w| (w, vec![])).collect();
        let mut data = HashMap::<String, Trip>::new();

        let cache_str = std::env::var("BRUSS_TEST_CACHE_DIR").unwrap_or(".cache".to_owned());
        let cache = Path::new(&cache_str);

        for w in 0..7 {
            let mut date = bounds.0 + TimeDelta::days(w);
            let time = NaiveTime::from_hms_opt(4, 0, 0).unwrap();
            let mut weeknum = 0;

            if !cache.exists() {
                std::fs::create_dir(cache).unwrap();
            }
            println!("getting trips for weekday {}...", w);

            while date <= bounds.1 {
                let start = Instant::now();
                let limit = 1 << 10;
                let code = format!("{}{}{}{}{}", r.area_ty, r.id, limit, date.format("%Y%m%d"), time.format("%H%M%S"));
                let trips: Vec<(Trip, TimeDelta)> = if cache.join(&code).exists() {
                    // println!("  using cache file: {}", cache.join(&code).to_str().unwrap());
                    serde_json::from_str(&std::fs::read_to_string(cache.join(code)).unwrap()).unwrap()
                } else {
                    let v = c.request_opt::<TTTrip, TripQuery>(Some(RequestOptions::new("/trips_new").query(TripQuery { 
                        ty: r.area_ty,
                        route_id: r.id,
                        limit: 1 << 10,
                        time: date.and_time(time)
                    })))
                        .await
                        .unwrap()
                        .into_iter()
                        .map(Trip::from_tt)
                        // .map(|(t, departure)| {
                        //     if let Some(h) = trip_hours.get(&t.id) {
                        //         assert_eq!(*h, departure);
                        //     } else {
                        //         trip_hours.insert(t.id.clone(), departure);
                        //     }
                        //     t
                        // })
                        .collect::<Vec<_>>();
                    // println!("  saving to cache file: {}", cache.join(&code).to_str().unwrap());
                    std::fs::write(cache.join(&code), serde_json::to_string(&v).unwrap()).unwrap();
                    v
                };

                let trips = trips
                    .into_iter()
                    .map(|(t, departure)| {
                        if let Some(h) = trip_hours.get(&t.id) {
                            assert_eq!(*h, departure);
                        } else {
                            trip_hours.insert(t.id.clone(), departure);
                        }
                        t
                    })
                    .collect::<Vec<_>>();

                // use integer formatter
                print!("  getting trip {} ({:3}/{:3}) (elapsed seconds: {:2.3})\r", date, trips_per_week.get(&w).unwrap().len(), bound_weeks, start.elapsed().as_secs_f64());
                std::io::stdout().flush().unwrap();

                for t in &trips {
                    if let Some(t_w) = weekday_trips.get_mut(&w) {
                        if t_w.contains_key(&t.id) {
                            let c = t_w.get_mut(&t.id).unwrap();
                            *c += 1;
                        } else {
                            t_w.insert(t.id.clone(), 1);
                        }
                    } else {
                        let mut m = HashMap::new();
                        m.insert(t.id.clone(), 1);
                        weekday_trips.insert(w, m);
                    }

                    if let Some(c) = global_trips.get_mut(&t.id) {
                        *c += 1;
                    } else {
                        global_trips.insert(t.id.clone(), 1);
                    }
                    
                    if let Some(c) = trips_in_weekdays.get_mut(&t.id) {
                        c.insert(w as u16);
                    } else {
                        let mut s = HashSet::new();
                        s.insert(w as u16);
                        trips_in_weekdays.insert(t.id.clone(), s);
                    }

                    if let Some(c) = trips_in_weeknums.get_mut(&t.id) {
                        c.insert(weeknum);
                    } else {
                        let mut s = HashSet::new();
                        s.insert(weeknum);
                        trips_in_weeknums.insert(t.id.clone(), s);
                    }

                }

                trips_per_week.get_mut(&w).unwrap().push(trips.iter().map(|t| t.id.clone()).collect::<Vec<_>>());
                data.extend(trips.into_iter().map(|t| (t.id.clone(), t)));

                date += TimeDelta::days(7);
                weeknum += 1;
            }
            println!();
        }

        let mut trip_totals = HashMap::new();

        for (w, m) in &weekday_trips {
            for (t, c) in m {
                println!("{}: {} -> {}", w, t, c);
                if !trip_totals.contains_key(t) {
                    trip_totals.insert(t.clone(), *c);
                } else {
                    *trip_totals.get_mut(t).unwrap() += c;
                }
            }
        }

        // let mut stats = (0..7).map(|w| (w, (0, trips_per_week.get(&w).unwrap().len()))).collect::<HashMap<_, _>>();

        // for (sample, data, w) in (0..7).map(|w| (sample.get(&w).unwrap(), trips_per_week.get(&w).unwrap(), w)) {
        //     for d in data {
        //         if d.len() == sample.len() && d == sample {
        //             stats.get_mut(&w).unwrap().0 += 1;
        //         }
        //     }
        // }

        // for i in 0..7 {
        //     println!("stats for {}: {}/{} ({:.2}%)", i, stats.get(&i).unwrap().0, stats.get(&i).unwrap().1, stats.get(&i).unwrap().0 as f64 / stats.get(&i).unwrap().1 as f64 * 100.0);
        // }

        // for (id, c) in &global_trips {
        //     println!("{} -> {}", id, c);
        // }

        // let mut global_trips_ordered_by_count: HashMap<u16, Vec<String>> = HashMap::new();
        // for (t, c) in global_trips {
        //     if let Some(v) = global_trips_ordered_by_count.get_mut(&c) {
        //         v.push(t);
        //     } else {
        //         global_trips_ordered_by_count.insert(c, vec![t]);
        //     }
        // }


        // let mut global_trips_ordered_by_count = global_trips_ordered_by_count.into_iter().collect::<Vec<_>>();
        // global_trips_ordered_by_count.sort_by_cached_key(|(c, _)| *c);

        let mut trips = global_trips.keys().cloned().collect::<Vec<_>>();
        trips.sort_by_cached_key(|t| *trip_hours.get(t).unwrap());
        // trips.sort();


        for t in trips {
            // println!("{}:", c);
            // v.sort_by_cached_key(|t| trip_hours.get(t).unwrap());
            // for t in v {
            let trip = data.get(&t).unwrap();
            print!("  {}", t);
            for w in 0..7 {
                if trips_in_weekdays.get(&t).unwrap().contains(&w) {
                    print!(" {}", WEEK_REPR[w as usize]);
                } else {
                    print!(" --");
                }
            }
            for wn in 0..bound_weeks {
                if trips_in_weeknums.get(&t).unwrap().contains(&(wn as u16)) {
                    print!(" {:02}", wn);
                } else {
                    print!(" --");
                }
            }
            println!(
                " - {} {}",
                (NaiveTime::MIN + *trip_hours.get(&t).unwrap()).format("%H:%M"),
                if matches!(trip.direction, Direction::Forward) { "→" } else { "←" },
            );
            // }
        }

        // assert_eq!(0, 1);

        // for _ in 0..1 {
        //     // get random day of week
        //     let wd= rand::random::<i64>() % 7;
        //     // random day between the bounds
        //     let (d1, d2) = loop {
        //         let d1 = bounds.0 + TimeDelta::days((rand::random::<i64>() % bound_weeks * 7 + wd).abs());
        //         let d2 = bounds.0 + TimeDelta::days((rand::random::<i64>() % bound_weeks * 7 + wd).abs());
        //         if d1 != d2 {
        //             break (d1, d2)
        //         }
        //     };
        //
        //     // get trips from random route at the two random days
        //     // let r = r.choose(&mut rand::thread_rng()).unwrap();
        //     let r = r.iter().find(|r| r.code == "7").unwrap();
        //     let c = CONFIGS.tt.client();
        //     let mut trips: Vec<Vec<(Trip, TimeDelta)>> = vec![];
        //     for d in &[d1, d2] {
        //         trips.push(c.request_opt::<TTTrip, TripQuery>(Some(RequestOptions::new("/trips_new").query(TripQuery { 
        //             ty: r.area_ty,
        //             route_id: r.id,
        //             limit: 1 << 10,
        //             time: d.and_time(NaiveTime::from_hms_opt(5, 0, 0).unwrap())
        //         })))
        //             .await
        //             .unwrap()
        //             .into_iter()
        //             .map(Trip::from_tt)
        //             .collect::<Vec<_>>());
        //         println!("got {} trips for route {} on {}", trips.last().unwrap().len(), r.id, d);
        //     }
        //     println!("testing trips on {} and {} ({}) for route {}", d1, d2, wd, r.id);
        //     assert_eq!(trips[0].len(), trips[1].len());
        //     for (t1, t2) in trips[0].iter().zip(trips[1].iter()) {
        //         println!("aaah {} {} == {} {}", t1.0.id, t1.1, t2.0.id, t2.1);
        //         assert_eq!(t1.0.id, t2.0.id);
        //     }
        // }
    }

    /// This test checks if the trips are consistent between two fixed weekdays on random weeks
    /// This usually works, but often it fails due to exceptions in the data, like holidays, ecc.
    #[tokio::test]
    async fn test_random_day_weekday_consistency() {
        let r = get_routes().await;
        let today = Local::now().date_naive();

        let bounds = if today <= NaiveDate::from_ymd_opt(today.year(), 6, 8).unwrap() {
            (NaiveDate::from_ymd_opt(today.year() - 1, 9, 16).unwrap(), NaiveDate::from_ymd_opt(today.year(), 6, 4).unwrap())
        } else if today <= NaiveDate::from_ymd_opt(today.year(), 9, 12).unwrap() {
            (NaiveDate::from_ymd_opt(today.year(), 6, 12).unwrap(), NaiveDate::from_ymd_opt(today.year(), 9, 8).unwrap())
        } else {
            (NaiveDate::from_ymd_opt(today.year(), 9, 15).unwrap(), NaiveDate::from_ymd_opt(today.year() + 1, 6, 4).unwrap())
        };
        let bound_weeks = (bounds.1 - bounds.0).num_days() / 7;
        println!("bounds: {:?} weeks: {}", bounds, bound_weeks);


        for _ in 0..1 {
            // get random day of week
            let wd= rand::random::<i64>() % 7;
            // random day between the bounds
            let (d1, d2) = loop {
                let d1 = bounds.0 + TimeDelta::days((rand::random::<i64>() % bound_weeks * 7 + wd).abs());
                let d2 = bounds.0 + TimeDelta::days((rand::random::<i64>() % bound_weeks * 7 + wd).abs());
                if d1 != d2 {
                    break (d1, d2)
                }
            };

            // get trips from random route at the two random days
            // let r = r.choose(&mut rand::thread_rng()).unwrap();
            let r = r.iter().find(|r| r.code == "7").unwrap();
            let c = CONFIGS.tt.client();
            let mut trips: Vec<Vec<(Trip, TimeDelta)>> = vec![];
            for d in &[d1, d2] {
                trips.push(c.request_opt::<TTTrip, TripQuery>(Some(RequestOptions::new("/trips_new").query(TripQuery { 
                    ty: r.area_ty,
                    route_id: r.id,
                    limit: 1 << 10,
                    time: d.and_time(NaiveTime::from_hms_opt(5, 0, 0).unwrap())
                })))
                    .await
                    .unwrap()
                    .into_iter()
                    .map(Trip::from_tt)
                    .collect::<Vec<_>>());
                println!("got {} trips for route {} on {}", trips.last().unwrap().len(), r.id, d);
            }
            println!("testing trips on {} and {} ({}) for route {}", d1, d2, wd, r.id);
            assert_eq!(trips[0].len(), trips[1].len());
            for (t1, t2) in trips[0].iter().zip(trips[1].iter()) {
                println!("aaah {} {} == {} {}", t1.0.id, t1.1, t2.0.id, t2.1);
                assert_eq!(t1.0.id, t2.0.id);
            }
        }
    }

    #[tokio::test]
    async fn test_trip_incrementality() {
        let c = CONFIGS.tt.client();
        let bounds = (
            NaiveDate::from_ymd_opt(2024, 9, 9).unwrap(),
            NaiveDate::from_ymd_opt(2025, 6, 27).unwrap(),
        );

        for i in 0..400000 {
            let t = format!("{:09}{}{}", i, bounds.0.format("%Y%m%d"), bounds.1.format("%Y%m%d"));
            println!("requesting trip {}...", t);
        }
    }

    #[tokio::test]
    async fn test_trip_time_stop_query() {
        use serde::Deserialize;

        let db = mongodb::Client::with_options(CONFIGS.db.gen_mongodb_options()).unwrap().database(CONFIGS.db.get_db());
        let stop = 423;
        let area = AreaType::U;
        let time = Utc::now();

        #[derive(Deserialize)]
        struct R {
            #[serde(with = "bson::serde_helpers::chrono_datetime_as_bson_datetime")]
            departure: chrono::DateTime<Utc>,
            trip: Trip,
        }

        let stop_time_string = format!("$hints.times.{}.arrival[0]", stop);
        let slen = stop_time_string.len();

        print!("running query...");
        let start = Instant::now();
        let trips: Vec<R> = db.collection::<Document>("schedules")
            .aggregate(vec![
                doc!{"$match": {"$and": [
                    // filter by area
                    {"hints.type": area.to_string()},
                    // filter by stop, if present in hits.times
                    {&stop_time_string[1..slen - 3]: {"$exists": true}},
                ]}},
                // calculate arrival at stop
                doc!{"$set": {"arrival_at_stop": {"$add": ["$departure", {"$multiply": [1000, {"$arrayElemAt": [&stop_time_string[0..slen - 3], 0]}]}]}}},
                // filter by arrival at stop
                doc!{"$match": {"arrival_at_stop": {"$gte": time}}},
                // sort by arrival at specific stop: we can't simply sort by general departure.
                doc!{"$sort": {"arrival_at_stop": 1}},
                // lookup trip
                doc!{"$lookup": {"from": "trips","localField": "id","foreignField": "id","as": "trip"}},
                // strip $lookup result
                doc!{"$unwind": "$trip"},
                // hard limit results
                doc!{"$limit": 100},
                // project only the necessary fields
                doc!{"$project": {"_id": 0,"trip": 1,"departure": 1}},
                // doc!{"$set": {"arrival_at_stop": }}
            ], None)
            .await
            .unwrap()
            .map(|r| r.map(|d| bson::from_document(d).unwrap()))
            .try_collect()
            .await
            .unwrap();
        println!(" done! that took {} seconds", start.elapsed().as_secs_f64());

        assert!(!trips.is_empty());
        println!("got {} trips", trips.len());
        for t in trips {
            // this is not right: the trip can start before the set time, while the bus can arrive at
            // the stop after the set time
            // assert!(t.departure > time);
            assert!(t.trip.times.has_stop(&stop));
            assert_eq!(t.trip.ty, area);
            assert!(t.departure + t.trip.times.get(&stop).unwrap().arrival >= time);
        }
    }

    #[tokio::test]
    async fn test_trip_time_route_query() {
        use serde::Deserialize;

        let db = mongodb::Client::with_options(CONFIGS.db.gen_mongodb_options()).unwrap().database(CONFIGS.db.get_db());
        let route = 402;
        let area = AreaType::U;
        let time = Utc::now();

        #[derive(Deserialize)]
        struct R {
            #[serde(with = "bson::serde_helpers::chrono_datetime_as_bson_datetime")]
            departure: chrono::DateTime<Utc>,
            trip: Trip,
        }

        let trips: Vec<R> = db.collection::<Document>("schedules")
            .aggregate(vec![
                doc!{"$match": {"$and": [
                    // we don't need to filter by area, since ids are unique across areas, unlike stops.
                    // // filter by area
                    // {"hints.type": area.to_string()},
                    // filter by route
                    {"hints.route": route as i32},
                    // filter by general departure
                    {"departure": {"$gte": time}},
                ]}},
                // sort by general departure
                doc!{"$sort": {"departure": 1}},
                // lookup trip
                doc!{"$lookup": {"from": "trips","localField": "id","foreignField": "id","as": "trip"}},
                // strip $lookup result
                doc!{"$unwind": "$trip"},
                // hard limit results
                doc!{"$limit": 100},
                // project only the necessary fields
                doc!{"$project": {"_id": 0,"trip": 1,"departure": 1}},
            ], None)
            .await
            .unwrap()
            .map(|r| r.map(|d| bson::from_document(d).unwrap()))
            .try_collect()
            .await
            .unwrap();
        assert!(!trips.is_empty());
        println!("got {} trips", trips.len());
        for t in trips {
            assert!(t.departure >= time);
            assert_eq!(t.trip.route, route);
            assert_eq!(t.trip.ty, area);
        }
    }
}
