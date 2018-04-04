use std::collections::{HashMap};
use std::sync::{Mutex,Arc};
use std::time::Instant;
use timely::Data;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Unary;
use timely::dataflow::{Stream, Scope, ScopeParent};
use timely::progress::Timestamp;

pub trait Timer<G: Scope, D: Data> {
    fn time_first<T: Timestamp>(&self, Arc<Mutex<HashMap<T, Instant>>>) -> Stream<G, D>
        where G: ScopeParent<Timestamp=T>;
    fn time_last<T: Timestamp>(&self, Arc<Mutex<HashMap<T, Instant>>>) -> Stream<G, D>
        where G: ScopeParent<Timestamp=T>;
}

impl<G: Scope, D: Data> Timer<G, D> for Stream<G, D> {
    fn time_first<T: Timestamp>(&self, time_map: Arc<Mutex<HashMap<T, Instant>>>) -> Stream<G, D>
    where G: ScopeParent<Timestamp=T> {
        self.unary_notify(Pipeline, "Counter", Vec::new(), move |input, _, _| {
            let mut time_map = time_map.lock().unwrap();
            while let Some((cap, _)) = input.next(){
                let time = cap.time().clone();
                time_map.entry(time).or_insert_with(|| Instant::now());
            }
        })
    }
    
    fn time_last<T: Timestamp>(&self, time_map: Arc<Mutex<HashMap<T, Instant>>>) -> Stream<G, D>
    where G: ScopeParent<Timestamp=T> {
        self.unary_notify(Pipeline, "Counter", Vec::new(), move |input, _, _| {
            let mut time_map = time_map.lock().unwrap();
            while let Some((cap, _)) = input.next(){
                let time = cap.time().clone();
                time_map.insert(time, Instant::now());
            }
        })
    }
}
