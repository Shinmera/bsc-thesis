use std::collections::HashMap;
use std::hash::Hash;
use timely::Data;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::Unary;
use timely::dataflow::{Stream, Scope};

pub trait Reduce<G: Scope, D: Data> {
    fn reduce_by<H, V, K, R>(&self, key_extractor: K, initial_value: V, reductor: R) -> Stream<G, (H, V)>
        where H: Hash+Eq+Data+Clone,
              V: Eq+Data+Clone,
              K: Fn(&D)->H+'static,
              R: Fn(&D, V)->V+'static;
}

impl<G: Scope, D: Data> Reduce<G, D> for Stream<G, D> {
    fn reduce_by<H, V, K, R>(&self, key_extractor: K, initial_value: V, reductor: R) -> Stream<G, (H, V)>
        where H: Hash+Eq+Data+Clone,
              V: Eq+Data+Clone,
              K: Fn(&D)->H+'static,
              R: Fn(&D, V)->V+'static {
        let mut epochs = HashMap::new();
        
        self.unary_notify(Pipeline, "Reduce", Vec::new(), move |input, output, notificator| {
            input.for_each(|time, data| {
                let window = epochs.entry(time.clone()).or_insert(HashMap::new());
                while let Some(dat) = data.pop() {
                    let key = key_extractor(&dat);
                    let value = reductor(&dat, window.remove(&key).unwrap_or(initial_value.clone()));
                    window.insert(key, value);
                }
                notificator.notify_at(time);
            });
            notificator.for_each(|time, _, _| {
                if let Some(mut window) = epochs.remove(&time) {
                    output.session(&time).give_iterator(window.drain());
                }
            });
        })
    }
}
