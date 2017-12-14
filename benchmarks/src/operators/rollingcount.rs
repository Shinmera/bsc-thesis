use std::cmp::Eq;
use std::collections::HashMap;
use std::hash::Hash;
use timely::Data;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::unary::Unary;
use timely::dataflow::{Stream, Scope};

pub trait RollingCount<G: Scope, D: Data> {
    fn rolling_count<H: Hash+Eq+Data+Clone, K: Fn(&D)->(H, usize)+'static>(&self, key_extractor: K) -> Stream<G, (H, usize)>;
}

impl<G: Scope, D: Data> RollingCount<G, D> for Stream<G, D> {
    fn rolling_count<H: Hash+Eq+Data+Clone, K: Fn(&D)->(H, usize)+'static>(&self, key_extractor: K) -> Stream<G, (H, usize)> {
        let mut counts = HashMap::new();
        self.unary_stream(Pipeline, "RollingCount", move |input, output| {
            input.for_each(|time, data| {
                output.session(&time).give_iterator(data.drain(..).map(|x|{
                    let (key, inc) = key_extractor(&x);
                    let count = counts.get(&key).unwrap_or(&0)+inc;
                    counts.insert(key.clone(), count);
                    (key, count)
                }));
            });
        })
    }
}
