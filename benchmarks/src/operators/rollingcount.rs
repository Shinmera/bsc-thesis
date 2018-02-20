use std::cmp::Eq;
use std::collections::HashMap;
use std::hash::Hash;
use timely::Data;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::unary::Unary;
use timely::dataflow::{Stream, Scope};

pub trait RollingCount<G: Scope, D: Data> {
    fn rolling_count<H: Hash+Eq+Data+Clone, DO: Hash+Eq+Data+Clone, K: Fn(&D)->(H, DO)+'static>(&self, key_extractor: K) -> Stream<G, (H, DO, usize)>;
}

impl<G: Scope, D: Data> RollingCount<G, D> for Stream<G, D> {
    fn rolling_count<H: Hash+Eq+Data+Clone, DO: Hash+Eq+Data+Clone, K: Fn(&D)->(H, DO)+'static>(&self, key_extractor: K) -> Stream<G, (H, DO, usize)> 
    {
        let mut counts = HashMap::new();
        self.unary_stream(Pipeline, "RollingCount", move |input, output| {
            input.for_each(|time, data| {
                output.session(&time).give_iterator(data.drain(..).map(|x|{
                    let (key, tail) = key_extractor(&x);
                    let count = counts.get(&key).unwrap_or(&0)+1;
                    counts.insert(key.clone(), count);
                    (key, tail, count)
                }));
            });
        })
    }
}
