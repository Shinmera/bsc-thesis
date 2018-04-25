use std::cmp::Eq;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::rc::Rc;
use timely::Data;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::generic::unary::Unary;
use timely::dataflow::{Stream, Scope};

pub trait RollingCount<G: Scope, D: Data+Send> {
    fn rolling_count<H, DO, K, C>(&self, key_extractor: K, counter: C) -> Stream<G, DO>
    where H: Hash+Eq+Data+Clone,
          DO: Data+Send,
          K: Fn(&D)->H+'static,
          C: Fn(D, usize)->DO+'static;
}

impl<G: Scope, D: Data+Send> RollingCount<G, D> for Stream<G, D> {
    fn rolling_count<H, DO, K, C>(&self, key_extractor: K, counter: C) -> Stream<G, DO>
    where H: Hash+Eq+Data+Clone,
          DO: Data+Send,
          K: Fn(&D)->H+'static,
          C: Fn(D, usize)->DO+'static {
        let mut counts = HashMap::new();

        let (key, exchange) = exchange!(key_extractor);
        
        self.unary_stream(exchange, "RollingCount", move |input, output| {
            input.for_each(|time, data| {
                output.session(&time).give_iterator(data.drain(..).map(|x|{
                    let key = key(&x);
                    let count = counts.get(&key).unwrap_or(&0)+1;
                    counts.insert(key.clone(), count);
                    counter(x, count)
                }));
            });
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use timely;
    use timely::dataflow::operators::{ToStream, Capture};
    use timely::dataflow::operators::capture::Extract;
    
    #[test]
    fn rolling_count() {
        let data = timely::example(|scope| {
            vec!(1, 2, 3, 4, 5)
                .to_stream(scope)
                .rolling_count(|x| x%2, |x, c| (x, c))
                .capture()
        });
        
        assert_eq!(data.extract()[0].1, vec!((1, 1), (2, 1), (3, 2), (4, 2), (5, 3)));
    }
}
