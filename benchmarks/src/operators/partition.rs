use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::rc::Rc;
use timely::Data;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::Unary;
use timely::dataflow::{Stream, Scope};

pub trait Partition<G: Scope, D: Data+Send> {
    fn partition<K, H>(&self, size: usize, key: K) -> Stream<G, Vec<D>>
    where K: Fn(&D)->H+'static,
          H: Hash+Eq+Data+Clone;
}

impl<G: Scope, D: Data+Send> Partition<G, D> for Stream<G, D> {
    fn partition<K, H>(&self, size: usize, key: K) -> Stream<G, Vec<D>>
    where K: Fn(&D)->H+'static,
          H: Hash+Eq+Data+Clone {
        let mut partitions = HashMap::new();
        
        let (key, exchange) = exchange!(key);

        self.unary_stream(exchange, "Partition", move |input, output| {
            input.for_each(|time, data| {
                data.drain(..).for_each(|dat| {
                    let key = key(&dat);
                    let mut partition = partitions.remove(&key).unwrap_or_else(|| Vec::with_capacity(size));
                    partition.push(dat);
                    if partition.len() == size {
                        output.session(&time).give(partition);
                    } else {
                        partitions.insert(key, partition);
                    }
                });
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
    fn partition() {
        let data = timely::example(|scope| {
            (0..10).to_stream(scope)
                .partition(2, |x| x % 2)
                .capture()
        });
        
        assert_eq!(data.extract()[0].1, vec!(vec!(0, 2),
                                             vec!(1, 3),
                                             vec!(4, 6),
                                             vec!(5, 7)));
    }
}
