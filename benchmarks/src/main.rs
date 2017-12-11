extern crate timely;

use std::cmp::Eq;
use std::collections::HashMap;
use std::hash::Hash;
use timely::Data;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::unary::Unary;
use timely::dataflow::operators::{Input, Probe, Inspect};
use timely::dataflow::{Stream, Scope};

pub trait RollingCount<G: Scope, D: Data> {
    fn rolling_count<H: Hash+Eq+Data, K: Fn(&D)->(H, usize)+'static>(&self, key_extractor: K) -> Stream<G, (D, usize)>;
}

impl<G: Scope, D: Data> RollingCount<G, D> for Stream<G, D> {
    fn rolling_count<H: Hash+Eq+Data, K: Fn(&D)->(H, usize)+'static>(&self, key_extractor: K) -> Stream<G, (D, usize)> {
        let mut counts = HashMap::new();
        self.unary_stream(Pipeline, "RollingCount", move |input, output| {
            input.for_each(|time, data| {
                output.session(&time).give_iterator(data.drain(..).map(|x|{
                    let (key, inc) = key_extractor(&x);
                    let count = counts.get(&key).unwrap_or(&0)+inc;
                    counts.insert(key, count);
                    (x, count)
                }));
            });
        })
    }
}

fn main() {
    timely::execute_from_args(std::env::args(), move |worker| {
        let data = vec![
            (0, ('a', 1)),
            (0, ('b', 2)),
            (1, ('a', 10)),
            (1, ('b', 20)),
            (1, ('c', 30)),
        ];

        let index = worker.index();
        let mut input = timely::dataflow::InputHandle::new();

        let _probe = worker.dataflow(|scope| {
            scope.input_from(&mut input)
                .rolling_count(|&(name, score)| (name, score))
                .inspect(move |&((name, _), score)| println!("worker {}:\t {}: {}", index, name, score))
                .probe()
        });

        let mut current_epoch = 0;
        for (epoch, data) in data {
            if epoch != current_epoch{
                input.advance_to(epoch);
            }
            current_epoch = epoch;
            input.send(data);
        }
    }).unwrap();
}
