extern crate timely;
mod operators;

use operators::RollingCount;
use timely::dataflow::operators::{Input, Probe, Inspect};

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
                .inspect(move |&(name, score)| println!("worker {}:\t {}: {}", index, name, score))
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
