extern crate timely;
mod operators;

use operators::RollingCount;
use operators::EpochWindow;
use timely::dataflow::operators::{Input, Probe, Inspect};

fn main() {
    timely::execute_from_args(std::env::args(), move |worker| {
        let data = vec![
            (0, ('a', 0)),
            (0, ('b', 0)),
            (1, ('a', 1)),
            (1, ('b', 1)),
            (1, ('c', 1)),
            (2, ('a', 2)),
            (3, ('a', 3)),
            (3, ('b', 3)),
            (3, ('c', 3))
        ];

        let index = worker.index();
        let mut input = timely::dataflow::InputHandle::new();

        // let _probe = worker.dataflow(|scope| {
        //     scope.input_from(&mut input)
        //         .rolling_count(|&(name, score)| (name, score))
        //         .inspect(move |&(name, score)| println!("worker {}:\t {}: {}", index, name, score))
        //         .probe()
        // });

        let _probe = worker.dataflow(|scope| {
            scope.input_from(&mut input)
                .epoch_window(2, 2)
                .inspect(move |ref v| println!("worker {}:\t {:?}", index, v))
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
