extern crate either;
extern crate timely;
mod operators;

use operators::RollingCount;
use operators::EpochWindow;
use operators::Join;
use timely::dataflow::operators::{Input, Probe, Inspect};
use either::{Either};

fn main() {
    timely::execute_from_args(std::env::args(), move |worker| {
        let data = vec![
            vec![('a', 0), ('b', 0)],
            vec![('a', 1), ('b', 1), ('c', 1)],
            vec![('a', 2)],
            vec![('a', 3), ('b', 3), ('c', 3)],
        ];
        let data2 = vec![
            vec![('a', 2), ('b', 1)],
            vec![('a', 1), ('b', 1), ('c', 1)],
            vec![('a', 2)],
            vec![('a', 1), ('b', 1), ('c', 1)]
        ];

        let index = worker.index();
        let mut input = timely::dataflow::InputHandle::new();
        let mut input2 = timely::dataflow::InputHandle::new();

        // let _probe = worker.dataflow(|scope| {
        //     scope.input_from(&mut input)
        //         .rolling_count(|&(name, score)| (name, score))
        //         .inspect(move |&(name, score)| println!("worker {}:\t {}: {}", index, name, score))
        //         .probe()
        // });

        // let _probe = worker.dataflow(|scope| {
        //     scope.input_from(&mut input)
        //         .epoch_window(2, 2)
        //         .inspect(move |ref v| println!("worker {}:\t {:?}", index, v))
        //         .probe()
        // });

        let stream2 = worker.dataflow(|scope| {
            scope.input_from(&mut input2)
        });

        let _probe = worker.dataflow(|scope| {
            scope.input_from(&mut input)
                .join(&stream2,
                      |dat| dat.either(|&(k, s)| s, |&(k, s)| s),
                      |&(name1, score1), &(name2, score2)| score1+score2)
                .inspect(move |&(name, score)| println!("worker {}:\t {}: {}", index, name, score))
                .probe()
        });

        let mut current_epoch = 0;
        for epoch in 0..data.len() {
            input.advance_to(epoch);
            for dat in data[epoch]{
                input.send(dat);
            }
            input2.advance_to(epoch);
            for dat in data2[epoch]{
                input2.send(dat);
            }
        }
    }).unwrap();
}
