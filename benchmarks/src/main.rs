extern crate timely;
mod operators;

use operators::RollingCount;
use operators::EpochWindow;
use operators::Join;
use timely::dataflow::operators::{Input, Probe, Inspect};

fn main() {
    timely::execute_from_args(std::env::args(), move |worker| {
        let mut data1 = vec![
            vec![('a', 0), ('b', 0)],
            vec![('a', 1), ('b', 1), ('c', 1)],
            vec![('a', 2)],
            vec![('a', 3), ('b', 3), ('c', 3)],
        ];
        let mut data2 = vec![
            vec![('a', 2), ('b', 1)],
            vec![('a', 1), ('b', 1), ('c', 1)],
            vec![('a', 2)],
            vec![('a', 1), ('b', 1), ('c', 1)]
        ];

        let index = worker.index();

        let (mut input1, mut input2, _probe) = worker.dataflow(move |scope| {
            let (input1, stream1) = scope.new_input();
            let (input2, stream2) = scope.new_input();
            let probe = stream1
                .join(&stream2,
                      |&(name, _)| name, |&(name, _)| name,
                      |&(name, s1), &(_, s2)| (name, s1+s2))
                .inspect(move |&(name, score)| println!("worker {}:\t {}: {}", index, name, score))
                .probe();
            (input1, input2, probe)
        });

        for epoch in 0..data1.len() {
            input1.advance_to(epoch);
            for dat in data1.pop().unwrap(){
                input1.send(dat);
            }
            input2.advance_to(epoch);
            for dat in data2.pop().unwrap(){
                input2.send(dat);
            }
        }
        input1.close();
        input2.close();
    }).unwrap();
}
