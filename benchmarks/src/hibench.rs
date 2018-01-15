use operators::RollingCount;
use operators::EpochWindow;
use operators::Join;
use timely::dataflow::scopes::root::Root;
use timely::dataflow::{Stream, Scope};
use timely::dataflow::operators::{Input};

fn identity(worker: &mut Root) -> ([Input], Stream) {
    worker.dataflow(move |scope| {
        let (input, stream) = scope.new_input();
        (vec![input], stream)
    })
}

fn repartition(worker: &mut Root) -> ([Input], Stream) {
    worker.dataflow(move |scope| {
        let (input, stream) = scope.new_input();
        (vec![input], stream)
    })
}

fn wordcount(worker: &mut Root) -> ([Input], Stream) {
    worker.dataflow(move |scope| {
        let (input, stream) = scope.new_input();
        let stream = stream
            .flat_map(|text| text.split_whitespace())
            .rolling_count(|word| (word, 1));
        (vec![input], stream)
    })
}

fn fixwindow(worker: &mut Root) -> ([Input], Stream){
    worker.dataflow(move |scope| {
        let (input, stream) = scope.new_input();
        let stream = stream
            .epochwindow(n, n)
            .map(|data| data.iter().sum());
        (vec![input], stream)
    })
}

pub fn hibench() -> [([Input], Stream)]{
    vec![
        identity(),
        repartition(),
        wordcount(),
        fixwindow()
    ]
}
