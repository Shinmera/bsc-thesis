use operators::RollingCount;
use operators::EpochWindow;
use timely::dataflow::operators::{Input, Exchange};
use timely::dataflow::scopes::child::Child;
use timely::dataflow::Stream;
use test::Test;

struct Identity {}
impl Test for Identity {
    fn name(&self) -> str { "Identity" }

    fn construct_dataflow(&self, scope: &mut Child) -> ([Input], Stream) {
        let (input, stream) = scope.new_input();
        (vec![input], stream)
    }
}

struct Repartition {}
impl Test for Repartition {
    fn name(&self) -> str { "Repartition" }
    
    fn construct_dataflow(&self, scope: &mut Child) -> ([Input], Stream) {
        let (input, stream) = scope.new_input();
        let stream = stream
            .exchange(|&x| x);
        (vec![input], stream)
    }
}

struct Wordcount {}
impl Test for Wordcount {
    fn name(&self) -> str { "Wordcount" }

    fn construct_dataflow(&self, scope: &mut Child) -> ([Input], Stream) {
        let (input, stream) = scope.new_input();
        let stream = stream
            .flat_map(|text| text.split_whitespace())
            .exchange(|word| word)
            .rolling_count(|word| (word, 1));
        (vec![input], stream)
    }
}

struct Fixwindow {}
impl Test for Fixwindow {
    fn name(&self) -> str { "Fixwindow" }

    fn construct_dataflow(&self, scope: &mut Child) -> ([Input], Stream) {
        let (input, stream) = scope.new_input();
        let stream = stream
            .epoch_window(1, 1)
            .map(|data| data.iter().sum());
        (vec![input], stream)
    }
}

pub fn hibench() -> [Test]{
    vec![
        Identity{},
        Repartition{},
        Wordcount{},
        Fixwindow{}
    ]
}
