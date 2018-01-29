use operators::RollingCount;
use operators::EpochWindow;
use timely::dataflow::operators::{Input, Exchange};
use timely::dataflow::operators::input::Handle;
use timely::dataflow::scopes::{Root, Child};
use timely::dataflow::{Stream};
use timely_communication::{Allocate};
use timely_communication::allocator::Generic;
use test::Test;
use test::TestImpl;

struct Identity {}
impl TestImpl for Identity {
    type D = String;
    type T = usize;
    
    fn name(&self) -> &str { "Identity" }

    fn initial_epoch(&self) -> Self::T { 0 }

    fn next_epoch(&self, epoch: Self::T) -> Self::T { epoch+1 }

    fn construct_dataflow(&self, scope: &mut Child<Root<Generic>, Self::T>) -> (Stream<Child<Root<Generic>, Self::T>, Self::D>, Vec<Handle<Self::T, Self::D>>) {
        let (input, stream) = scope.new_input();
        (stream, vec![input])
    }
}

// struct Repartition {}
// impl<A: Allocate> Test<A> for Repartition{}
// impl<A: Allocate> TestImpl<A> for Repartition {
//     type D = String;
//     type T = usize;
    
//     fn name(&self) -> str { "Repartition" }
    
//     fn construct_dataflow(&self, scope: &mut Child<Self::G, Self::T>) -> (Stream<Self::G, Self::D>, [Box<Handle<Self::T, Self::D>>]) {
//         let (input, stream) = scope.new_input();
//         let stream = stream
//             .exchange(|&x| x);
//         (vec![input], stream)
//     }
// }

// struct Wordcount {}
// impl<A: Allocate> Test<A> for Wordcount{}
// impl<A: Allocate> TestImpl<A> for Wordcount {
//     type D = String;
//     type T = usize;
    
//     fn name(&self) -> str { "Wordcount" }

//     fn construct_dataflow(&self, scope: &mut Child<Self::G, Self::T>) -> (Stream<Self::G, Self::D>, [Box<Handle<Self::T, Self::D>>]) {
//         let (input, stream) = scope.new_input();
//         let stream = stream
//             .flat_map(|text| text.split_whitespace())
//             .exchange(|word| word)
//             .rolling_count(|word| (word, 1));
//         (vec![input], stream)
//     }
// }

// struct Fixwindow {}
// impl<A: Allocate> Test<A> for Fixwindow{}
// impl<A: Allocate> TestImpl<A> for Fixwindow {
//     type D = String;
//     type T = usize;
    
//     fn name(&self) -> str { "Fixwindow" }

//     fn construct_dataflow(&self, scope: &mut Child<Self::G, Self::T>) -> (Stream<Self::G, Self::D>, [Box<Handle<Self::T, Self::D>>]) {
//         let (input, stream) = scope.new_input();
//         let stream = stream
//             .epoch_window(1, 1)
//             .map(|data| data.iter().sum());
//         (vec![input], stream)
//     }
// }

pub fn hibench() -> Vec<Box<Test>>{
    vec![Box::new(Identity{})]
}
