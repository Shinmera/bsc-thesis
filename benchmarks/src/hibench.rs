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

    fn construct_dataflow<'scope>(&self, scope: &mut Child<'scope, Root<Generic>, Self::T>) -> (Stream<Child<'scope, Root<Generic>, Self::T>, Self::D>, Vec<Handle<Self::T, Self::D>>) {
        let (input, stream) = scope.new_input();
        (stream, vec![input])
    }
}

// struct Repartition {}
// impl TestImpl for Repartition {
//     type D = String;
//     type T = usize;
    
//     fn name(&self) -> &str { "Repartition" }

//     fn initial_epoch(&self) -> Self::T { 0 }
    
//     fn construct_dataflow<'scope>(&self, scope: &mut Child<'scope, Root<Generic>, Self::T>) -> (Stream<Child<'scope, Root<Generic>, Self::T>, Self::D>, Vec<Handle<Self::T, Self::D>>) {
//         let (input, stream) = scope.new_input();
//         let stream = stream
//             .exchange(|&x| x);
//         (stream, vec![input])
//     }
// }

// struct Wordcount {}
// impl TestImpl for Wordcount {
//     type D = String;
//     type T = usize;
    
//     fn name(&self) -> &str { "Wordcount" }

//     fn initial_epoch(&self) -> Self::T { 0 }

//     fn construct_dataflow<'scope>(&self, scope: &mut Child<'scope, Root<Generic>, Self::T>) -> (Stream<Child<'scope, Root<Generic>, Self::T>, Self::D>, Vec<Handle<Self::T, Self::D>>) {
//         let (input, stream) = scope.new_input();
//         let stream = stream
//             .flat_map(|text| text.split_whitespace())
//             .exchange(|word| word)
//             .rolling_count(|word| (word, 1));
//         (stream, vec![input])
//     }
// }

// struct Fixwindow {}
// impl TestImpl for Fixwindow {
//     type D = String;
//     type T = usize;
    
//     fn name(&self) -> &str { "Fixwindow" }

//     fn initial_epoch(&self) -> Self::T { 0 }

//     fn construct_dataflow<'scope>(&self, scope: &mut Child<'scope, Root<Generic>, Self::T>) -> (Stream<Child<'scope, Root<Generic>, Self::T>, Self::D>, Vec<Handle<Self::T, Self::D>>) {
//         let (input, stream) = scope.new_input();
//         let stream = stream
//             .epoch_window(1, 1)
//             .map(|data| data.iter().sum());
//         (stream, vec![input])
//     }
// }

pub fn hibench() -> Vec<Box<Test>>{
    vec![Box::new(Identity{})]
}
