use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use timely::dataflow::operators::{Input, Exchange, Map};
use timely::dataflow::operators::input::Handle;
use timely::dataflow::scopes::{Root, Child};
use timely::dataflow::{Stream};
use timely_communication::allocator::Generic;
use operators::RollingCount;
use operators::EpochWindow;
use test::Test;
use test::TestImpl;

fn hasher(x: &String) -> u64 {
    let mut s = DefaultHasher::new();
    x.hash(&mut s);
    s.finish()
}

struct Identity {}
impl TestImpl for Identity {
    type D = String;
    type DO = String;
    type T = usize;

    fn new(_args: &[String]) -> Self { Identity{} }
    
    fn name(&self) -> &str { "Identity" }

    fn initial_epoch(&self) -> Self::T { 0 }

    fn construct_dataflow<'scope>(&self, scope: &mut Child<'scope, Root<Generic>, Self::T>) -> (Stream<Child<'scope, Root<Generic>, Self::T>, Self::DO>, Vec<Handle<Self::T, Self::D>>) {
        let (input, stream) = scope.new_input();
        (stream, vec![input])
    }
}

struct Repartition {}
impl TestImpl for Repartition {
    type D = String;
    type DO = String;
    type T = usize;

    fn new(_args: &[String]) -> Self { Repartition{} }
    
    fn name(&self) -> &str { "Repartition" }

    fn initial_epoch(&self) -> Self::T { 0 }
    
    fn construct_dataflow<'scope>(&self, scope: &mut Child<'scope, Root<Generic>, Self::T>) -> (Stream<Child<'scope, Root<Generic>, Self::T>, Self::DO>, Vec<Handle<Self::T, Self::D>>) {
        let (input, stream) = scope.new_input();
        let stream = stream
            .exchange(hasher);
        (stream, vec![input])
    }
}

struct Wordcount {}
impl TestImpl for Wordcount {
    type D = String;
    type DO = (String, usize);
    type T = usize;

    fn new(_args: &[String]) -> Self { Wordcount{} }
    
    fn name(&self) -> &str { "Wordcount" }

    fn initial_epoch(&self) -> Self::T { 0 }

    fn construct_dataflow<'scope>(&self, scope: &mut Child<'scope, Root<Generic>, Self::T>) -> (Stream<Child<'scope, Root<Generic>, Self::T>, Self::DO>, Vec<Handle<Self::T, Self::D>>) {
        let (input, stream) = scope.new_input();
        let stream = stream
            .flat_map(|text: String| text.split_whitespace().map(|x| String::from(x)).collect::<Vec<_>>())
            .exchange(hasher)
            .rolling_count(|word| (word.clone(), 1));
        (stream, vec![input])
    }
}

struct Fixwindow {}
impl TestImpl for Fixwindow {
    type D = usize;
    type DO = usize;
    type T = usize;

    fn new(_args: &[String]) -> Self { Fixwindow{} }
    
    fn name(&self) -> &str { "Fixwindow" }

    fn initial_epoch(&self) -> Self::T { 0 }

    fn construct_dataflow<'scope>(&self, scope: &mut Child<'scope, Root<Generic>, Self::T>) -> (Stream<Child<'scope, Root<Generic>, Self::T>, Self::D>, Vec<Handle<Self::T, Self::D>>) {
        let (input, stream) = scope.new_input();
        let stream = stream
            .epoch_window(100, 100)
            .map(|data| data.iter().sum());
        (stream, vec![input])
    }
}

pub fn hibench(args: &[String]) -> Vec<Box<Test>>{
    vec![Box::new(Identity::new(args)),
         Box::new(Repartition::new(args)),
         Box::new(Wordcount::new(args)),
         Box::new(Fixwindow::new(args))]
}
