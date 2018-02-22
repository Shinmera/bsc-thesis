use timely::dataflow::scopes::{Root, Child};
use timely::dataflow::{Stream};
use timely_communication::allocator::Generic;
use operators::{EpochWindow};
use test::SimpleTest;

// This is still very verbose.
// Unfortuantely I can't figure out how to make a test
// struct instance with closures work out properly due
// to timely's lifetime constraints. Sigh!
struct HoppingWindowTest {}
impl SimpleTest for HoppingWindowTest {
    type D = usize;
    type DO = usize;

    fn name(&self) -> &str { "Hopping Window Test" }

    fn generate(&self, epoch: &usize) -> Vec<Self::D> {
        vec![*epoch]
    }

    fn construct<'scope>(&self, stream: &mut Stream<Child<'scope, Root<Generic>, usize>, Self::D>) -> Stream<Child<'scope, Root<Generic>, usize>, Self::DO> {
        stream.epoch_window(5, 5)
    }
}

struct SlidingWindowTest {}
impl SimpleTest for SlidingWindowTest {
    type D = usize;
    type DO = usize;

    fn name(&self) -> &str { "Sliding Window Test" }

    fn generate(&self, epoch: &usize) -> Vec<Self::D> {
        vec![*epoch]
    }

    fn construct<'scope>(&self, stream: &mut Stream<Child<'scope, Root<Generic>, usize>, Self::D>) -> Stream<Child<'scope, Root<Generic>, usize>, Self::DO> {
        stream.epoch_window(5, 2)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test::run_test;
    use config::Config;

    // FIXME: These aren't really proper tests at this point because
    //        the results from the dataflow are simply printed rather
    //        than gathered and compared against an expected value.
    #[test]
    fn test_hop() {
        run_test(&Config::new(), Box::new(HoppingWindowTest{})).unwrap();
    }

    #[test]
    fn test_slide() {
        run_test(&Config::new(), Box::new(SlidingWindowTest{})).unwrap();
    }
}
