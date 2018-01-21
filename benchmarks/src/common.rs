use timely::dataflow::{Stream};
use timely::dataflow::scopes::child::Child;
use timely::dataflow::operators::{Input};
use timely::Data;

pub trait Test{
    fn name(&self) -> str;
    fn construct_dataflow(&self, &mut Child) -> ([Input], Stream);
    fn generate_data(&self) -> Option<[Data]> {
        println!("Warning: {} does not implement a data generator.", self.name());
        None
    }
}
