use timely::Data;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::unary::Unary;
use timely::dataflow::{Stream, Scope};

pub trait FilterMap<G: Scope, D: Data> {
    fn filter_map<DO: Data, K: Fn(D)->Option<DO>+'static>(&self, map: K) -> Stream<G, DO>;
}

impl<G: Scope, D: Data> FilterMap<G, D> for Stream<G, D> {
    fn filter_map<DO: Data, K: Fn(D)->Option<DO>+'static>(&self, map: K) -> Stream<G, DO> {
        self.unary_stream(Pipeline, "FilterMap", move |input, output| {
            input.for_each(|time, data| {
                let mut session = output.session(&time);
                data.drain(..).for_each(|x|{
                    if let Some(d) = map(x) {
                        session.give(d);
                    }
                });
            });
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use timely;
    use timely::dataflow::operators::{ToStream, Capture};
    use timely::dataflow::operators::capture::Extract;
    use std::str::FromStr;
    
    #[test]
    fn filtermap() {
        let data = timely::example(|scope| {
            vec!(String::from("1"),
                 String::from("2"),
                 String::from("a"),
                 String::from("d"),
                 String::from("3"))
                .to_stream(scope)
                .filter_map(|s| i32::from_str(&s).ok())
                .capture()
        });
        
        assert_eq!(data.extract()[0].1, vec!(1, 2, 3));
    }
}
