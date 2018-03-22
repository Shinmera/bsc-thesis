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
