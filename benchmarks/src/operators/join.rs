use std::collections::HashMap;
use std::hash::Hash;
use timely::Data;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::Binary;
use timely::dataflow::{Stream, Scope};

pub trait Join<G: Scope, D: Data> {
    fn join<H: Hash+Eq+Data+Clone, D2: Data, D3: Data, K: Fn(&D)->H>(&self, stream: &Stream<G, D2>, key_extractor: K) -> Stream<G, D3>;
}

impl<G: Scope, D: Data> Join<G, D> for Stream<G, D> {
    fn join<H: Hash+Eq+Data+Clone, D2: Data, D3: Data, K: Fn(&D)->H>(&self, stream: &Stream<G, D2>, key_extractor: K) -> Stream<G, D3>{
        
        self.binary_notify(stream, Pipeline, Pipeline, "Join", Vec::new(), move |input1, input2, output, notificator| {
            input1.for_each(|time, data|{

            });
            input2.for_each(|time, data|{
                
            });
            notificator.for_each(|time, _, _|{
                //output.session(&time).give();
            });
        })
    }
}
