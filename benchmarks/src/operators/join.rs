use std::collections::HashMap;
use std::hash::Hash;
use std::cmp::min;
use timely::Data;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::Binary;
use timely::dataflow::{Stream, Scope};
use either::{Either, Left, Right};

pub trait Join<G: Scope, D1: Data> {
    fn join<H, D2, D3, K, J>(&self, stream: &Stream<G, D2>, key_extractor: K, joiner: J) -> Stream<G, D3>
        where H: Hash+Eq+Data+Clone,
              D2: Data, D3: Data,
              K: Fn(Either<&D1,&D2>)->H+'static,
              J: Fn(&D1, &D2)->D3+'static;
}

impl<G: Scope, D1: Data> Join<G, D1> for Stream<G, D1> {
    fn join<H, D2, D3, K, J>(&self, stream: &Stream<G, D2>, key_extractor: K, joiner: J) -> Stream<G, D3>
        where H: Hash+Eq+Data+Clone,
              D2: Data, D3: Data,
              K: Fn(Either<&D1,&D2>)->H+'static,
              J: Fn(&D1, &D2)->D3+'static{
        let mut epoch1 = HashMap::new();
        let mut epoch2 = HashMap::new();
        
        self.binary_notify(stream, Pipeline, Pipeline, "Join", Vec::new(), move |input1, input2, output, notificator| {
            input1.for_each(|time, data|{
                let epoch = epoch1.entry(time.clone()).or_insert(HashMap::new());
                while let Some(dat) = data.pop(){
                    let key = key_extractor(Left(&dat));
                    let datavec = epoch.entry(key).or_insert(Vec::new());
                    datavec.push(dat);
                }
                notificator.notify_at(time);
            });
            
            input2.for_each(|time, data|{
                let epoch = epoch2.entry(time.clone()).or_insert(HashMap::new());
                while let Some(dat) = data.pop(){
                    let key = key_extractor(Right(&dat));
                    let datavec = epoch.entry(key).or_insert(Vec::new());
                    datavec.push(dat);
                }
                notificator.notify_at(time);
            });
            
            notificator.for_each(|time, _, _|{
                let k1 = epoch1.get(&time).unwrap_or(&HashMap::new());
                let k2 = epoch2.get(&time).unwrap_or(&HashMap::new());
                let out = output.session(&time);
                for (key, data1) in k1{
                    k2.get(key).and_then(|data2| {
                        for i in 0..min(data1.len(), data2.len()){
                            out.give(joiner(&data1[i], &data2[i]));
                        }
                        None
                    });
                }
            });
        })
    }
}
