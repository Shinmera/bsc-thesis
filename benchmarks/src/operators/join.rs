use std::collections::HashMap;
use std::hash::Hash;
use std::cmp::min;
use timely::Data;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::Binary;
use timely::dataflow::{Stream, Scope};

pub trait Join<G: Scope, D1: Data> {
    fn join<H, D2, D3, K1, K2, J>(&self, stream: &Stream<G, D2>, key_1: K1, key_2: K2, joiner: J) -> Stream<G, D3>
        where H: Hash+Eq+Data+Clone,
              D2: Data, D3: Data,
              K1: Fn(&D1)->H+'static,
              K2: Fn(&D2)->H+'static,
              J: Fn(&D1, &D2)->D3+'static;
}

impl<G: Scope, D1: Data> Join<G, D1> for Stream<G, D1> {
    fn join<H, D2, D3, K1, K2, J>(&self, stream: &Stream<G, D2>, key_1: K1, key_2: K2, joiner: J) -> Stream<G, D3>
        where H: Hash+Eq+Data+Clone,
              D2: Data, D3: Data,
              K1: Fn(&D1)->H+'static,
              K2: Fn(&D2)->H+'static,
              J: Fn(&D1, &D2)->D3+'static{
        let mut epoch1 = HashMap::new();
        let mut epoch2 = HashMap::new();
        
        self.binary_notify(stream, Pipeline, Pipeline, "Join", Vec::new(), move |input1, input2, output, notificator| {
            input1.for_each(|time, data|{
                let epoch = epoch1.entry(time.clone()).or_insert(HashMap::new());
                while let Some(dat) = data.pop(){
                    let key = key_1(&dat);
                    let datavec = epoch.entry(key).or_insert(Vec::new());
                    datavec.push(dat);
                }
                notificator.notify_at(time);
            });
            
            input2.for_each(|time, data|{
                let epoch = epoch2.entry(time.clone()).or_insert(HashMap::new());
                while let Some(dat) = data.pop(){
                    let key = key_2(&dat);
                    let datavec = epoch.entry(key).or_insert(Vec::new());
                    datavec.push(dat);
                }
                notificator.notify_at(time);
            });
            
            notificator.for_each(|time, _, _|{
                if let Some(k1) = epoch1.remove(&time) {
                    if let Some(k2) = epoch2.remove(&time) {
                        let mut out = output.session(&time);
                        for (key, data1) in k1{
                            if let Some(data2) = k2.get(&key) {
                                for i in 0..min(data1.len(), data2.len()){
                                    out.give(joiner(&data1[i], &data2[i]));
                                }
                            }
                        }
                    }
                } else {
                    epoch2.remove(&time);
                }
            });
        })
    }
}
