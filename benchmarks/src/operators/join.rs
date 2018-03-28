use std::collections::HashMap;
use std::hash::Hash;
use timely::Data;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::Binary;
use timely::dataflow::{Stream, Scope};

pub trait Join<G: Scope, D1: Data> {
    fn epoch_join<H, D2, D3, K1, K2, J>(&self, stream: &Stream<G, D2>, key_1: K1, key_2: K2, joiner: J) -> Stream<G, D3>
    where H: Hash+Eq+Data+Clone,
          D2: Data, D3: Data,
          K1: Fn(&D1)->H+'static,
          K2: Fn(&D2)->H+'static,
          J: Fn(D1, D2)->D3+'static;

    fn left_join<H, D2, D3, K1, K2, J>(&self, stream: &Stream<G, D2>, key_1: K1, key_2: K2, joiner: J) -> Stream<G, D3>
    where H: Hash+Eq+Data+Clone,
          D2: Data, D3: Data,
          K1: Fn(&D1)->H+'static,
          K2: Fn(&D2)->H+'static,
          J: Fn(D1, D2)->D3+'static;
}

impl<G: Scope, D1: Data> Join<G, D1> for Stream<G, D1> {
    fn epoch_join<H, D2, D3, K1, K2, J>(&self, stream: &Stream<G, D2>, key_1: K1, key_2: K2, joiner: J) -> Stream<G, D3>
    where H: Hash+Eq+Data+Clone,
          D2: Data, D3: Data,
          K1: Fn(&D1)->H+'static,
          K2: Fn(&D2)->H+'static,
          J: Fn(D1, D2)->D3+'static{
        let mut epoch1 = HashMap::new();
        let mut epoch2 = HashMap::new();
        
        self.binary_notify(stream, Pipeline, Pipeline, "Join", Vec::new(), move |input1, input2, output, notificator| {
            input1.for_each(|time, data|{
                let epoch = epoch1.entry(time.clone()).or_insert_with(||HashMap::new());
                while let Some(dat) = data.pop(){
                    let key = key_1(&dat);
                    let datavec = epoch.entry(key).or_insert_with(||Vec::new());
                    datavec.push(dat);
                }
                notificator.notify_at(time);
            });
            
            input2.for_each(|time, data|{
                let epoch = epoch2.entry(time.clone()).or_insert_with(||HashMap::new());
                while let Some(dat) = data.pop(){
                    let key = key_2(&dat);
                    let datavec = epoch.entry(key).or_insert_with(||Vec::new());
                    datavec.push(dat);
                }
                notificator.notify_at(time);
            });
            
            notificator.for_each(|time, _, _|{
                if let Some(k1) = epoch1.remove(&time) {
                    if let Some(mut k2) = epoch2.remove(&time) {
                        let mut out = output.session(&time);
                        for (key, mut data1) in k1{
                            if let Some(mut data2) = k2.remove(&key) {
                                data1.drain(..).zip(data2.drain(..)).for_each(|(data1, data2)|{
                                    out.give(joiner(data1, data2));
                                });
                            }
                        }
                    }
                } else {
                    epoch2.remove(&time);
                }
            });
        })
    }

    fn left_join<H, D2, D3, K1, K2, J>(&self, stream: &Stream<G, D2>, key_1: K1, key_2: K2, joiner: J) -> Stream<G, D3>
    where H: Hash+Eq+Data+Clone,
          D2: Data, D3: Data,
          K1: Fn(&D1)->H+'static,
          K2: Fn(&D2)->H+'static,
          J: Fn(D1, D2)->D3+'static {
        let mut d1s = HashMap::new();
        let mut d2s = HashMap::new();

        self.binary_notify(stream, Pipeline, Pipeline, "Join", Vec::new(), move |input1, input2, output, _| {
            input1.for_each(|time, data|{
                data.drain(..).for_each(|d1| {
                    let k1 = key_1(&d1);
                    if let Some(d2) = d2s.remove(&k1) {
                        output.session(&time).give(joiner(d1.clone(), d2));
                    }
                    d1s.insert(k1, d1);
                });
            });

            input2.for_each(|time, data|{
                data.drain(..).for_each(|d2| {
                    let k2 = key_2(&d2);
                    if let Some(d1) = d1s.get(&k2) {
                        output.session(&time).give(joiner(d1.clone(), d2));
                    } else {
                        d2s.insert(k2, d2);
                    }
                });
            });
        })
    }
}
