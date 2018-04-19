use std::collections::{HashMap};
use std::hash::{Hash, Hasher};
use std::rc::Rc;
use std::ops::{Add,Sub};
use timely::Data;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::Unary;
use timely::dataflow::{Stream, Scope};
use timely::progress::nested::product::Product;
use timely::progress::timestamp::{Timestamp,RootTimestamp};

pub trait Relative<N> {
    fn add(self, other: N) -> Self;
    fn sub(self, other: N) -> Self;
}

// Fucking rust.
// impl<N> Add<N> for Relative<N> {
//     type Output = Relative<N>;
    
//     fn add(self, rhs: N) -> Relative<N> { Relative::add(self, rhs) }
// }

// impl<N> Sub<N> for Relative<N> {
//     type Output = Relative<N>;
    
//     fn sub(self, rhs: N) -> Relative<N> { Relative::sub(self, rhs) }
// }

impl<N: Add<Output=N>+Sub<Output=N>+Timestamp> Relative<N> for Product<RootTimestamp, N> {
    fn add(self, other: N) -> Product<RootTimestamp, N> {
        RootTimestamp::new(self.inner + other)
    }
    fn sub(self, other: N) -> Product<RootTimestamp, N> {
        RootTimestamp::new(self.inner - other)
    }
}

pub trait Session<G: Scope, D: Data+Send> {
    fn session<W, H>(&self, timeout: usize, sessioner: W) -> Stream<G, (H, Vec<D>)>
    where W: Fn(&D)->(H, G::Timestamp)+'static,
          H: Hash+Eq+Data+Clone,
          G::Timestamp: Relative<usize>+Hash;
}

impl<G: Scope, D: Data+Send> Session<G, D> for Stream<G, D> {
    fn session<W, H>(&self, timeout: usize, sessioner: W) -> Stream<G, (H, Vec<D>)>
    where W: Fn(&D)->(H, G::Timestamp)+'static,
          H: Hash+Eq+Data+Clone,
          G::Timestamp: Relative<usize>+Hash {
        let mut sessions = HashMap::new();

        let (key, exchange) = exchange!(sessioner, |(s, _)| s);
        
        self.unary_notify(exchange, "Session", Vec::new(), move |input, output, notificator| {
            input.for_each(|cap, data| {
                for data in data.drain(..){
                    let (s, t) = key(&data);
                    notificator.notify_at(cap.delayed(&(t.clone().add(timeout))));
                    let session = sessions
                        .entry(t).or_insert_with(HashMap::new)
                        .entry(s).or_insert_with(Vec::new);
                    session.push(data);
                };
            });
            
            notificator.for_each(|cap, _, _| {
                // For each session at the original time we need to check if it
                // has expired, or if we need to delay.
                let otime = cap.time().clone().sub(timeout);
                let mut expired = sessions.remove(&otime).unwrap_or_else(HashMap::new);
                expired.drain().for_each(|(s, mut d)| {
                    // Now we check backwards from the currently expired epoch.
                    let mut found = false;
                    for i in 0..timeout {
                        let t = cap.time().clone().sub(i);
                        if let Some(session) = sessions.get_mut(&t) {
                            if let Some(data) = session.get_mut(&s) {
                                // If we find data within the timeout, delay our data to
                                // that later time. If that time does not happen to be
                                // final either, both this and that data will get moved
                                // ahead even further automatically.
                                
                                // FIXME: This causes events to be out of order.
                                //        That may or may not be important.
                                data.append(&mut d);
                                found = true;
                                break;
                            }
                        }
                    }
                    if !found {
                        // If we don't find a any data within the timeout, the
                        // session is full and we can output it.
                        output.session(&cap).give((s, d));
                    }
                });
            });
        })
    }
}
