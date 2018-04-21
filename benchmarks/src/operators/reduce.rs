use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::rc::Rc;
use num::{ToPrimitive, Num};
use timely::Data;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::generic::Unary;
use timely::dataflow::{Stream, Scope};

pub trait Reduce<G: Scope, D: Data+Send> {
    fn reduce<H, V, K, R, D2, C>(&self, key_extractor: K, initial_value: V, reductor: R, completor: C) -> Stream<G, D2>
    where H: Hash+Eq+Data+Clone,
          V: Eq+Data+Clone,
          D2: Data,
          K: Fn(&D)->H+'static,
          R: Fn(D, V)->V+'static,
          C: Fn(H, V, usize)->D2+'static;
    
    fn reduce_by<H, V, K, R>(&self, key_extractor: K, initial_value: V, reductor: R) -> Stream<G, (H, V)>
    where H: Hash+Eq+Data+Clone,
          V: Eq+Data+Clone,
          K: Fn(&D)->H+'static,
          R: Fn(D, V)->V+'static;

    fn average_by<H, V, K, R>(&self, key_extractor: K, reductor: R) -> Stream<G, (H, f32)>
    where H: Hash+Eq+Data+Clone,
          V: Eq+Data+Clone+Num+ToPrimitive,
          K: Fn(&D)->H+'static,
          R: Fn(D)->V+'static;

    fn reduce_to<V, R>(&self, initial_value: V, reductor: R) -> Stream<G, V>
    where V: Eq+Data+Clone,
          R: Fn(D, V)->V+'static;
}

impl<G: Scope, D: Data+Send> Reduce<G, D> for Stream<G, D> {
    fn reduce<H, V, K, R, D2, C>(&self, key_extractor: K, initial_value: V, reductor: R, completor: C) -> Stream<G, D2>
    where H: Hash+Eq+Data+Clone,
          V: Eq+Data+Clone,
          D2: Data,
          K: Fn(&D)->H+'static,
          R: Fn(D, V)->V+'static,
          C: Fn(H, V, usize)->D2+'static{
        let mut epochs = HashMap::new();

        let (key, exchange) = exchange!(key_extractor);

        self.unary_notify(exchange, "Reduce", Vec::new(), move |input, output, notificator| {
            input.for_each(|time, data| {
                let window = epochs.entry(time.clone()).or_insert_with(|| HashMap::new());
                data.drain(..).for_each(|dat|{
                    let key = key(&dat);
                    let (v, c) = window.remove(&key).unwrap_or_else(|| (initial_value.clone(), 0));
                    let value = reductor(dat, v);
                    window.insert(key, (value, c+1));
                });
                notificator.notify_at(time.retain());
            });
            notificator.for_each(|time, _, _| {
                if let Some(mut window) = epochs.remove(&time) {
                    output.session(&time).give_iterator(window.drain().map(|(k, (v, c))| completor(k, v, c)));
                }
            });
        })
    }
    
    fn reduce_by<H, V, K, R>(&self, key_extractor: K, initial_value: V, reductor: R) -> Stream<G, (H, V)>
    where H: Hash+Eq+Data+Clone,
          V: Eq+Data+Clone,
          K: Fn(&D)->H+'static,
          R: Fn(D, V)->V+'static {
        self.reduce(key_extractor, initial_value, reductor, |k, v, _| (k, v))
    }

    fn average_by<H, V, K, R>(&self, key_extractor: K, reductor: R) -> Stream<G, (H, f32)>
    where H: Hash+Eq+Data+Clone,
          V: Eq+Data+Clone+Num+ToPrimitive,
          K: Fn(&D)->H+'static,
          R: Fn(D)->V+'static,{
        self.reduce(key_extractor, V::zero(), move |d, c| reductor(d)+c, |k, v, c| (k, v.to_f32().unwrap()/c as f32))
    }

    fn reduce_to<V, R>(&self, initial_value: V, reductor: R) -> Stream<G, V>
    where V: Eq+Data+Clone,
          R: Fn(D, V)->V+'static {
        let mut epochs = HashMap::new();
        
        self.unary_notify(Exchange::new(|_| 0), "Reduce", Vec::new(), move |input, output, notificator| {
            input.for_each(|time, data| {
                let mut reduced = epochs.remove(&time.time().clone()).unwrap_or_else(|| initial_value.clone());
                while let Some(dat) = data.pop() {
                    reduced = reductor(dat, reduced);
                };
                epochs.insert(time.clone(), reduced);
                notificator.notify_at(time.retain());
            });
            notificator.for_each(|time, _, _| {
                if let Some(reduced) = epochs.remove(&time.time().clone()) {
                    output.session(&time).give(reduced);
                }
            });
        })
    }
}
