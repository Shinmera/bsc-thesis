use std::collections::HashMap;
use std::ops::DerefMut;
use timely::Data;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::unary::Unary;
use timely::dataflow::{Stream, Scope};

pub trait EpochWindow<G: Scope, D: Data> {
    fn epoch_window<>(&self, size: usize, slide: usize) -> Stream<G, Vec<D>>;
}

impl<G: Scope, D: Data> EpochWindow<G, D> for Stream<G, D> {
    fn epoch_window<>(&self, size: usize, slide: usize) -> Stream<G, Vec<D>> {
        // Note: size and slide currently ignored, for now just implementing size 1 slide 1.
        let mut windows = HashMap::new();
        self.unary_notify(Pipeline, "EpochWindow", Vec::new(), move |input, output, notificator| {
            input.for_each(|time, data| {
                let mut window = windows.entry(&time).or_insert(&Vec::new());
                window.append(data.deref_mut());
                notificator.notify_at(time);
            });
            notificator.for_each(|time,_,_| {
                let mut window = windows.remove(&time).unwrap_or(&Vec::new());
                output.session(&time).give(window);
            });
        })
    }
}
