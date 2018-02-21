use std::collections::{HashMap,VecDeque};
use std::ops::{DerefMut, Add};
use timely::Data;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::unary::Unary;
use timely::dataflow::{Stream, Scope};

pub trait EpochWindow<G: Scope, D: Data> {
    fn epoch_window<>(&self, size: usize, slide: usize) -> Stream<G, D>;
}

impl<G: Scope, D: Data> EpochWindow<G, D> for Stream<G, D> {
    fn epoch_window<>(&self, size: usize, slide: usize) -> Stream<G, D> {
        assert!(slide <= size, "The window slide cannot be greater than the window size.");
        let mut window_parts = HashMap::new();
        let mut times = VecDeque::new();
        self.unary_notify(Pipeline, "EpochWindow", Vec::new(), move |input, output, notificator| {
            input.for_each(|capability, data| {
                // Push the data onto a partial window for the current time.
                let time = capability.time();
                let part = window_parts.entry(time).or_insert_with(|| Vec::new());
                part.append(data.deref_mut());
                // Remember this time for reconstruction of partial windows.
                if !times.contains(time) {
                    times.push_back(time.clone());
                }
                capability.downgrade(time + if times.len() < size {size} else {slide});
                notificator.notify_at(capability);
            });
            notificator.for_each(|capability, _, _| {
                let time = capability.time();
                let pos = times.iter().position(|x| x == time).unwrap();
                if size == pos {
                    // Gather complete window from partial windows.
                    let mut window = Vec::new();
                    times.iter().take(size).for_each(|time|{
                        if let Some(part) = window_parts.get(time){
                            part.iter().for_each(|entry| window.push(entry.clone()));
                        }
                    });
                    // Send out the completed window.
                    output.session(&capability).give_iterator(window.drain(..));
                    // Invalidate partial windows that fell out of the slide.
                    times.drain(0..slide).for_each(|time|{
                        window_parts.remove(&time);
                    });
                }else{
                    output.session(&capability);
                }
            });
        })
    }
}
