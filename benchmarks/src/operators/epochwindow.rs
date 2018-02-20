use std::collections::{HashMap,VecDeque};
use std::ops::DerefMut;
use timely::Data;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::unary::Unary;
use timely::dataflow::{Stream, Scope};

pub trait EpochWindow<G: Scope, D: Data> {
    fn epoch_window<>(&self, size: usize, slide: usize) -> Stream<G, D>;
}

impl<G: Scope, D: Data> EpochWindow<G, D> for Stream<G, D> 
{    
    fn epoch_window<>(&self, size: usize, slide: usize) -> Stream<G, D> 
    {
        assert!(slide <= size, "The window slide cannot be greater than the window size.");
        let mut windows = HashMap::new();
        let mut times = VecDeque::new();
        self.unary_notify(Pipeline, "EpochWindow", Vec::new(), move |input, output, notificator| {
            input.for_each(|time, data| {
                // Push the data onto a partial window for the current time.
                let window = windows.entry(time.clone()).or_insert(Vec::new());
                window.append(data.deref_mut());
                // Remember this time for reconstruction of partial windows.
                if !times.contains(&time) {
                    times.push_back(time.clone());
                    // Only notify if we have a full window and slide.
                    if size <= times.len() && (times.len()-size) % slide == 0 {
                        notificator.notify_at(time);
                    }
                }
            });
            notificator.for_each(|time,_,_| {
                // Gather complete window from partial windows.
                let mut window = Vec::new();
                for t in &times {
                    if time.time() < t.time() { break; }
                    for entry in windows.get(&t).unwrap_or(&Vec::new()) {
                        window.push(entry.clone());
                    }
                }
                // Send out the completed window.
                output.session(&time).give_iterator(window.drain(..));
                // Invalidate partial windows that fell out of the slide.
                for _ in 0..slide {
                    let time = times.pop_front().expect("EpochWindow: Could not find time slot to remove.");
                    windows.remove(&time);
                }
            });
        })
    }
}