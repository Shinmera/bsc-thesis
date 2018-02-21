use std::collections::{HashMap,VecDeque};
use std::ops::DerefMut;
use std::cmp::min;
use timely::Data;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::unary::Unary;
use timely::dataflow::{Stream, Scope};

pub trait EpochWindow<G: Scope, D: Data> {
    fn epoch_window<T>(&self, size: usize, slide: usize, timestepper: T) -> Stream<G, D>
        where T: Fn(&G::Timestamp, usize)->G::Timestamp+'static;
}

impl<G: Scope, D: Data> EpochWindow<G, D> for Stream<G, D> {
    fn epoch_window<T>(&self, size: usize, slide: usize, timestepper: T) -> Stream<G, D>
    where T: Fn(&G::Timestamp, usize)->G::Timestamp+'static {
        assert!(slide <= size, "The window slide cannot be greater than the window size.");
        let mut window_parts = HashMap::new();
        let mut times = VecDeque::new();
        self.unary_notify(Pipeline, "EpochWindow", Vec::new(), move |input, output, notificator| {
            input.for_each(|mut cap, data| {
                {
                    let time = cap.time();
                    // Push the data onto a partial window for the current time.
                    let part = window_parts.entry(time.clone()).or_insert_with(|| Vec::new());
                    part.append(data.deref_mut());
                    // Remember this time for reconstruction of partial windows.
                    if !times.contains(time) {
                        times.push_back(time.clone());
                    }
                }
                let next = timestepper(&cap, if times.len() < size {size} else {slide});
                cap.downgrade(&next);
                notificator.notify_at(cap);
            });
            notificator.for_each(|cap, _, _| {
                let time = cap.time();
                // Apparently we may get notified of a time that is not even in the
                // times vector yet when the stream ends. In that case we just
                // pretend we have a full window to flush everything.
                let pos = times.iter().position(|x| x == time).unwrap_or(size);
                if size == pos {
                    // Gather complete window from partial windows.
                    let mut window = Vec::new();
                    times.iter().take(size).for_each(|time|{
                        if let Some(part) = window_parts.get(time){
                            part.iter().for_each(|entry| window.push(entry.clone()));
                        }
                    });
                    // Send out the completed window.
                    output.session(&cap).give_iterator(window.drain(..));
                    // Invalidate partial windows that fell out of the slide.
                    let count = min(slide, times.len());
                    times.drain(0..count).for_each(|time|{
                        window_parts.remove(&time);
                    });
                }
            });
        })
    }
}
