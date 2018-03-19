use std::collections::{HashMap,VecDeque};
use std::cmp::min;
use timely::Data;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Unary;
use timely::dataflow::{Stream, Scope};

pub trait Window<G: Scope, D: Data> {
    fn window<F>(&self, size: usize, slide: usize, time: F) -> Stream<G, D>
        where F: Fn(&G::Timestamp, &D)->G::Timestamp+'static;
    fn epoch_window(&self, size: usize, slide: usize) -> Stream<G, D>;
}

impl<G: Scope, D: Data> Window<G, D> for Stream<G, D> {
    fn window<F>(&self, size: usize, slide: usize, time: F) -> Stream<G, D>
    where F: Fn(&G::Timestamp, &D)->G::Timestamp+'static{
        assert!(slide <= size, "The window slide cannot be greater than the window size.");
        let mut window_parts = HashMap::new();
        let mut times = VecDeque::new();
        self.unary_notify(Pipeline, "Window", Vec::new(), move |input, output, notificator| {
            input.for_each(|cap, data| {
                while let Some(data) = data.pop() {
                    let time = time(cap.time(), &data);
                    // Push the data onto a partial window for the current time.
                    let part = window_parts.entry(time.clone()).or_insert_with(|| Vec::new());
                    part.push(data);
                    // Remember this time for reconstruction of partial windows.
                    if !times.contains(&time) {
                        // FIXME: fix for out of order epoch arrival.
                        times.push_back(time.clone());
                    }
                }
                
                notificator.notify_at(cap);
            });
            notificator.for_each(|cap, _, _| {
                let pos = 1 + times.iter().position(|x| x == cap.time()).unwrap_or(size);
                // Only send out data if this is on a complete window.
                if size <= pos && (pos-size) % slide == 0 {
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
    
    fn epoch_window(&self, size: usize, slide: usize) -> Stream<G, D> {
        self.window(size, slide, |t, _| t.clone())
    }
}
