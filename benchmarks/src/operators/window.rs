use std::collections::HashMap;
use timely::Data;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Unary;
use timely::dataflow::{Stream, Scope};
use super::IntegerTimestamp;

pub trait Window<G: Scope, D: Data>
where G::Timestamp: IntegerTimestamp {
    fn window<F>(&self, size: usize, slide: usize, time: F) -> Stream<G, D>
    where F: Fn(&G::Timestamp, &D)->G::Timestamp+'static;
    fn tumbling_window(&self, size: usize) -> Stream<G, D>;
    fn epoch_window(&self, size: usize, slide: usize) -> Stream<G, D>;
}

impl<G: Scope, D: Data> Window<G, D> for Stream<G, D>
where G::Timestamp: IntegerTimestamp {
    fn window<F>(&self, size: usize, slide: usize, time: F) -> Stream<G, D>
    where F: Fn(&G::Timestamp, &D)->G::Timestamp+'static{
        
        let mut window_parts = HashMap::new();
        let mut start_epoch = None;
        self.unary_notify(Pipeline, "Window", Vec::new(), move |input, output, notificator| {
            input.for_each(|cap, data| {
                data.drain(..).for_each(|data|{
                    let time = time(cap.time(), &data).to_integer();
                    // Record the starting epoch in case we don't begin at zero.
                    if start_epoch.is_none() { start_epoch = Some(time); }
                    // Push the data onto a partial window for the current time.
                    let part = window_parts.entry(time).or_insert_with(Vec::new);
                    part.push(data);
                    // Calculate the next epochs on which this partial window would
                    // be output in a slide, then notify on those times. We need to
                    // do this for every possible future slide within the window
                    // size to ensure slides are still being output properly when
                    // the window is bigger than twice the slide and we get a lot
                    // of empty input epochs.
                    let start = start_epoch.unwrap();
                    for i in 0..size/slide {
                        let target = if time-start < size {
                            start+size-1
                        } else {
                            start+size+((time-start-size)/slide+1+i)*slide-1
                        };
                        notificator.notify_at(cap.delayed(&G::Timestamp::from_integer(target)));
                    }
                });
            });
            
            notificator.for_each(|cap, _, _| {
                let end = cap.time().to_integer();
                let mut time = end+1-size;
                let slide_end = time+slide;
                let mut window = Vec::new();
                // Compute full window from partials. First gather parts
                // that would fall out of the window and remove them.
                while time < slide_end {
                    if let Some(mut part) = window_parts.remove(&time) {
                        window.append(&mut part);
                    }
                    time += 1;
                }
                // Then gather and clone parts that will still be relevant
                // later.
                while time <= end {
                    if let Some(part) = window_parts.get(&time) {
                        part.iter().for_each(|entry| window.push(entry.clone()));
                    }
                    time += 1;
                }
                // Finally output the full window.
                output.session(&cap).give_iterator(window.drain(..));
            });
        })
    }

    fn tumbling_window(&self, size: usize) -> Stream<G, D>{
        let mut windows = HashMap::new();
        
        self.unary_notify(Pipeline, "Window", Vec::new(), move |input, output, notificator| {
            let size = size.clone();
            input.for_each(|cap, data| {
                // Round the time up to the next window.
                let wtime = (cap.time().clone().to_integer() / size + 1) * size;
                // Now act as if we were on that window's time.
                notificator.notify_at(cap.delayed(&G::Timestamp::from_integer(wtime)));
                let window = windows.entry(wtime).or_insert_with(|| Vec::new());
                data.drain(..).for_each(|data|{
                    window.push(data);
                });
            });
            
            notificator.for_each(|cap, _, _| {
                if let Some(mut window) = windows.remove(&cap.time().to_integer()) {
                    output.session(&cap).give_iterator(window.drain(..));
                }
            });
        })
    }
    
    fn epoch_window(&self, size: usize, slide: usize) -> Stream<G, D> {
        self.window(size, slide, |t, _| t.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use timely;
    use timely::dataflow::operators::{ToStream, Capture, Delay, Map};
    use timely::dataflow::operators::capture::Extract;
    use timely::progress::timestamp::RootTimestamp;
    
    #[test]
    fn epoch_window() {
        let data = timely::example(|scope| {
            vec!((0, 1), (0, 2), (0, 3),
                 (1, 4), (1, 5),
                 (2, 6),
                 (3, 7),
                 (4, 8), (4, 9),
                 (6, 10))
                .to_stream(scope)
                .delay(|d, _| RootTimestamp::new(d.0))
                .map(|d| d.1)
                .epoch_window(3, 2)
                .capture()
        });

        let data = data.extract();
        assert_eq!(data[0].1, vec!(1, 2, 3, 4, 5, 6));
        assert_eq!(data[1].1, vec!(6, 7, 8, 9));
        assert_eq!(data[2].1, vec!(8, 9, 10));

        let data = timely::example(|scope| {
            vec!((0, 1), (3, 1))
                .to_stream(scope)
                .delay(|d, _| RootTimestamp::new(d.0))
                .map(|d| d.1)
                .epoch_window(3, 1)
                .capture()
        });

        let data = data.extract();
        assert_eq!(data[0].1, vec!(1));
        assert_eq!(data[1].1, vec!(1));
        assert_eq!(data[2].1, vec!(1));
    }
    
    #[test]
    fn tumbling_window() {
        let data = timely::example(|scope| {
            vec!((0, 1), (0, 2), (0, 3),
                 (1, 4), (1, 5),
                 (2, 6),
                 (3, 7))
                .to_stream(scope)
                .delay(|d, _| RootTimestamp::new(d.0))
                .map(|d| d.1)
                .tumbling_window(2)
                .capture()
        });
        
        let data = data.extract();
        assert_eq!(data[0].1, vec!(1, 2, 3, 4, 5));
        assert_eq!(data[1].1, vec!(6, 7));
    }
}
