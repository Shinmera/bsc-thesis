use std::io::{self, Write, Stdout, Stdin, Lines, BufReader, BufRead};
use std::fs::File;
use std::fmt::Debug;
use timely::dataflow::operators::capture::event::{Event, EventIterator, EventPusher};
use timely::progress::timestamp::{Timestamp, RootTimestamp};
use timely::progress::nested::product::Product;

pub struct Null();

pub trait ToData<T, D> {
    fn to_data(self) -> Option<(T, D)>;
}

impl ToData<Product<RootTimestamp, usize>, String> for String{
    fn to_data(self) -> Option<(Product<RootTimestamp, usize>, String)> {
        Some((RootTimestamp::new(0), self))
    }
}

fn to_message<T: Timestamp, D, F>(mut next: F) -> Option<Event<T, D>>
where F: FnMut()->Option<(T, D)> {
    let mut data = Vec::new();

    if let Some((t, d)) = next() {
        data.push(d);
        // BAD: We leak one event into the next epoch.
        while let Some((t2, d)) = next() {
            data.push(d);
            if t != t2 { break; }
        }
        return Some(Event::Messages(t, data));
    }
    None
}

impl Null {
    pub fn new() -> Self {
        Null()
    }
}

impl<T: Timestamp, D> EventIterator<T, D> for Null {
    fn next(&mut self) -> Option<&Event<T, D>> {None}
}

impl<T: Timestamp, D> EventPusher<T, D> for Null {
    fn push(&mut self, _: Event<T, D>) {}
}

pub struct Console(Stdin, Stdout);

impl Console {
    pub fn new() -> Self {
        Console(io::stdin(), io::stdout())
    }
}

impl<T: Timestamp, D> EventIterator<T, D> for Console where String: ToData<T, D>{
    fn next(&mut self) -> Option<&Event<T, D>> {
        let &mut Console(ref mut stdin, _) = self;
        to_message(||{
            let mut line = String::new();
            stdin.read_line(&mut line).ok()
                .and_then(|_| line.to_data())
        }).map(|m| &m)
    }
}

impl<T: Timestamp+Debug, D: Debug> EventPusher<T, D> for Console {
    fn push(&mut self, event: Event<T, D>) {
        let &mut Console(_, ref mut stdout) = self;
        if let Event::Messages(t, d) = event {
            stdout.write_fmt(format_args!("{:?} {:?}", t, d));
        }
    }
}

pub struct FileInput(Lines<BufReader<File>>);

impl FileInput {
    pub fn new(f: File) -> Self {
        FileInput(BufReader::new(f).lines())
    }
}

impl<T: Timestamp, D> EventIterator<T, D> for FileInput where String: ToData<T, D> {
    fn next(&mut self) -> Option<&Event<T, D>> {
        let &mut FileInput(ref mut reader) = self;

        to_message(||{
            reader.next()
                .and_then(|n| n.ok())
                .and_then(|l| l.to_data())
        }).map(|m| &m)
    }
}

pub struct Source<T, D>(Box<EventIterator<T, D>>);

impl<T: Timestamp, D> Source<T, D> {
    pub fn new(it: Box<EventIterator<T, D>>) -> Self {
        Source(it)
    }
}

impl<T: Timestamp, D> EventIterator<T, D> for Source<T, D> {
    fn next(&mut self) -> Option<&Event<T, D>> {
        let &mut Source(ref mut it) = self;
        it.next()
    }
}

impl<T: Timestamp, D> Into<Source<T, D>> for File where String: ToData<T, D> {
    fn into(self) -> Source<T, D> {
        Source::new(Box::new(FileInput::new(self)))
    }
}

pub struct Drain<T, D>(Box<EventPusher<T, D>>);

impl<T: Timestamp, D> Drain<T, D> {
    pub fn new(it: Box<EventPusher<T, D>>) -> Self {
        Drain(it)
    }
}

impl<T: Timestamp, D> EventPusher<T, D> for Drain<T, D> {
    fn push(&mut self, event: Event<T, D>) {
        let &mut Drain(ref mut it) = self;
        it.push(event);
    }
}

impl<T: Timestamp, D> Into<Drain<T, D>> for () {
    fn into(self) -> Drain<T, D> {
        Drain::new(Box::new(Null::new()))
    }
}
