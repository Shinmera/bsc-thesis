use std::io::{self, Write, Stdout, Stdin, Lines, BufReader, BufRead};
use std::fs::File;
use std::fmt::Debug;
use timely::dataflow::operators::capture::event::{Event, EventIterator, EventPusher};
use timely::progress::timestamp::{Timestamp, RootTimestamp};
use timely::progress::nested::product::Product;
use timely::Data;

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

pub struct Console<T: Timestamp, D>{
    stdin: Stdin,
    stdout: Stdout,
    next: Option<Event<T, D>>,
}

impl<T: Timestamp, D> Console<T, D> {
    pub fn new() -> Self {
        Console{stdin: io::stdin(), stdout: io::stdout(), next: None}
    }
}

impl<T: Timestamp, D> EventIterator<T, D> for Console<T, D> where String: ToData<T, D>{
    fn next(&mut self) -> Option<&Event<T, D>> {
        let ref mut stdin = self.stdin;
        self.next = to_message(||{
            let mut line = String::new();
            stdin.read_line(&mut line).ok()
                .and_then(|_| line.to_data())
        });
        self.next.as_ref()
    }
}

impl<T: Timestamp+Debug, D: Debug> EventPusher<T, D> for Console<T, D> {
    fn push(&mut self, event: Event<T, D>) {
        let ref mut stdout = self.stdout;
        if let Event::Messages(t, d) = event {
            stdout.write_fmt(format_args!("{:?} {:?}", t, d)).unwrap();
        }
    }
}

pub struct FileInput<T: Timestamp, D>{
    stream: Lines<BufReader<File>>,
    next: Option<Event<T, D>>
}

impl<T: Timestamp, D> FileInput<T, D> {
    pub fn new(f: File) -> Self {
        FileInput{stream: BufReader::new(f).lines(), next: None}
    }
}

impl<T: Timestamp, D> EventIterator<T, D> for FileInput<T, D> where String: ToData<T, D> {
    fn next(&mut self) -> Option<&Event<T, D>> {
        let ref mut stream = self.stream;
        self.next = to_message(||{
            stream.next()
                .and_then(|n| n.ok())
                .and_then(|l| l.to_data())
        });
        self.next.as_ref()
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

impl<T: Timestamp, D: Data> Into<Source<T, D>> for File where String: ToData<T, D> {
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
