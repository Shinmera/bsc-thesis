use config::Config;
use std::io::{self, Result, Error, ErrorKind, Write, Stdout, Stdin, Lines, BufReader, BufRead, BufWriter};
use std::fs::File;
use std::mem;
use timely::dataflow::operators::capture::event::{Event, EventIterator, EventPusher};
use timely::progress::timestamp::{Timestamp, RootTimestamp};
use timely::progress::nested::product::Product;
use timely::Data;
use kafkaesque::{self};
use rdkafka::config::ClientConfig;

pub trait ToData<T, D> {
    fn to_data(self) -> Option<(T, D)>;
}

impl ToData<Product<RootTimestamp, usize>, String> for String{
    fn to_data(self) -> Option<(Product<RootTimestamp, usize>, String)> {
        Some((RootTimestamp::new(0), self))
    }
}

pub trait FromData<T: Timestamp> {
    fn from_data(&self, t: &T) -> String;
}

impl<T: Timestamp> FromData<T> for String {
    fn from_data(&self, t: &T) -> String {
        format!("{:?} {}", t, self)
    }
}

fn to_message<T: Timestamp, D, F>(mut next: F) -> Option<(T, Vec<D>)>
where F: FnMut()->Option<(T, D)> {
    let mut data = Vec::new();

    if let Some((t, d)) = next() {
        data.push(d);
        // BAD: We leak one event into the next epoch.
        while let Some((t2, d)) = next() {
            data.push(d);
            if t != t2 { break; }
        }
        return Some((t, data));
    }
    None
}

pub struct NullInput<T: Timestamp, D>{
    queue: Vec<Event<T, D>>,
}

impl<T: Timestamp, D> NullInput<T, D> {
    pub fn new() -> Self {
        NullInput{ queue: vec!(Event::Progress(vec!((Default::default(), -1))), Event::Progress(vec!())) }
    }
}

impl<T: Timestamp, D> EventIterator<T, D> for NullInput<T, D> {
    fn next(&mut self) -> Option<&Event<T, D>> {
        self.queue.pop();
        self.queue.last()
    }
}

pub struct NullOutput();

impl NullOutput {
    pub fn new() -> Self {
        NullOutput()
    }
}

impl<T: Timestamp, D> EventPusher<T, D> for NullOutput {
    fn push(&mut self, _: Event<T, D>) {}
}

pub struct ConsoleInput<T: Timestamp, D> {
    stream: Stdin,
    queue: Vec<Event<T, D>>,
    last_time: T
}

impl<T: Timestamp, D> ConsoleInput<T, D> {
    pub fn new() -> Self {
        ConsoleInput{
            stream: io::stdin(),
            queue: Vec::new(),
            last_time: Default::default()
        }
    }
}

impl<T: Timestamp, D> EventIterator<T, D> for ConsoleInput<T, D> where String: ToData<T, D>{
    fn next(&mut self) -> Option<&Event<T, D>> {
        let ref mut stream = self.stream;
        self.queue.pop();
        if self.queue.is_empty() {
            if let Some((t, d)) = to_message(||{ let mut line = String::new();
                                                 stream.read_line(&mut line).ok()
                                                 .and_then(|_| line.to_data()) }) {
                let mut p = mem::replace(&mut self.last_time, t.clone());
                self.queue.push(Event::Progress(vec!((p, -1), (t.clone(), 1))));
                self.queue.push(Event::Messages(t, d));
            } else if self.last_time != Default::default() {
                let mut p = mem::replace(&mut self.last_time, Default::default());
                self.queue.push(Event::Progress(vec!((p, -1))));
            }
        }
        self.queue.last()
    }
}

pub struct ConsoleOutput {
    stream: Stdout
}

impl ConsoleOutput {
    pub fn new() -> Self {
        ConsoleOutput{ stream: io::stdout() }
    }
}

impl<T: Timestamp, D: FromData<T>> EventPusher<T, D> for ConsoleOutput {
    fn push(&mut self, event: Event<T, D>) {
        let ref mut stream = self.stream;
        if let Event::Messages(t, d) = event {
            for e in d {
                stream.write_all(e.from_data(&t).as_bytes()).unwrap();
                stream.write(b"\n").unwrap();
                stream.flush().unwrap();
            }
        }
    }
}

pub struct FileInput<T: Timestamp, D> {
    stream: Lines<BufReader<File>>,
    queue: Vec<Event<T, D>>,
    last_time: T
}

impl<T: Timestamp, D> FileInput<T, D> {
    pub fn new(f: File) -> Self {
        FileInput{
            stream: BufReader::new(f).lines(),
            queue: Vec::new(),
            last_time: Default::default()
        }
    }
}

impl<T: Timestamp, D> EventIterator<T, D> for FileInput<T, D> where String: ToData<T, D> {
    fn next(&mut self) -> Option<&Event<T, D>> {
        let ref mut stream = self.stream;
        self.queue.pop();
        if self.queue.is_empty() {
            if let Some((t, d)) = to_message(||{ stream.next()
                                                 .and_then(|n| n.ok())
                                                 .and_then(|l| l.to_data()) }) {
                let mut p = mem::replace(&mut self.last_time, t.clone());
                self.queue.push(Event::Progress(vec!((p, -1), (t.clone(), 1))));
                self.queue.push(Event::Messages(t, d));
            } else if self.last_time != Default::default() {
                let mut p = mem::replace(&mut self.last_time, Default::default());
                self.queue.push(Event::Progress(vec!((p, -1))));
            }
        }
        self.queue.last()
    }
}

pub struct FileOutput {
    stream: BufWriter<File>
}

impl FileOutput {
    pub fn new(f: File) -> Self {
        FileOutput{stream: BufWriter::new(f)}
    }
}

impl<T: Timestamp, D: FromData<T>> EventPusher<T, D> for FileOutput {
    fn push(&mut self, event: Event<T, D>) {
        let ref mut stream = self.stream;
        if let Event::Messages(t, d) = event {
            for e in d {
                stream.write_all(e.from_data(&t).as_bytes()).unwrap();
                stream.write(b"\n").unwrap();
                stream.flush().unwrap();
            }
        }
    }
}

pub struct VectorInput<T: Timestamp, D> {
    vector: Vec<Event<T, D>>,
    index: usize
}

impl<T: Timestamp, D> VectorInput<T, D> {
    pub fn new(mut v: Vec<(T, Vec<D>)>) -> Self {
        VectorInput{vector: v.drain(..).map(|(t, d)|Event::Messages(t, d)).collect(), index: 0}
    }
}

impl<T: Timestamp, D> EventIterator<T, D> for VectorInput<T, D> {
    fn next(&mut self) -> Option<&Event<T, D>> {
        let event = self.vector.get(self.index);
        self.index += 1;
        event
    }
}

pub struct VectorOutput<T: Timestamp, D> {
    vector: Vec<(T, Vec<D>)>
}

impl<T: Timestamp, D> VectorOutput<T, D> {
    pub fn new() -> Self {
        VectorOutput{vector: Vec::new()}
    }
}

impl<T: Timestamp, D> EventPusher<T, D> for VectorOutput<T, D> {
    fn push(&mut self, event: Event<T, D>) {
        if let Event::Messages(t, d) = event {
            self.vector.push((t, d));
        }
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

impl<T: Timestamp, D: 'static> Into<Source<T, D>> for () {
    fn into(self) -> Source<T, D> {
        Source::new(Box::new(NullInput::new()))
    }
}

impl<T: Timestamp, D: Data> Into<Source<T, D>> for Stdin where String: ToData<T, D> {
    fn into(self) -> Source<T, D> {
        Source::new(Box::new(ConsoleInput::new()))
    }
}

impl<T: Timestamp, D: Data> Into<Source<T, D>> for File where String: ToData<T, D> {
    fn into(self) -> Source<T, D> {
        Source::new(Box::new(FileInput::new(self)))
    }
}

/// God damnit rust.
// impl<T: Timestamp, D> Into<Source<T, D>> for Vec<(T, Vec<D>)> {
//     fn into(self) -> Source<T, D> {
//         Source::new(Box::new(VectorInput::new(self)));
//     }
// }

impl<T: Timestamp, D: Data> Into<Result<Source<T, D>>> for Config
where Stdin: Into<Source<T, D>>,
      File: Into<Source<T, D>> {
    fn into(self) -> Result<Source<T, D>> {
        match self.get_or("input", "file").as_ref() {
            "null" => {
                Ok(().into())
            },
            "console" => {
                Ok(io::stdin().into())
            },
            "file" => {
                Ok(File::open(self.get_or("input-file", "input.log"))?.into())
            },
            "kafka" => {
                let mut config = ClientConfig::new();
                config
                    .set("produce.offset.report", "true")
                    .set("bootstrap.servers", &self.get_or("kafka-server", "localhost:9092"));
                Ok(Source::new(Box::new(kafkaesque::EventConsumer::new(config, self.get_or("kafka-topic", "1")))))
            },
            _ => Err(Error::new(ErrorKind::Other, "Unknown output."))
        }
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
        Drain::new(Box::new(NullOutput::new()))
    }
}

impl<T: Timestamp, D: Data+FromData<T>> Into<Drain<T, D>> for Stdout {
    fn into(self) -> Drain<T, D> {
        Drain::new(Box::new(ConsoleOutput::new()))
    }
}

impl<T: Timestamp, D: Data+FromData<T>> Into<Drain<T, D>> for File {
    fn into(self) -> Drain<T, D> {
        Drain::new(Box::new(FileOutput::new(self)))
    }
}

/// God damnit rust.
// impl<T: Timestamp, D> Into<Drain<T, D>> for Vec<(T, Vec<D>)> {
//     fn into(self) -> Drain<T, D> {
//         Drain::new(Box::new(VectorOutput::new()));
//     }
// }

impl<T: Timestamp, D: Data+FromData<T>> Into<Result<Drain<T, D>>> for Config {
    fn into(self) -> Result<Drain<T, D>> {
        match self.get_or("output", "null").as_ref() {
            "null" => {
                Ok(().into())
            },
            "console" => {
                Ok(io::stdout().into())
            },
            "file" => {
                Ok(File::create(self.get_or("output-file", "output.log"))?.into())
            },
            "kafka" => {
                let mut config = ClientConfig::new();
                config
                    .set("produce.offset.report", "true")
                    .set("bootstrap.servers", &self.get_or("kafka-server", "localhost:9092"));
                Ok(Drain::new(Box::new(kafkaesque::EventProducer::new(config, self.get_or("kafka-topic", "1")))))
            },
            _ => Err(Error::new(ErrorKind::Other, "Unknown output."))
        }
    }
}
