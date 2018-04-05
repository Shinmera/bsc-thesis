use config::Config;
use std::io::{self, Result, Error, ErrorKind, Write, Stdout, Stdin, Lines, BufReader, BufRead, BufWriter};
use std::fs::File;
use std::mem;
use timely::dataflow::operators::capture::event::{Event, EventIterator, EventPusher};
use timely::progress::timestamp::{Timestamp, RootTimestamp};
use timely::progress::nested::product::Product;
use timely::Data;

/// This trait is responsible for converting an opaque input type into a timestamp and data object for use in a data source.
pub trait ToData<T, D> {
    /// Convert the object into a timestamp and data instance.
    ///
    /// The timestamp should correspond to the epoch on which the data should be fed into the dataflow.
    /// If a parsing failure or other unexpected circumstances occur, this function should return None.
    fn to_data(self) -> Option<(T, D)>;
}

impl ToData<Product<RootTimestamp, usize>, String> for String{
    fn to_data(self) -> Option<(Product<RootTimestamp, usize>, String)> {
        Some((RootTimestamp::new(0), self))
    }
}

/// This trait is responsible for converting an opaque data type and timestamp into a string for use in a data drain.
pub trait FromData<T: Timestamp> {
    /// Convert the object and timestamp into a string.
    ///
    /// This string should be either in an expected format in an external file, or readable by humans.
    /// The exact behaviour is up to the implementation of the trait.
    fn from_data(&self, t: &T) -> String;
}

impl<T: Timestamp> FromData<T> for String {
    fn from_data(&self, t: &T) -> String {
        format!("{:?} {}", t, self)
    }
}

/// A shorthand to read a complete epoch from a data source closure.
///
/// Returns the timestamp for the epoch and a vector of data instances. The data is converted from the
/// closure's return value via the ToData trait. If the closure ever returns None, the data is assumed
/// to have been exhausted. If any data was already accumulated in this call to to_message, then the
/// current data vector is returned. Otherwise, None is returned.
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

/// This input does not provide any data and simply immediately ends the epoch.
///
/// Its primary use is as a default source and as a way to test whether dataflow construction
/// succeeds properly.
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

/// This output does not do anything with the data and simply discards it.
///
/// Its primary use is to measure the performance of the dataflow when the actual results of
/// the computation aren't of consequence.
pub struct NullOutput();

impl NullOutput {
    pub fn new() -> Self {
        NullOutput()
    }
}

impl<T: Timestamp, D> EventPusher<T, D> for NullOutput {
    fn push(&mut self, _: Event<T, D>) {}
}

/// This input reads line by line from the standard input.
///
/// With this you can pipe data into the dataflow from an external file or other kind of on-the-fly
/// data source. Note that each event /must/ fit onto a single line, and the ToData trait /must/
/// be implemented for the String type.
pub struct ConsoleInput<T: Timestamp, D> {
    stream: Stdin,
    queue: Vec<Event<T, D>>,
    last_time: Option<T>
}

impl<T: Timestamp, D> ConsoleInput<T, D> {
    pub fn new() -> Self {
        ConsoleInput{
            stream: io::stdin(),
            queue: Vec::new(),
            last_time: Some(Default::default())
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
                let mut p = mem::replace(&mut self.last_time, Some(t.clone())).unwrap();
                self.queue.push(Event::Progress(vec!((p, -1), (t.clone(), 1))));
                self.queue.push(Event::Messages(t, d));
            } else if let Some(p) = self.last_time.take() {
                self.queue.push(Event::Progress(vec!((p, -1))));
            }
        }
        self.queue.last()
    }
}

/// This output simply prints all the data to the standard output.
///
/// This is mostly useful for short test runs and debugging sessions, in order to be able to
/// watch the data as it comes out of the dataflow. Note that the FromData trait /must/ be
/// implemented in order for this to work.
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

/// This input reads lines from a file and converts them into data to feed into the dataflow.
///
/// In order for this to work, every event must fit onto a single line in the file, and the
/// appropriate ToData conversion trait /must/ be implemented for the String type.
pub struct FileInput<T: Timestamp, D> {
    stream: Lines<BufReader<File>>,
    queue: Vec<Event<T, D>>,
    last_time: Option<T>
}

impl<T: Timestamp, D> FileInput<T, D> {
    pub fn new(f: File) -> Self {
        FileInput{
            stream: BufReader::new(f).lines(),
            queue: Vec::new(),
            last_time: Some(Default::default())
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
                let mut p = mem::replace(&mut self.last_time, Some(t.clone())).unwrap();
                self.queue.push(Event::Progress(vec!((p, -1), (t.clone(), 1))));
                self.queue.push(Event::Messages(t, d));
            } else if let Some(p) = self.last_time.take() {
                self.queue.push(Event::Progress(vec!((p, -1))));
            }
        }
        self.queue.last()
    }
}

/// This output writes everything to a file.
///
/// In order for this to work, the FromData trait /must/ be implemented for the data type.
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

/// This input reads data elements from a vector.
///
/// This is mostly useful for setting up short test runs in-source. The vector
/// should have a type of Vec<(T, Vec<D>)> where each tuple contains all data for a
/// specific epoch. The epochs should be ordered.
#[allow(dead_code)]
pub struct VectorInput<T: Timestamp, D> {
    vector: Vec<Event<T, D>>,
    index: usize
}

impl<T: Timestamp, D> VectorInput<T, D> {
    #[allow(dead_code)]
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

/// This output writes data elemetns to a vector.
///
/// This is mostly useful for writing unit tests to ensure the proper behaviour
/// of a dataflow, as the vector can then be compared against expected results for
/// a test run.
#[allow(dead_code)]
pub struct VectorOutput<T: Timestamp, D> {
    vector: Vec<(T, Vec<D>)>
}

impl<T: Timestamp, D> VectorOutput<T, D> {
    #[allow(dead_code)]
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

/// This struct acts as an opaque event source for a dataflow.
///
/// It is merely a container to bypass Rust's restriction on materialised traits.
/// You should be able to use the Source::from method to convert a usable type into
/// a source that can be attached to a dataflow.
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
            _ => Err(Error::new(ErrorKind::Other, "Unknown output."))
        }
    }
}

/// This struct acts as an opaque event drain for a dataflow.
///
/// It is merely a container to bypass Rust's restriction on materialised traits.
/// You should be able to use the Drain::from method to convert a usable type into
/// a drain that can be attached to a dataflow.
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
            _ => Err(Error::new(ErrorKind::Other, "Unknown output."))
        }
    }
}
