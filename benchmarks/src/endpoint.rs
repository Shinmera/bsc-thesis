use config::Config;
use std::io::{self, Result, Error, ErrorKind, Write, Stdout, Stdin, Lines, BufReader, BufRead, BufWriter};
use std::error::Error as StdError;
use std::fs::File;
use timely::progress::timestamp::Timestamp;
use timely::Data;
//use kafkaesque;
//use rdkafka::config::ClientConfig;

pub const OUT_OF_DATA: &str = "out of data";

pub fn out_of_data<D>() -> Result<D> {
    Err(Error::new(ErrorKind::Other, OUT_OF_DATA))
}

pub fn accept_out_of_data<D>(r: Result<D>) -> Result<D>
where D: Default {
    r.or_else(|e| if e.description() == OUT_OF_DATA {
        Ok(Default::default())
    } else { Err(e) })
}

pub trait EventSource<T, D> {
    fn next(&mut self) -> Result<(T, Vec<D>)>;
}

pub trait EventDrain<T, D> {
    fn next(&mut self, T, Vec<D>);
}

/// This trait is responsible for converting an opaque input type into a timestamp and data object for use in a data source.
pub trait ToData<T, D> {
    /// Convert the object into a timestamp and data instance.
    ///
    /// The timestamp should correspond to the epoch on which the data should be fed into the dataflow.
    /// If a parsing failure or other unexpected circumstances occur, this function should return None.
    fn to_data(self) -> Result<(T, D)>;
}

impl ToData<usize, String> for String{
    fn to_data(self) -> Result<(usize, String)> {
        Ok((0, self))
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
fn to_message<T: Timestamp, D, F>(mut next: F) -> Result<(T, Vec<D>)>
where F: FnMut()->Result<(T, D)> {
    let mut data = Vec::new();

    match next() {
        Ok((t, d)) => {
            data.push(d);
            // BAD: We leak one event into the next epoch.
            while let Ok((t2, d)) = next() {
                data.push(d);
                if t != t2 { break; }
            }
            Ok((t, data))
        },
        Err(e) => Err(e)
    }
}

/// This input does not provide any data and simply immediately ends the epoch.
///
/// Its primary use is as a default source and as a way to test whether dataflow construction
/// succeeds properly.
pub struct Null{}

impl Null {
    pub fn new() -> Self {
        Null{}
    }
}

impl<T, D> EventSource<T, D> for Null {
    fn next(&mut self) -> Result<(T, Vec<D>)> {
        out_of_data()
    }
}

impl<T: Timestamp, D> EventDrain<T, D> for Null {
    fn next(&mut self, _: T, _: Vec<D>) {}
}

/// This input reads line by line from the standard input.
///
/// With this you can pipe data into the dataflow from an external file or other kind of on-the-fly
/// data source. Note that each event /must/ fit onto a single line, and the ToData trait /must/
/// be implemented for the String type.
pub struct Console {
    stdin: Stdin,
    stdout: Stdout,
}

impl Console {
    pub fn new() -> Self {
        Console{
            stdin: io::stdin(),
            stdout: io::stdout(),
        }
    }
}

impl<T: Timestamp, D> EventSource<T, D> for Console where String: ToData<T, D>{
    fn next(&mut self) -> Result<(T, Vec<D>)> {
        let ref mut stream = self.stdin;
        to_message(||{ let mut line = String::new();
                       stream.read_line(&mut line)
                       .and_then(|_| line.to_data()) })
    }
}

impl<T: Timestamp, D: FromData<T>> EventDrain<T, D> for Console {
    fn next(&mut self, t: T, d: Vec<D>) {
        let ref mut stream = self.stdout;
        for e in d {
            stream.write_all(e.from_data(&t).as_bytes()).unwrap();
            stream.write(b"\n").unwrap();
            stream.flush().unwrap();
        }
    }
}

/// This input reads lines from a file and converts them into data to feed into the dataflow.
///
/// In order for this to work, every event must fit onto a single line in the file, and the
/// appropriate ToData conversion trait /must/ be implemented for the String type.
pub struct FileInput {
    stream: Lines<BufReader<File>>,
}

impl FileInput {
    pub fn new(f: File) -> Self {
        FileInput{
            stream: BufReader::new(f).lines(),
        }
    }
}

impl<T: Timestamp, D> EventSource<T, D> for FileInput where String: ToData<T, D> {
    fn next(&mut self) -> Result<(T, Vec<D>)> {
        let ref mut stream = self.stream;
        to_message(|| stream.next()
                   .unwrap_or_else(|| out_of_data())
                   .and_then(|line| line.to_data()))
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

impl<T: Timestamp, D: FromData<T>> EventDrain<T, D> for FileOutput {
    fn next(&mut self, t: T, d: Vec<D>) {
        let ref mut stream = self.stream;
        for e in d {
            stream.write_all(e.from_data(&t).as_bytes()).unwrap();
            stream.write(b"\n").unwrap();
            stream.flush().unwrap();
        }
    }
}

/// This input reads data elements from a vector.
///
/// This is mostly useful for setting up short test runs in-source. The vector
/// should have a type of Vec<(T, Vec<D>)> where each tuple contains all data for a
/// specific epoch. The epochs should be ordered.
#[allow(dead_code)]
pub struct VectorEndpoint<T: Timestamp, D> {
    vector: Vec<(T, Vec<D>)>,
}

impl<T: Timestamp, D> VectorEndpoint<T, D> {
    #[allow(dead_code)]
    pub fn new(mut v: Vec<(T, Vec<D>)>) -> Self {
        v.reverse();
        VectorEndpoint{vector: v}
    }
}

impl<T: Timestamp, D> EventSource<T, D> for VectorEndpoint<T, D> {
    fn next(&mut self) -> Result<(T, Vec<D>)> {
        if let Some(e) = self.vector.pop() {
            Ok(e)
        } else {
            out_of_data()
        }
    }
}

impl<T: Timestamp, D> EventDrain<T, D> for VectorEndpoint<T, D> {
    fn next(&mut self, t: T, d: Vec<D>) {
        self.vector.push((t, d));
    }
}

/// This struct acts as an opaque event source for a dataflow.
///
/// It is merely a container to bypass Rust's restriction on materialised traits.
/// You should be able to use the Source::from method to convert a usable type into
/// a source that can be attached to a dataflow.
pub struct Source<T, D>(Box<EventSource<T, D>>);

impl<T: Timestamp, D> Source<T, D> {
    pub fn new(it: Box<EventSource<T, D>>) -> Self {
        Source(it)
    }
}

impl<T: Timestamp, D> EventSource<T, D> for Source<T, D> {
    fn next(&mut self) -> Result<(T, Vec<D>)> {
        let &mut Source(ref mut it) = self;
        it.next()
    }
}

impl<T: Timestamp, D: 'static> Into<Source<T, D>> for () {
    fn into(self) -> Source<T, D> {
        Source::new(Box::new(Null::new()))
    }
}

impl<T: Timestamp, D: Data> Into<Source<T, D>> for Stdin where String: ToData<T, D> {
    fn into(self) -> Source<T, D> {
        Source::new(Box::new(Console::new()))
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
where String: ToData<T, D> {
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
            // "kafka" => {
            //     let mut config = ClientConfig::new();
            //     config
            //         .set("produce.offset.report", "true")
            //         .set("bootstrap.servers", &self.get_or("kafka-server", "localhost:9092"));
            //     Ok(Source::new(Box::new(kafkaesque::EventConsumer::new(config, self.get_or("kafka-topic", "1")))))
            // },
            _ => Err(Error::new(ErrorKind::Other, "Unknown output."))
        }
    }
}

/// This struct acts as an opaque event drain for a dataflow.
///
/// It is merely a container to bypass Rust's restriction on materialised traits.
/// You should be able to use the Drain::from method to convert a usable type into
/// a drain that can be attached to a dataflow.
pub struct Drain<T, D>(Box<EventDrain<T, D>>);

impl<T: Timestamp, D> Drain<T, D> {
    pub fn new(it: Box<EventDrain<T, D>>) -> Self {
        Drain(it)
    }
}

impl<T: Timestamp, D> EventDrain<T, D> for Drain<T, D> {
    fn next(&mut self, t: T, d: Vec<D>) {
        let &mut Drain(ref mut it) = self;
        it.next(t, d);
    }
}

impl<T: Timestamp, D> Into<Drain<T, D>> for () {
    fn into(self) -> Drain<T, D> {
        Drain::new(Box::new(Null::new()))
    }
}

impl<T: Timestamp, D: Data+FromData<T>> Into<Drain<T, D>> for Stdout {
    fn into(self) -> Drain<T, D> {
        Drain::new(Box::new(Console::new()))
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
            // "kafka" => {
            //     let mut config = ClientConfig::new();
            //     config
            //         .set("produce.offset.report", "true")
            //         .set("bootstrap.servers", &self.get_or("kafka-server", "localhost:9092"));
            //     Ok(Drain::new(Box::new(kafkaesque::EventProducer::new(config, self.get_or("kafka-topic", "1")))))
            // },
            _ => Err(Error::new(ErrorKind::Other, "Unknown output."))
        }
    }
}
