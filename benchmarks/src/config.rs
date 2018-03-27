use std::io::{Result, Error, ErrorKind};
use std::collections::HashMap;
use std::str::FromStr;

#[derive(Clone)]
pub struct Config {
    args: HashMap<String, String>
}

impl Config {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Config{ args: HashMap::new() }
    }
    
    pub fn from<I: Iterator<Item=String>>(mut cmd_args: I) -> Result<Self> {
        let mut args = HashMap::new();
        let mut i = 0;
        while let Some(arg) = cmd_args.next() {
            if arg.starts_with("--") {
                match cmd_args.next() {
                    Some(value) => args.insert(format!("{}", &arg[2..]), value),
                    None => return Err(Error::new(ErrorKind::Other, "No corresponding value."))
                };
            } else {
                args.insert(format!("{}", i), arg);
                i = i+1;
            }
        }
        Ok(Config{ args: args })
    }

    #[allow(dead_code)]
    pub fn insert(&mut self, key: &str, value: String) {
        self.args.insert(String::from(key), value);
    }

    pub fn get(&self, key: &str) -> Option<String> {
        self.args.get(key).map(|x| x.clone())
    }

    pub fn get_as<T: FromStr>(&self, key: &str) -> Option<T> {
        self.args.get(key).map_or(None, |x| x.parse::<T>().ok())
    }

    pub fn get_or(&self, key: &str, default: &str) -> String {
        self.args.get(key).map_or(String::from(default), |x| x.clone())
    }

    pub fn get_as_or<T: FromStr>(&self, key: &str, default: T) -> T {
        self.get_as(key).unwrap_or(default)
    }
}
