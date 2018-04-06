use std::ops::Add;
use std::convert::From;
use std::time::{Instant, Duration};
use std::fmt;

/// This is a simple struct to represent statistical values
/// over a set of samples.
///
/// You can use Statistics::from to convert a vector of timing
/// values into this statistics struct. It will automatically
/// calculate the relevant fields.
pub struct Statistics{
    pub total: f64,
    pub count: usize,
    pub minimum: f64,
    pub maximum: f64,
    pub average: f64,
    pub median: f64,
    pub deviation: f64
}

/// Returns the duration as a double float of seconds.
fn duration_fsecs(d: &Duration) -> f64{
    d.as_secs() as f64 + d.subsec_nanos() as f64 / 1_000_000_000 as f64
}

impl<'a> From<Vec<(&'a Instant, &'a Instant)>> for Statistics{
    fn from(vec: Vec<(&'a Instant, &'a Instant)>) -> Self{
        // We explicitly recompute min and max for in order to
        // still get an accurate total for overlapping durations.
        let now = Instant::now();
        let mut min = vec.get(0).map(|&(s, _)| s).unwrap_or(&now);
        let mut max = vec.get(0).map(|&(_, e)| e).unwrap_or(&now);
        let floats : Vec<f64> = vec.iter().map(|&(s, e)| {
            min = min.min(s);
            max = max.max(e);
            duration_fsecs(&e.duration_since(*s))
        }).collect();
        let mut stats = Self::from(floats);
        stats.total = duration_fsecs(&max.duration_since(*min));
        stats
    }
}

impl From<Vec<(Instant, Instant)>> for Statistics{
    fn from(vec: Vec<(Instant, Instant)>) -> Self{
        let vec : Vec<f64> = vec.iter().map(|&(s, e)| duration_fsecs(&e.duration_since(s))).collect();
        Self::from(vec)
    }
}

impl From<Vec<Duration>> for Statistics{
    fn from(vec: Vec<Duration>) -> Self{
        let vec : Vec<f64> = vec.iter().map(duration_fsecs).collect();
        Self::from(vec)
    }
}

impl From<Vec<f64>> for Statistics{
    fn from(vec: Vec<f64>) -> Self{
        let mut local = vec.clone();
        local.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap());
        let total = local.iter().fold(0.0, f64::add);
        let count = local.len();
        let minimum = local.iter().cloned().fold(0./0., f64::min);
        let maximum = local.iter().cloned().fold(0./0., f64::max);
        let average = total / local.len() as f64;
        let deviation = local.iter().map(|x|{ let a=x-average; a*a }).fold(0.0, f64::add) / local.len() as f64;
        let median = *local.get(local.len()/2).unwrap_or(&0.0);
        Statistics{
            total: total,
            count: count,
            minimum: minimum,
            maximum: maximum,
            average: average,
            median: median,
            deviation: deviation.sqrt()
        }
    }
}

impl fmt::Display for Statistics{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Samples:   Total:     Minimum:   Maximum:   Median:    Average:   Std. Dev:  \n{:10.5} {:10.5} {:10.5} {:10.5} {:10.5} {:10.5} {:10.5}",
               self.count, self.total, self.minimum, self.maximum, self.median, self.average, self.deviation)
    }
}
