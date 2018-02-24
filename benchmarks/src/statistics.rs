use std::ops::Add;
use std::convert::From;
use std::time::Duration;
use std::fmt;

pub struct Statistics{
    pub total: f64,
    pub minimum: f64,
    pub maximum: f64,
    pub average: f64,
    pub median: f64,
    pub deviation: f64
}

fn duration_fsecs(d: &Duration) -> f64{
    d.as_secs() as f64 + d.subsec_nanos() as f64 / 10_000_000 as f64
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
        let minimum = local.iter().cloned().fold(0./0., f64::min);
        let maximum = local.iter().cloned().fold(0./0., f64::max);
        let average = total / local.len() as f64;
        let deviation = local.iter().map(|x|{ let a=x-average; a*a }).fold(0.0, f64::add) / local.len() as f64;
        let median = local[local.len()/2];
        Statistics{
            total: total,
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
        write!(f, "Total:     Minimum:   Maximum:   Median:    Average:   Std. Dev:  \n{:10.5} {:10.5} {:10.5} {:10.5} {:10.5} {:10.5}",
               self.total, self.minimum, self.maximum, self.median, self.average, self.deviation)
    }
}
