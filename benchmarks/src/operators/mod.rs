macro_rules! exchange {
    ( $x:expr ) => {
        {
            let key = Rc::new($x);
            let key_ = key.clone();
            let exchange = Exchange::new(move |d| {
                let mut h: ::fnv::FnvHasher = Default::default();
                key_(d).hash(&mut h);
                h.finish()
            });
            (key, exchange)
        }
    };
    ( $x:expr, $d:expr ) => {
        {
            let key = Rc::new($x);
            let key_ = key.clone();
            let exchange = Exchange::new(move |d| {
                let mut h: ::fnv::FnvHasher = Default::default();
                $d(key_(d)).hash(&mut h);
                h.finish()
            });
            (key, exchange)
        }
    };
}

pub use self::rollingcount::RollingCount;
pub use self::window::Window;
pub use self::join::Join;
pub use self::reduce::Reduce;
pub use self::filtermap::FilterMap;
pub use self::timer::Timer;
pub use self::session::Session;

pub mod rollingcount;
pub mod window;
pub mod join;
pub mod reduce;
pub mod filtermap;
pub mod timer;
pub mod session;
