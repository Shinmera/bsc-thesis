use timely::progress::nested::product::Product;
use timely::progress::timestamp::{Timestamp, RootTimestamp};

/// Shorthand for creating an Exchange instance based on a key extractor.
///
/// This is used frequently to wrap around the necessary Rc logic.
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

/// Simple trait to convert timestamps into integers so we can calculate with them.
///
/// Initially I wanted to do a much more generic scheme that allowed
/// you to use whatever kind of construct you wanted without having
/// to coerce to an integer. However, due to Rust's limitations and
/// constraints regarding types and trait implementations, this made
/// the design a pain in the ass to manage and work with.
///
/// So I give up. You win, Rust. I'll do the ugly, but at least
/// workable, hack instead. Thanks for nothing.
pub trait IntegerTimestamp: Timestamp {
    fn to_integer(&self) -> usize;
    fn from_integer(usize) -> Self;
}

impl<N: IntegerTimestamp+Timestamp> IntegerTimestamp for Product<RootTimestamp, N> {
    #[inline]
    fn to_integer(&self) -> usize {
        self.inner.to_integer()
    }
    #[inline]
    fn from_integer(arg: usize) -> Self {
        RootTimestamp::new(N::from_integer(arg))
    }
}

impl IntegerTimestamp for () {
    #[inline]
    fn to_integer(&self) -> usize { 0 }
    #[inline]
    fn from_integer(_: usize) -> Self { () }
}

macro_rules! integer_timestampify {
    ( $x:ident ) => {
        impl IntegerTimestamp for $x {
            #[inline]
            fn to_integer(&self) -> usize { *self as usize }
            #[inline]
            fn from_integer(arg: usize) -> Self { arg as $x }
        }
    };
}

integer_timestampify!(i32);
integer_timestampify!(u32);
integer_timestampify!(u64);
integer_timestampify!(usize);

pub use self::filtermap::FilterMap;
pub use self::join::Join;
pub use self::partition::Partition;
pub use self::reduce::Reduce;
pub use self::rollingcount::RollingCount;
pub use self::session::Session;
pub use self::window::Window;

pub mod filtermap;
pub mod join;
pub mod partition;
pub mod reduce;
pub mod rollingcount;
pub mod session;
pub mod window;
