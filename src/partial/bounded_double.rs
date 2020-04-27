use std::convert::From;
use std::fmt::{Display, Error as FmtError, Formatter};

use crate::StdResult;

#[derive(Clone, Debug, Copy, PartialEq)]
pub struct BoundedDouble {
    pub mean: f64,
    pub confidence: f64,
    pub low: f64,
    pub high: f64,
}

impl From<(f64, f64, f64, f64)> for BoundedDouble {
    fn from(src: (f64, f64, f64, f64)) -> BoundedDouble {
        let (mean, confidence, low, high) = src;
        BoundedDouble {
            mean,
            confidence,
            low,
            high,
        }
    }
}

impl Display for BoundedDouble {
    fn fmt(&self, _fmt: &mut Formatter) -> StdResult<(), FmtError> {
        // override def toString(): String = "[%.3f, %.3f]".format(low, high)
        todo!()
    }
}
