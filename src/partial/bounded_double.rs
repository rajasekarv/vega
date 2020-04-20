use std::convert::From;
use std::fmt::{Display, Error as FmtError, Formatter};

use crate::StdResult;

#[derive(Debug)]
pub struct BoundedDouble {
    mean: f64,
    confidence: f64,
    low: f64,
    high: f64,
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

impl PartialEq for BoundedDouble {
    fn eq(&self, other: &Self) -> bool {
        self.mean == other.mean
            && self.confidence == other.confidence
            && self.low == other.low
            && self.high == other.high
    }
}

impl Display for BoundedDouble {
    // override def toString(): String = "[%.3f, %.3f]".format(low, high)
    fn fmt(&self, _fmt: &mut Formatter) -> StdResult<(), FmtError> {
        todo!()
    }
}
