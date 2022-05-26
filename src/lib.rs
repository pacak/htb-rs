#![doc = include_str!("../README.md")]
use std::num::TryFromIntError;
use std::ops::Index;
use std::time::Duration;

impl<T> Index<T> for HTB<T>
where
    usize: From<T>,
{
    type Output = usize;

    fn index(&self, index: T) -> &Self::Output {
        &self.state[usize::from(index)].value
    }
}

/// Internal bucket state representation
#[derive(Debug, Copy, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
struct Bucket {
    /// capacity, in abstract units
    cap: usize,
    /// currently contained value
    value: usize,
}

/// Bucket configuration
#[derive(Copy, Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct BucketCfg<T> {
    /// Current bucket name
    pub this: T,
    /// Parent name
    pub parent: Option<T>,
    /// Allowed flow rate in number of tokens per duration
    pub rate: (usize, Duration),
    /// Burst capacity in tokens, can be 0 if burst is not required
    /// at this step.
    ///
    /// If tokens are going to be consumed directly from this bucket
    /// this also limits how granular rate restriction can be.
    ///
    /// For example for a rate of 10 tokens per second
    /// capacity of 10 means you can consume 10 tokens every second
    /// at once or as 10 individual events distributed in any way though
    /// this second.
    /// capacity of 5 gives the same rate of 10 tokens per second bucket
    /// on average but 5 tokens must be consumed in first half of the second
    /// and 5 remaining tokens - in the second half of the second.
    pub capacity: usize,
}

#[derive(Clone, Copy, Debug)]
pub enum Error {
    /// First bucket passed to [`HTB::new`] must be a node with `parent` set to None
    NoRoot,
    /// Calculated flow rate is higher that what can fit into usize
    ///
    /// flow rate is calculated using least common multiplier and if it is very small
    /// HTB ends up sing their product which can overflow. To fix this problem try to tweak
    /// the values to have bigger LCM. For example instead of using 881 and 883 (both are prime
    /// numbers) try using 882
    InvalidRate,

    /// Invalid config passed to HTB:
    ///
    /// Buckets should be given in depth first search traversal order:
    /// - root with `parent` set to None
    /// - higher priority child of the root
    /// - followed by high priority child of the child, if any, etc.
    /// - followed by the next child
    InvalidStructure,
}
impl std::error::Error for Error {}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::NoRoot => f.write_str("Problem with a root node of some sort"),
            Error::InvalidRate => f.write_str("Requested message rate can't be represented"),
            Error::InvalidStructure => f.write_str("Problem with message structure"),
        }
    }
}

impl From<TryFromIntError> for Error {
    fn from(_: TryFromIntError) -> Self {
        Error::InvalidRate
    }
}

/// Hierarchical Token Bucket structure
///
/// You can advance time for HTB structure using [`advance`][Self::advance] and
/// [`advance_ns`][Self::advance_ns] and examine/alter internal state using
/// [`peek`][Self::peek]/[`take`][Self::take].
///
/// When several buckets are feeding from a single parent earlier one gets a priority
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct HTB<T> {
    state: Vec<Bucket>,
    ops: Vec<Op<T>>,
    /// Normalized unit cost, each nanosecond corresponds to this many units
    pub unit_cost: usize,
    /// Maximum time required to refill every possible cell
    time_limit: usize,
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
enum Op<T> {
    Inflow(usize),
    Take(T, usize),
    Deposit(T),
}

fn lcm(a: u128, b: u128) -> u128 {
    (a * b) / gcd::Gcd::gcd(a, b)
}

impl<T> HTB<T>
where
    T: Copy + Eq + PartialEq,
    usize: From<T>,
{
    /// Create HTB for a given bucket configuration
    ///
    /// Buckets should be given in depth first search traversal order:
    /// - root with `parent` set to None
    /// - higher priority child of the root
    /// - followed by high priority child of the child, if any, etc.
    /// - followed by the next child
    ///
    /// # Errors
    /// If bucket configuration is invalid - returns an [`Error`] type describing a problem
    pub fn new(tokens: &[BucketCfg<T>]) -> Result<Self, Error> {
        if tokens.is_empty() || tokens[0].parent.is_some() {
            return Err(Error::NoRoot);
        }

        // first we need to convert flow rate from items per unit of time
        // to fractions per nanosecond
        let unit_cost: usize = tokens
            .iter()
            .map(|cfg| cfg.rate.1.as_nanos())
            .reduce(lcm)
            .ok_or(Error::NoRoot)?
            .try_into()?;
        let rates = tokens
            .iter()
            .map(|cfg| {
                usize::try_from(cfg.rate.0 as u128 * unit_cost as u128 / cfg.rate.1.as_nanos())
            })
            .collect::<Result<Vec<_>, _>>()?;

        let things = tokens.iter().zip(rates.iter().copied()).enumerate();

        let mut ops = Vec::new();
        let mut items = Vec::new();
        let mut stack = Vec::new();

        for (ix, (cur, rate)) in things {
            // items must be given in form of depth first traversal
            if ix != cur.this.into() {
                return Err(Error::InvalidStructure);
            }

            // sanity check, first item must be root
            if items.is_empty() && cur.parent.is_some() {
                return Err(Error::NoRoot);
            }

            if cur.capacity as u128 * unit_cost as u128 > usize::MAX as u128 {
                return Err(Error::InvalidRate);
            }

            items.push(Bucket {
                cap: cur.capacity * unit_cost,
                value: cur.capacity * unit_cost,
            });

            if cur.parent.as_ref() != stack.last() {
                loop {
                    if let Some(parent) = stack.last() {
                        if Some(parent) == cur.parent.as_ref() {
                            ops.push(Op::Deposit(*parent));
                            break;
                        }
                        ops.push(Op::Deposit(*parent));
                        stack.pop();
                    } else {
                        return Err(Error::InvalidStructure);
                    }
                }
            }

            stack.push(cur.this);
            match cur.parent {
                Some(parent) => ops.push(Op::Take(parent, rate)),
                None => ops.push(Op::Inflow(rate)),
            }
        }
        for leftover in stack.iter().rev().copied() {
            ops.push(Op::Deposit(leftover));
        }

        let limit = unit_cost as u128 * rates.iter().map(|r| *r as u128).sum::<u128>();
        if limit > usize::MAX as u128 / 2 {
            // In this case is possible for "flow" to overflow the usize
            return Err(Error::InvalidRate);
        }

        Ok(Self {
            unit_cost,
            state: items,
            ops,
            time_limit: limit as usize,
        })
    }

    /// Advance time by number of nanoseconds
    ///
    /// Updates internal structure, see also [`advance`][Self::advance]
    ///
    /// # Performance
    ///
    /// Update cost is O(N) where N is number of buckets
    pub fn advance_ns(&mut self, time_diff: usize) {
        // we start at the top and insert new tokens according to this rules:
        // 1. at most `rate * time_diff` is propagated via links
        // 2. incoming `rate * time_diff` is combined with stored values
        // 3. unused tokens go back and deposited at the previous level
        // 4. at most incoming `rate * time_diff` is propagated back!
        // 5. at most `capacity` is deposited to nodes after the final pass
        let mut flow = 0;
        let time_diff = std::cmp::min(time_diff, self.time_limit);
        for op in self.ops.iter().copied() {
            match op {
                Op::Inflow(rate) => flow = rate * time_diff,
                Op::Take(k, rate) => {
                    let combined = flow + self.state[usize::from(k)].value;
                    flow = combined.min(rate * time_diff);
                    self.state[usize::from(k)].value = combined - flow;
                }
                Op::Deposit(k) => {
                    let ix = usize::from(k);
                    let combined = flow + self.state[ix].value;
                    let deposited = self.state[ix].cap.min(combined);
                    self.state[ix].value = deposited;
                    if combined > deposited {
                        flow = combined - deposited;
                    } else {
                        flow = 0;
                    }
                }
            }
        }
    }

    /// Advance time by [`Duration`]
    ///
    /// Updates internal structure, see also [`advance_ns`][Self::advance_ns]
    pub fn advance(&mut self, time_diff: Duration) {
        self.advance_ns(time_diff.as_nanos() as usize);
    }

    /// Check if there's at least one token available at index `T`
    ///
    /// See also [`peek_n`][Self::peek_n]
    pub fn peek(&self, label: T) -> bool {
        self.state[usize::from(label)].value >= self.unit_cost
    }

    /// Check if there's at least `cnt` tokens available at index `T`
    ///
    /// See also [`peek`][Self::peek]
    pub fn peek_n(&self, label: T, cnt: usize) -> bool {
        self.state[usize::from(label)].value >= self.unit_cost * cnt
    }

    /// Consume a single token from `T`
    ///
    /// See also [`take_n`][Self::take_n]
    pub fn take(&mut self, label: T) -> bool {
        let item = &mut self.state[usize::from(label)];
        match item.value.checked_sub(self.unit_cost) {
            Some(new) => {
                item.value = new;
                true
            }
            None => false,
        }
    }

    /// Consume `cnt` tokens from `T`
    ///
    /// See also [`take`][Self::take]
    pub fn take_n(&mut self, label: T, cnt: usize) -> bool {
        let item = &mut self.state[usize::from(label)];
        match item.value.checked_sub(self.unit_cost * cnt) {
            Some(new) => {
                item.value = new;
                true
            }
            None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[derive(Copy, Clone, Debug, Eq, PartialEq)]
    enum Rate {
        Long,
        Short,
        Hedge,
        HedgeFut,
        Make,
    }

    impl From<Rate> for usize {
        fn from(rate: Rate) -> Self {
            rate as usize
        }
    }

    fn sample_htb() -> HTB<Rate> {
        HTB::new(&[
            BucketCfg {
                this: Rate::Long,
                parent: None,
                rate: (100, Duration::from_millis(200)),
                capacity: 1500,
            },
            BucketCfg {
                this: Rate::Short,
                parent: Some(Rate::Long),
                rate: (250, Duration::from_secs(1)),
                capacity: 250,
            },
            BucketCfg {
                this: Rate::Hedge,
                parent: Some(Rate::Short),
                rate: (1000, Duration::from_secs(1)),
                capacity: 10,
            },
            BucketCfg {
                this: Rate::HedgeFut,
                parent: Some(Rate::Hedge),
                rate: (2000, Duration::from_secs(2)),
                capacity: 10,
            },
            BucketCfg {
                this: Rate::Make,
                parent: Some(Rate::Short),
                rate: (1000, Duration::from_secs(1)),
                capacity: 6,
            },
        ])
        .unwrap()
    }
    #[test]
    fn it_works() {
        let mut htb = sample_htb();
        assert!(htb.take_n(Rate::Hedge, 4));
        assert!(htb.take_n(Rate::Hedge, 4));
        assert!(htb.take_n(Rate::Hedge, 2));
        assert!(!htb.take_n(Rate::Hedge, 1));
        htb.advance(Duration::from_millis(1));
        assert!(htb.peek_n(Rate::Hedge, 1));
        assert!(!htb.peek_n(Rate::Hedge, 2));
        assert!(htb.take(Rate::Hedge));
        assert!(!htb.take(Rate::Hedge));
        htb.advance(Duration::from_millis(5));
        assert!(htb.peek_n(Rate::Hedge, 5));
        assert!(!htb.peek_n(Rate::Hedge, 6));
        htb.advance_ns(usize::MAX / 2);
        assert!(htb.take_n(Rate::Hedge, 4));
        htb.advance_ns(usize::MAX);
        assert!(htb.take_n(Rate::Hedge, 4));
    }
}
