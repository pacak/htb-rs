# Hierarchical token Bucket

This crate implements Hierarchical Token Bucket algorithm with fixed structure
<https://en.wikipedia.org/wiki/Token_bucket#Hierarchical_token_bucket>

Crate does not rely on periodic updates to maintain the token buckets which
means it can be updated right before requesting tokens

```rust
use htb::*;
use std::time::Duration;

#[derive(Clone, Copy, Eq, PartialEq)]
enum Rate {
    Long,
    Short,
}

impl From<Rate> for usize {
    fn from(rate: Rate) -> Self {
        match rate {
            Rate::Long => 0,
            Rate::Short => 1,
        }
    }
}

// let's implement a rate limiter with two required properties:
// - packet rate should not exceed 250 msg per second
// - packet rate should not exceed 1500 msg per 15 seconds

let mut htb = HTB::new(&[
    BucketCfg {
        this: Rate::Long,
        parent: None,
        rate: (1500, Duration::from_secs(15)),
        capacity: 0,
    },
    BucketCfg {
        this: Rate::Short,
        parent: Some(Rate::Long),
        rate: (250, Duration::from_secs(1)),
        capacity: 250,
    },
])?;

// we are allowed a single 250 token burst
assert!(htb.take_n(Rate::Short, 250));
assert!(!htb.peek(Rate::Short));
htb.advance(Duration::from_secs(1));

// after this point established packet rate obeys "long" indefinitely
for _ in 0..10 {
    assert!(htb.take_n(Rate::Short, 100));
    assert!(!htb.peek(Rate::Short));
    htb.advance(Duration::from_secs(1));
}

// if we stop consuming tokens for some time
htb.advance(Duration::from_secs(10));
assert!(htb.take_n(Rate::Short, 250));
// we get more bursts
assert!(!htb.peek(Rate::Short));
# Ok::<(), Error>(())
```
