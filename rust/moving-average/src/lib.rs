// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

#![forbid(unsafe_code)]

use std::collections::VecDeque;

// TPS data
pub struct MovingAverage {
    window_millis: u64,
    // (timestamp_millis, value)
    values: VecDeque<(u64, u64)>,
    sum: u64,
}

impl MovingAverage {
    pub fn new(window_millis: u64) -> Self {
        let now = chrono::Utc::now().naive_utc().timestamp_millis() as u64;
        let mut queue = VecDeque::new();
        queue.push_back((now, 0));
        Self {
            window_millis,
            values: queue,
            sum: 0,
        }
    }

    pub fn tick_now(&mut self, value: u64) {
        let now = chrono::Utc::now().naive_utc().timestamp_millis() as u64;
        self.tick(now, value);
    }

    pub fn tick(&mut self, timestamp_millis: u64, value: u64) -> f64 {
        self.values.push_back((timestamp_millis, value));
        self.sum += value;
        while self.values.len() > 2 {
            match self.values.front() {
                None => break,
                Some((ts, val)) => {
                    if timestamp_millis - ts > self.window_millis {
                        self.sum -= val;
                        self.values.pop_front();
                    } else {
                        break;
                    }
                },
            }
        }
        self.avg()
    }

    // Only be called after tick_now/tick is called.
    pub fn avg(&self) -> f64 {
        if self.values.len() < 2 {
            0.0
        } else {
            let elapsed = self.values.back().unwrap().0 - self.values.front().unwrap().0;
            (self.sum * 1000) as f64 / elapsed as f64
        }
    }

    pub fn sum(&self) -> u64 {
        self.sum
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_moving_average() {
        // 10 Second window.
        let mut ma = MovingAverage::new(10_000);
        // 9 seconds spent at 100 TPS.
        for _ in 0..9 {
            ma.tick_now(100);
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
        // No matter what algorithm we use, the average should be 99 at least.
        let avg = ma.avg();
        assert!(avg >= 99.0, "Average is too low: {}", avg);
    }
}
