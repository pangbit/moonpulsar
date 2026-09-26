//! Matched workload adapter; the upstream Pulsar client is unmodified.
use futures::{future::try_join_all, TryStreamExt};
use pulsar::{
    consumer::InitialPosition, producer::ProducerOptions, proto::command_subscribe::SubType,
    Authentication, Consumer, ConsumerOptions, Pulsar, TokioExecutor,
};
use serde_json::{json, Value};
use std::{
    env,
    error::Error,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::Mutex as AsyncMutex;

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

struct Stats {
    buckets: Vec<u64>,
    count: u64,
    bytes: u64,
    max: f64,
}
impl Stats {
    fn new() -> Self {
        Self {
            buckets: vec![0; 600002],
            count: 0,
            bytes: 0,
            max: 0.0,
        }
    }
    fn record(&mut self, size: usize, us: f64) {
        let i = if us > 60_000_000.0 {
            600001
        } else {
            (us / 100.0).ceil() as usize
        };
        self.buckets[i] += 1;
        self.count += 1;
        self.bytes += size as u64;
        self.max = self.max.max(us);
    }
    fn percentile(&self, p: f64) -> Value {
        if self.count == 0 {
            return Value::Null;
        }
        let rank = (self.count as f64 * p).ceil() as u64;
        let mut count = 0;
        for i in 0..=600000 {
            count += self.buckets[i];
            if count >= rank {
                return json!(i * 100);
            }
        }
        Value::Null
    }
}

fn number(name: &str, fallback: u64, min: u64, max: u64) -> Result<u64> {
    let n = env::var(name).map(|s| s.parse()).unwrap_or(Ok(fallback))?;
    if n < min || n > max {
        return Err(format!("invalid {name}").into());
    }
    Ok(n)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let mode = env::var("PULSAR_PERF_MODE").unwrap_or("produce".into());
    if mode != "produce" && mode != "consume" {
        return Err("invalid mode".into());
    }
    let topic = env::var("PULSAR_TOPIC")?;
    if topic.is_empty() {
        return Err("PULSAR_TOPIC is required".into());
    }
    let warmup = Duration::from_millis(number("PERF_WARMUP_MS", 5000, 0, 86400000)?);
    let duration = Duration::from_millis(number("PERF_DURATION_MS", 30000, 1, 86400000)?);
    let timeout = Duration::from_millis(number("PERF_TIMEOUT_MS", 10000, 1, 60000)?);
    let size = number("PERF_SIZE", 1024, 1, 1048576)? as usize;
    let concurrency = number("PERF_CONCURRENCY", 32, 1, 4096)?;
    let batch = number("PERF_BATCH_MESSAGES", 0, 0, 10000)? as u32;
    let delay = Duration::from_millis(number("PERF_BATCH_DELAY_MS", 1, 1, 60000)?);
    let queue = number("PERF_QUEUE_SIZE", 1000, 1, 1000000)? as usize;
    let rate = number("PERF_RATE", 0, 0, 10000000)?;
    let url = env::var("PULSAR_URL").unwrap_or("pulsar://127.0.0.1:6650".into());
    let mut builder = Pulsar::builder(url, TokioExecutor).with_outbound_channel_size(queue);
    if let Ok(token) = env::var("PULSAR_TOKEN") {
        builder = builder.with_auth(Authentication {
            name: "token".into(),
            data: token.into_bytes(),
        });
    }
    let client = tokio::time::timeout(timeout, builder.build()).await??;
    let stats = Arc::new(Mutex::new(Stats::new()));
    if mode == "produce" {
        let options = ProducerOptions {
            batch_size: if batch == 0 { None } else { Some(batch) },
            batch_byte_size: if batch == 0 { None } else { Some(131072) },
            batch_timeout: if batch == 0 { None } else { Some(delay) },
            block_queue_if_full: true,
            ..Default::default()
        };
        let producer = tokio::time::timeout(
            timeout,
            client
                .producer()
                .with_topic(&topic)
                .with_options(options)
                .build(),
        )
        .await??;
        let producer = Arc::new(AsyncMutex::new(producer));
        let payload: Arc<Vec<u8>> = Arc::new((0..size).map(|i| (i % 251) as u8).collect());
        let clock = Instant::now();
        let stop = clock + warmup + duration;
        let ticket = Arc::new(Mutex::new(0_u64));
        let mut workers = Vec::new();
        for _ in 0..concurrency {
            let producer = producer.clone();
            let payload = payload.clone();
            let stats = stats.clone();
            let ticket = ticket.clone();
            workers.push(async move {
                while Instant::now() < stop {
                    if rate > 0 {
                        let n = {
                            let mut t = ticket.lock().unwrap();
                            let n = *t;
                            *t += 1;
                            n
                        };
                        let due = clock + Duration::from_secs_f64(n as f64 / rate as f64);
                        if due >= stop {
                            break;
                        }
                        tokio::time::sleep_until(due.into()).await;
                    }
                    let began = Instant::now();
                    if began >= stop {
                        break;
                    }
                    tokio::time::timeout(timeout, async {
                        // Serialize access only while enqueueing; await receipt outside the lock.
                        let receipt = producer
                            .lock()
                            .await
                            .send_non_blocking(payload.as_slice())
                            .await?;
                        receipt.await?;
                        Ok::<(), pulsar::Error>(())
                    })
                    .await??;
                    let ended = Instant::now();
                    if began >= clock + warmup && ended < stop {
                        stats
                            .lock()
                            .unwrap()
                            .record(size, (ended - began).as_secs_f64() * 1e6);
                    }
                }
                Ok::<(), Box<dyn Error + Send + Sync>>(())
            });
        }
        tokio::time::timeout(warmup + duration + timeout, try_join_all(workers)).await??;
        tokio::time::sleep_until(stop.into()).await;
        tokio::time::timeout(timeout, producer.lock().await.close()).await??;
    } else {
        let subscription = env::var("PULSAR_SUBSCRIPTION").unwrap_or("perf".into());
        let options = ConsumerOptions::default()
            .durable(false)
            .with_initial_position(InitialPosition::Earliest)
            .with_receiver_queue_size(queue as u32);
        let mut consumer: Consumer<Vec<u8>, _> = tokio::time::timeout(
            timeout,
            client
                .consumer()
                .with_topic(&topic)
                .with_subscription(subscription)
                .with_subscription_type(SubType::Exclusive)
                .with_options(options)
                .build(),
        )
        .await??;
        let clock = Instant::now();
        let stop = clock + warmup + duration;
        while Instant::now() < stop {
            let message = match tokio::time::timeout_at(stop.into(), consumer.try_next()).await {
                Err(_) => break,
                Ok(result) => result?.ok_or("consumer closed")?,
            };
            let received = Instant::now();
            tokio::time::timeout(timeout, consumer.ack(&message)).await??;
            if received >= clock + warmup && Instant::now() < stop {
                stats
                    .lock()
                    .unwrap()
                    .record(message.payload.data.len(), 0.0);
            }
        }
        tokio::time::timeout(timeout, consumer.close()).await??;
    }
    let s = stats.lock().unwrap();
    if s.count == 0 {
        return Err("no measured messages".into());
    }
    let latency = if mode == "produce" {
        json!({"unit":"us", "bucket_width_us":100, "p50":s.percentile(0.5), "p95":s.percentile(0.95), "p99":s.percentile(0.99), "p999":s.percentile(0.999), "max":s.max, "overflow_count":s.buckets[600001]})
    } else {
        Value::Null
    };
    println!(
        "{}",
        json!({"status":"complete", "mode":mode, "messages":s.count, "payload_bytes":s.bytes, "messages_per_second":s.count as f64/duration.as_secs_f64(), "payload_mib_per_second":s.bytes as f64/duration.as_secs_f64()/1048576.0, "send_receipt_latency":latency})
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn histogram() {
        let mut s = Stats::new();
        assert_eq!(s.percentile(0.99), Value::Null);
        for v in [0.0, 1.0, 100.0, 101.0] {
            s.record(1024, v);
        }
        assert_eq!(s.percentile(0.5), json!(100));
        assert_eq!(s.percentile(0.99), json!(200));
        assert_eq!(s.bytes, 4096);
        let mut s = Stats::new();
        s.record(1, 60_000_000.0);
        s.record(1, 60_000_001.0);
        assert_eq!(s.percentile(0.5), json!(60_000_000));
        assert_eq!(s.percentile(0.99), Value::Null);
        assert_eq!(s.buckets[600001], 1);
    }
}
