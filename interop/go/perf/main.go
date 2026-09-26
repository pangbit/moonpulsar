// Matched workload adapter for the pinned official Go client.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	log "github.com/sirupsen/logrus"
)

type stats struct {
	mu           sync.Mutex
	buckets      [600002]uint64
	count, bytes uint64
	max          float64
}

func (s *stats) record(size int, us float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i := 600001
	if us <= 60000000 {
		i = int(math.Ceil(us / 100))
	}
	s.buckets[i]++
	s.count++
	s.bytes += uint64(size)
	s.max = math.Max(s.max, us)
}

func (s *stats) percentile(p float64) any {
	if s.count == 0 {
		return nil
	}
	rank := uint64(math.Ceil(float64(s.count) * p))
	var count uint64
	for i := 0; i <= 600000; i++ {
		count += s.buckets[i]
		if count >= rank {
			return i * 100
		}
	}
	return nil
}

func number(name string, fallback, min, max int) (int, error) {
	text := os.Getenv(name)
	if text == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(text)
	if err != nil || n < min || n > max {
		return 0, fmt.Errorf("invalid %s", name)
	}
	return n, nil
}

func run() error {
	log.SetLevel(log.ErrorLevel)
	mode := os.Getenv("PULSAR_PERF_MODE")
	if mode == "" {
		mode = "produce"
	}
	if mode != "produce" && mode != "consume" {
		return errors.New("invalid mode")
	}
	topic := os.Getenv("PULSAR_TOPIC")
	if topic == "" {
		return errors.New("PULSAR_TOPIC is required")
	}
	url := os.Getenv("PULSAR_URL")
	if url == "" {
		url = "pulsar://127.0.0.1:6650"
	}
	values := map[string]int{}
	for name, spec := range map[string][3]int{
		"PERF_WARMUP_MS": {5000, 0, 86400000}, "PERF_DURATION_MS": {30000, 1, 86400000},
		"PERF_SIZE": {1024, 1, 1048576}, "PERF_CONCURRENCY": {32, 1, 4096},
		"PERF_RATE": {0, 0, 10000000}, "PERF_BATCH_MESSAGES": {0, 0, 10000},
		"PERF_BATCH_DELAY_MS": {1, 1, 60000}, "PERF_QUEUE_SIZE": {1000, 1, 1000000},
		"PERF_TIMEOUT_MS": {10000, 1, 60000},
	} {
		n, err := number(name, spec[0], spec[1], spec[2])
		if err != nil {
			return err
		}
		values[name] = n
	}
	warmup := time.Duration(values["PERF_WARMUP_MS"]) * time.Millisecond
	duration := time.Duration(values["PERF_DURATION_MS"]) * time.Millisecond
	timeout := time.Duration(values["PERF_TIMEOUT_MS"]) * time.Millisecond
	options := pulsar.ClientOptions{URL: url, ConnectionTimeout: timeout, OperationTimeout: timeout}
	if token := os.Getenv("PULSAR_TOKEN"); token != "" {
		options.Authentication = pulsar.NewAuthenticationToken(token)
	}
	client, err := pulsar.NewClient(options)
	if err != nil {
		return err
	}
	defer client.Close()
	s := new(stats)
	if mode == "produce" {
		batch := values["PERF_BATCH_MESSAGES"]
		producer, err := client.CreateProducer(pulsar.ProducerOptions{
			Topic: topic, DisableBatching: batch == 0, BatchingMaxMessages: uint(batch),
			BatchingMaxSize: 131072, BatchingMaxPublishDelay: time.Duration(values["PERF_BATCH_DELAY_MS"]) * time.Millisecond,
			MaxPendingMessages: values["PERF_QUEUE_SIZE"], SendTimeout: timeout,
			CompressionType: pulsar.NoCompression, DisableBlockIfQueueFull: false,
		})
		if err != nil {
			return err
		}
		defer producer.Close()
		payload := make([]byte, values["PERF_SIZE"])
		for i := range payload {
			payload[i] = byte(i % 251)
		}
		start := time.Now()
		stop := start.Add(warmup + duration)
		ctx, cancel := context.WithDeadline(context.Background(), stop.Add(timeout))
		defer cancel()
		var ticket atomic.Uint64
		var wg sync.WaitGroup
		failures := make(chan error, values["PERF_CONCURRENCY"])
		for worker := 0; worker < values["PERF_CONCURRENCY"]; worker++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for time.Now().Before(stop) && ctx.Err() == nil {
					if rate := values["PERF_RATE"]; rate > 0 {
						due := start.Add(time.Duration(float64(ticket.Add(1)-1) * float64(time.Second) / float64(rate)))
						if !due.Before(stop) {
							return
						}
						time.Sleep(time.Until(due))
					}
					began := time.Now()
					if !began.Before(stop) {
						return
					}
					done := make(chan error, 1)
					// Send() forces a flush in this client; use SendAsync + wait to preserve batching.
					producer.SendAsync(ctx, &pulsar.ProducerMessage{Payload: payload}, func(_ pulsar.MessageID, _ *pulsar.ProducerMessage, e error) { done <- e })
					var e error
					select {
					case e = <-done:
					case <-ctx.Done():
						e = ctx.Err()
					}
					if e != nil {
						failures <- e
						cancel()
						return
					}
					ended := time.Now()
					if !began.Before(start.Add(warmup)) && ended.Before(stop) {
						s.record(len(payload), float64(ended.Sub(began))/1000)
					}
				}
			}()
		}
		wg.Wait()
		close(failures)
		for e := range failures {
			return e
		}
		time.Sleep(time.Until(stop))
	} else {
		subscription := os.Getenv("PULSAR_SUBSCRIPTION")
		if subscription == "" {
			subscription = "perf"
		}
		consumer, err := client.Subscribe(pulsar.ConsumerOptions{
			Topic: topic, SubscriptionName: subscription, Type: pulsar.Exclusive,
			SubscriptionMode: pulsar.NonDurable, SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
			ReceiverQueueSize: values["PERF_QUEUE_SIZE"], AckGroupingOptions: &pulsar.AckGroupingOptions{MaxSize: 1, MaxTime: 0},
		})
		if err != nil {
			return err
		}
		defer consumer.Close()
		start := time.Now()
		stop := start.Add(warmup + duration)
		ctx, cancel := context.WithDeadline(context.Background(), stop)
		defer cancel()
		for time.Now().Before(stop) {
			message, err := consumer.Receive(ctx)
			if errors.Is(err, context.DeadlineExceeded) {
				break
			}
			if err != nil {
				return err
			}
			received := time.Now()
			if err := consumer.Ack(message); err != nil {
				return err
			}
			if !received.Before(start.Add(warmup)) && time.Now().Before(stop) {
				s.record(len(message.Payload()), 0)
			}
		}
	}
	if s.count == 0 {
		return errors.New("no measured messages")
	}
	var latency any
	if mode == "produce" {
		latency = map[string]any{"unit": "us", "bucket_width_us": 100, "p50": s.percentile(.5), "p95": s.percentile(.95), "p99": s.percentile(.99), "p999": s.percentile(.999), "max": s.max, "overflow_count": s.buckets[600001]}
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{
		"status": "complete", "mode": mode, "messages": s.count, "payload_bytes": s.bytes,
		"messages_per_second": float64(s.count) / duration.Seconds(), "payload_mib_per_second": float64(s.bytes) / duration.Seconds() / 1048576,
		"send_receipt_latency": latency,
	})
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
