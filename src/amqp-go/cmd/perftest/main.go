package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"
	amqp "github.com/rabbitmq/amqp091-go"
)

// latencyHistogram wraps HDR histogram with a mutex for concurrent access.
type latencyHistogram struct {
	mu   sync.Mutex
	hist *hdrhistogram.Histogram
}

func newLatencyHistogram() *latencyHistogram {
	// Range: 1µs to 30s, 3 significant figures
	return &latencyHistogram{
		hist: hdrhistogram.New(1, 30_000_000, 3),
	}
}

func (h *latencyHistogram) record(d time.Duration) {
	us := d.Microseconds()
	if us < 1 {
		us = 1
	}
	h.mu.Lock()
	_ = h.hist.RecordValue(us)
	h.mu.Unlock()
}

func (h *latencyHistogram) percentile(p float64) time.Duration {
	h.mu.Lock()
	v := h.hist.ValueAtQuantile(p)
	h.mu.Unlock()
	return time.Duration(v) * time.Microsecond
}

func (h *latencyHistogram) totalCount() int64 {
	h.mu.Lock()
	v := h.hist.TotalCount()
	h.mu.Unlock()
	return v
}

// stats holds all counters shared across goroutines.
type stats struct {
	published atomic.Int64
	consumed  atomic.Int64
	confirmed atomic.Int64

	// Confirm-mode failure accounting: publishes never confirmed within the
	// post-run grace period, and negative confirms (basic.nack).
	unconfirmed atomic.Int64
	nacked      atomic.Int64

	consumerLatency *latencyHistogram
	confirmLatency  *latencyHistogram
}

func newStats() *stats {
	return &stats{
		consumerLatency: newLatencyHistogram(),
		confirmLatency:  newLatencyHistogram(),
	}
}

func main() {
	var (
		url          = flag.String("url", "amqp://guest:guest@localhost:5672/", "AMQP server URL")
		producers    = flag.Int("producers", 1, "Number of producer goroutines")
		consumers    = flag.Int("consumers", 1, "Number of consumer goroutines")
		duration     = flag.Duration("duration", 30*time.Second, "Test run duration")
		size         = flag.Int("size", 1024, "Message body size in bytes")
		confirm      = flag.Bool("confirm", false, "Enable publisher confirms and track confirmed msgs/s; the run FAILS if any publish is never confirmed")
		confirmGrace = flag.Duration("confirm-grace", 15*time.Second, "How long to wait after the run for outstanding confirms to drain (confirm mode)")
		outstanding  = flag.Int("outstanding", 1000, "Max unconfirmed publishes in flight per producer (confirm mode; mirrors RabbitMQ PerfTest --confirm). 0 = unbounded")
		prefetch     = flag.Int("prefetch", 100, "Consumer QoS prefetch count")
		queue        = flag.String("queue", "perf-test", "Queue name")
		durable      = flag.Bool("durable", false, "Declare durable queue and use persistent messages")
		exchange     = flag.String("exchange", "", "Exchange name (default = default exchange)")
	)
	flag.Parse()

	st := newStats()

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		cancel()
	}()

	startTime := time.Now()

	var wg sync.WaitGroup

	for i := 0; i < *producers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runProducer(ctx, *url, *queue, *exchange, *size, *durable, *confirm, *confirmGrace, *outstanding, st)
		}()
	}

	for i := 0; i < *consumers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runConsumer(ctx, *url, *queue, *prefetch, *durable, st)
		}()
	}

	// Per-second stats printer
	go printPerSecondStats(ctx, st)

	wg.Wait()

	elapsed := time.Since(startTime)
	printSummary(elapsed, *confirm, st)

	// Confirm mode is a correctness gate, not just a benchmark: a publish that
	// is never confirmed (or is nacked) fails the whole run.
	if *confirm {
		if unconfirmed, nacked := st.unconfirmed.Load(), st.nacked.Load(); unconfirmed > 0 || nacked > 0 {
			fmt.Printf("\nFAIL: %d publish(es) never confirmed, %d nacked\n", unconfirmed, nacked)
			os.Exit(1)
		}
	}
}

func printPerSecondStats(ctx context.Context, st *stats) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var (
		lastPub int64
		lastCon int64
		sec     int
	)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sec++
			pub := st.published.Load()
			con := st.consumed.Load()
			sentRate := pub - lastPub
			recvRate := con - lastCon
			lastPub = pub
			lastCon = con

			if st.consumerLatency.totalCount() > 0 {
				p50 := st.consumerLatency.percentile(50)
				p95 := st.consumerLatency.percentile(95)
				p99 := st.consumerLatency.percentile(99)
				fmt.Printf("[%ds]  sent: %d/s  recv: %d/s  lat p50=%s p95=%s p99=%s\n",
					sec, sentRate, recvRate,
					formatDuration(p50), formatDuration(p95), formatDuration(p99))
			} else {
				fmt.Printf("[%ds]  sent: %d/s  recv: %d/s\n", sec, sentRate, recvRate)
			}
		}
	}
}

func printSummary(elapsed time.Duration, withConfirm bool, st *stats) {
	pub := st.published.Load()
	con := st.consumed.Load()
	lost := pub - con
	if lost < 0 {
		lost = 0
	}
	lostPct := 0.0
	if pub > 0 {
		lostPct = float64(lost) / float64(pub) * 100
	}

	secs := elapsed.Seconds()
	fmt.Printf("\n=== SUMMARY (%.0fs) ===\n", secs)
	fmt.Printf("Published : %d total  |  %.0f avg/s\n", pub, float64(pub)/secs)
	fmt.Printf("Consumed  : %d total  |  %.0f avg/s\n", con, float64(con)/secs)
	fmt.Printf("Lost      : %d (%.2f%%)\n", lost, lostPct)
	if withConfirm {
		confirmed := st.confirmed.Load()
		fmt.Printf("Confirmed : %d total  |  %.0f avg/s  (unconfirmed: %d, nacked: %d)\n",
			confirmed, float64(confirmed)/secs, st.unconfirmed.Load(), st.nacked.Load())
	}

	if st.consumerLatency.totalCount() > 0 {
		fmt.Printf("Consumer latency  p50=%s  p75=%s  p95=%s  p99=%s  p99.9=%s\n",
			formatDuration(st.consumerLatency.percentile(50)),
			formatDuration(st.consumerLatency.percentile(75)),
			formatDuration(st.consumerLatency.percentile(95)),
			formatDuration(st.consumerLatency.percentile(99)),
			formatDuration(st.consumerLatency.percentile(99.9)),
		)
	}

	if withConfirm && st.confirmLatency.totalCount() > 0 {
		fmt.Printf("Confirm latency   p50=%s  p75=%s  p95=%s  p99=%s  p99.9=%s\n",
			formatDuration(st.confirmLatency.percentile(50)),
			formatDuration(st.confirmLatency.percentile(75)),
			formatDuration(st.confirmLatency.percentile(95)),
			formatDuration(st.confirmLatency.percentile(99)),
			formatDuration(st.confirmLatency.percentile(99.9)),
		)
	}
}

func formatDuration(d time.Duration) string {
	if d >= time.Second {
		return fmt.Sprintf("%.2fs", d.Seconds())
	}
	if d >= time.Millisecond {
		return fmt.Sprintf("%.2fms", float64(d)/float64(time.Millisecond))
	}
	return fmt.Sprintf("%.0fµs", float64(d)/float64(time.Microsecond))
}

func runProducer(ctx context.Context, url, queueName, exchange string, size int, durable, useConfirm bool, confirmGrace time.Duration, outstanding int, st *stats) {
	conn, err := amqp.Dial(url)
	if err != nil {
		log.Printf("producer: connect failed: %v", err)
		return
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Printf("producer: open channel failed: %v", err)
		return
	}
	defer ch.Close()

	if _, err = ch.QueueDeclare(queueName, durable, false, false, false, nil); err != nil {
		log.Printf("producer: queue declare failed: %v", err)
		return
	}

	if useConfirm {
		if err := ch.Confirm(false); err != nil {
			log.Printf("producer: confirm mode failed: %v", err)
			return
		}

		confirmCh := ch.NotifyPublish(make(chan amqp.Confirmation, 1024))
		// Map seqNo -> publish timestamp for confirm latency
		var pendingMu sync.Mutex
		pending := make(map[uint64]time.Time)

		// inFlight caps unconfirmed publishes so the benchmark measures confirm
		// throughput rather than how far a flood can outrun broker backpressure.
		var inFlight chan struct{}
		if outstanding > 0 {
			inFlight = make(chan struct{}, outstanding)
		}
		releaseInFlight := func() {
			if inFlight != nil {
				select {
				case <-inFlight:
				default:
				}
			}
		}

		go func() {
			for c := range confirmCh {
				now := time.Now()
				pendingMu.Lock()
				ts, ok := pending[c.DeliveryTag]
				delete(pending, c.DeliveryTag)
				pendingMu.Unlock()
				releaseInFlight()
				if !c.Ack {
					// A nack is a (negative) response, but it still fails the run.
					st.nacked.Add(1)
					continue
				}
				st.confirmed.Add(1)
				if ok {
					st.confirmLatency.record(now.Sub(ts))
				}
			}
		}()

		body := make([]byte, size)
		deliveryMode := amqp.Transient
		if durable {
			deliveryMode = amqp.Persistent
		}

		// drainConfirms waits (bounded by confirmGrace) for every outstanding
		// publish to be confirmed; whatever remains counts as never-confirmed
		// and fails the run.
		drainConfirms := func() {
			deadline := time.Now().Add(confirmGrace)
			for {
				pendingMu.Lock()
				remaining := len(pending)
				pendingMu.Unlock()
				if remaining == 0 {
					return
				}
				if time.Now().After(deadline) {
					log.Printf("producer: %d publish(es) never confirmed after %s grace", remaining, confirmGrace)
					st.unconfirmed.Add(int64(remaining))
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
		}

		for {
			select {
			case <-ctx.Done():
				drainConfirms()
				return
			default:
				if inFlight != nil {
					select {
					case inFlight <- struct{}{}:
					case <-ctx.Done():
						drainConfirms()
						return
					}
				}
				// Record publish time under lock before publishing so the
				// confirm goroutine never misses an entry.
				seqNo := ch.GetNextPublishSeqNo()
				publishTime := time.Now()
				pendingMu.Lock()
				pending[seqNo] = publishTime
				pendingMu.Unlock()

				if err := ch.PublishWithContext(ctx, exchange, queueName, false, false, amqp.Publishing{
					DeliveryMode: deliveryMode,
					ContentType:  "application/octet-stream",
					Body:         body,
					Timestamp:    publishTime,
				}); err != nil {
					// Remove the entry we pre-recorded since the publish failed.
					pendingMu.Lock()
					delete(pending, seqNo)
					pendingMu.Unlock()
					releaseInFlight()
					if ctx.Err() != nil {
						drainConfirms()
						return
					}
					log.Printf("producer: publish error: %v", err)
					time.Sleep(10 * time.Millisecond)
					continue
				}
				st.published.Add(1)
			}
		}
	}

	// No-confirm path (simpler, higher throughput)
	body := make([]byte, size)
	deliveryMode := amqp.Transient
	if durable {
		deliveryMode = amqp.Persistent
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
			if err := ch.PublishWithContext(ctx, exchange, queueName, false, false, amqp.Publishing{
				DeliveryMode: deliveryMode,
				ContentType:  "application/octet-stream",
				Body:         body,
				Timestamp:    time.Now(),
			}); err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("producer: publish error: %v", err)
				time.Sleep(10 * time.Millisecond)
				continue
			}
			st.published.Add(1)
		}
	}
}

func runConsumer(ctx context.Context, url, queueName string, prefetch int, durable bool, st *stats) {
	conn, err := amqp.Dial(url)
	if err != nil {
		log.Printf("consumer: connect failed: %v", err)
		return
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Printf("consumer: open channel failed: %v", err)
		return
	}
	defer ch.Close()

	if _, err = ch.QueueDeclare(queueName, durable, false, false, false, nil); err != nil {
		log.Printf("consumer: queue declare failed: %v", err)
		return
	}

	if err := ch.Qos(prefetch, 0, false); err != nil {
		log.Printf("consumer: qos failed: %v", err)
		return
	}

	msgs, err := ch.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		log.Printf("consumer: consume failed: %v", err)
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-msgs:
			if !ok {
				return
			}
			if !msg.Timestamp.IsZero() {
				st.consumerLatency.record(time.Since(msg.Timestamp))
			}
			st.consumed.Add(1)
			if err := msg.Ack(false); err != nil && ctx.Err() == nil {
				log.Printf("consumer: ack error: %v", err)
			}
		}
	}
}
