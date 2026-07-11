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

// benchConfig is the fully-resolved configuration for one benchmark run. main()
// builds it from flags; tests build it directly so they can drive runBenchmark
// against an embedded server without going through main()/os.Exit.
type benchConfig struct {
	url, queue, exchange                              string
	producers, consumers, size, prefetch, outstanding int
	duration, confirmGrace, drainTimeout              time.Duration
	durable, confirm                                  bool

	// consumerDelay throttles each consumer by sleeping after every ack. It is
	// a test-only pacing knob (0 in production, no CLI flag) used to force a
	// guaranteed backlog at the publish-window cutoff so the drain path is
	// exercised deterministically.
	consumerDelay time.Duration

	// maxMessages caps the aggregate number of messages the producers publish
	// (0 = unbounded, the production default; no CLI flag). It is a test-only
	// knob: capping the count makes `target` hardware-independent so a fast
	// machine cannot build a backlog larger than the bounded drain can clear
	// within drainTimeout, which would otherwise flake TestDrainRecoversBacklog.
	maxMessages int
}

// benchResult is the outcome of a run. Throughput fields are measured over the
// timed publish window only; completeness fields (target/delivered/lost) are
// measured after the drain phase. It carries everything printSummary needs, so
// printSummary stays a pure formatter.
type benchResult struct {
	windowElapsed time.Duration

	published        int64 // socket writes (PublishWithContext returned nil)
	confirmed        int64 // broker-acknowledged (confirm mode)
	consumedInWindow int64 // consumed at window close, for consumer throughput

	// Completeness, computed after the drain:
	//   target    = broker-accepted count (confirmed in confirm mode, else published)
	//   delivered = messages actually consumed by end of the bounded drain
	//   lost      = max(0, target-delivered): a genuine, un-drainable strand
	target    int64
	delivered int64
	lost      int64

	unconfirmed int64 // confirm mode: publishes never confirmed within grace
	nacked      int64 // confirm mode: basic.nack count
}

func main() {
	var (
		url          = flag.String("url", "amqp://guest:guest@localhost:5672/", "AMQP server URL")
		producers    = flag.Int("producers", 1, "Number of producer goroutines")
		consumers    = flag.Int("consumers", 1, "Number of consumer goroutines")
		duration     = flag.Duration("duration", 30*time.Second, "Test run duration")
		drainTimeout = flag.Duration("drain-timeout", 30*time.Second, "Max time to drain the consumer backlog after the publish window before counting the remainder as lost")
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

	// runCtx spans the whole run. A signal cancels it so a manual Ctrl-C aborts
	// the publish window AND any in-progress drain (drainToTarget bails on it),
	// rather than hanging for the full drain timeout against dead consumers.
	runCtx, runCancel := context.WithCancel(context.Background())
	defer runCancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		runCancel()
	}()

	cfg := benchConfig{
		url:          *url,
		queue:        *queue,
		exchange:     *exchange,
		producers:    *producers,
		consumers:    *consumers,
		size:         *size,
		prefetch:     *prefetch,
		outstanding:  *outstanding,
		duration:     *duration,
		confirmGrace: *confirmGrace,
		drainTimeout: *drainTimeout,
		durable:      *durable,
		confirm:      *confirm,
		// consumerDelay stays 0 and maxMessages stays 0: production runs never
		// throttle the consumer and never cap the publish count.
	}

	result := runBenchmark(runCtx, cfg, st)
	printSummary(result, *confirm, st)

	// Confirm mode is a correctness gate, not just a benchmark: a publish that
	// is never confirmed (or is nacked) fails the whole run.
	if *confirm {
		if result.unconfirmed > 0 || result.nacked > 0 {
			fmt.Printf("\nFAIL: %d publish(es) never confirmed, %d nacked\n", result.unconfirmed, result.nacked)
			os.Exit(1)
		}
	}
}

// runBenchmark spawns producers and consumers, runs the timed publish window,
// drains the in-flight backlog, and returns the measured result. It never
// prints or exits, so tests can drive it directly.
//
// The core of the iter7 fix lives here: producers and consumers have SEPARATE
// lifecycles. Producers stop at cfg.duration (the throughput window).
// Consumers keep running through a bounded drain phase so the backlog that was
// in flight at the cutoff (broker queue + amqp091-go client prefetch buffers)
// is consumed and counted, instead of being frozen and miscounted as loss.
func runBenchmark(runCtx context.Context, cfg benchConfig, st *stats) benchResult {
	// pubCtx bounds the producers to the throughput window.
	pubCtx, pubCancel := context.WithTimeout(runCtx, cfg.duration)
	defer pubCancel()
	// consumeCtx keeps consumers alive; it is cancelled only AFTER the drain.
	consumeCtx, consumeCancel := context.WithCancel(runCtx)
	defer consumeCancel()

	startTime := time.Now()

	var producerWg, consumerWg sync.WaitGroup

	for i := 0; i < cfg.producers; i++ {
		producerWg.Add(1)
		go func() {
			defer producerWg.Done()
			runProducer(pubCtx, cfg.url, cfg.queue, cfg.exchange, cfg.size, cfg.durable, cfg.confirm, cfg.confirmGrace, cfg.outstanding, cfg.maxMessages, st)
		}()
	}

	for i := 0; i < cfg.consumers; i++ {
		consumerWg.Add(1)
		go func() {
			defer consumerWg.Done()
			runConsumer(consumeCtx, cfg.url, cfg.queue, cfg.prefetch, cfg.durable, cfg.consumerDelay, st)
		}()
	}

	// Per-second printer runs across the window and the drain phase, stopping
	// when consumeCtx is cancelled below.
	go printPerSecondStats(consumeCtx, st)

	// 1. Producers finish the throughput window. In confirm mode drainConfirms
	//    has run inside runProducer, so confirmed/unconfirmed/nacked are final.
	producerWg.Wait()
	windowElapsed := time.Since(startTime)

	// 2. Snapshot consumer throughput BEFORE the (idle) drain phase, so the
	//    round-trip rate is measured over the window only and not diluted.
	consumedInWindow := st.consumed.Load()

	// 3. Completeness target = what the broker accepted. In confirm mode that is
	//    the confirmed count (broker-acknowledged), NOT published: published
	//    counts socket writes, which can include not-yet-confirmed or nacked
	//    publishes in flight, so subtracting from it would report those as loss
	//    even after a perfect drain.
	published := st.published.Load()
	confirmed := st.confirmed.Load()
	target := published
	if cfg.confirm {
		target = confirmed
	}

	// 4. Drain the in-flight backlog, bounded by drainTimeout.
	delivered := drainToTarget(runCtx, target, cfg.drainTimeout, st)

	// 5. Drain is done (or timed out); stop the consumers.
	consumeCancel()
	consumerWg.Wait()

	lost := target - delivered
	if lost < 0 {
		lost = 0
	}

	return benchResult{
		windowElapsed:    windowElapsed,
		published:        published,
		confirmed:        confirmed,
		consumedInWindow: consumedInWindow,
		target:           target,
		delivered:        delivered,
		lost:             lost,
		unconfirmed:      st.unconfirmed.Load(),
		nacked:           st.nacked.Load(),
	}
}

// drainToTarget polls until the consumer has consumed `target` messages, the
// `timeout` deadline passes, or runCtx is cancelled (manual abort). Whatever is
// still un-consumed once it returns is genuine loss, so a real strand is never
// masked and the drain never blocks indefinitely.
func drainToTarget(runCtx context.Context, target int64, timeout time.Duration, st *stats) int64 {
	deadline := time.Now().Add(timeout)
	for st.consumed.Load() < target && time.Now().Before(deadline) {
		if runCtx.Err() != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	return st.consumed.Load()
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

// printSummary is a pure formatter fed from benchResult (counts) plus st (the
// latency histograms). Throughput is reported over the timed publish window;
// completeness (delivered/lost) is reported after the bounded drain.
func printSummary(r benchResult, withConfirm bool, st *stats) {
	secs := r.windowElapsed.Seconds()
	lostPct := 0.0
	if r.target > 0 {
		lostPct = float64(r.lost) / float64(r.target) * 100
	}

	fmt.Printf("\n=== SUMMARY (%.0fs) ===\n", secs)
	fmt.Printf("Published : %d total  |  %.0f avg/s\n", r.published, float64(r.published)/secs)
	fmt.Printf("Consumed  : %d in window  |  %.0f avg/s\n", r.consumedInWindow, float64(r.consumedInWindow)/secs)
	fmt.Printf("Delivered : %d total  (after drain)\n", r.delivered)
	fmt.Printf("Lost      : %d (%.2f%%)\n", r.lost, lostPct)
	if withConfirm {
		fmt.Printf("Confirmed : %d total  |  %.0f avg/s  (unconfirmed: %d, nacked: %d)\n",
			r.confirmed, float64(r.confirmed)/secs, r.unconfirmed, r.nacked)
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

func runProducer(ctx context.Context, url, queueName, exchange string, size int, durable, useConfirm bool, confirmGrace time.Duration, outstanding, maxMessages int, st *stats) {
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
				// Test-only cap: once the aggregate published count reaches the
				// bound, stop publishing but still drain confirms exactly like a
				// ctx-cancel, so confirmed/unconfirmed/nacked stay correct.
				if maxMessages > 0 && st.published.Load() >= int64(maxMessages) {
					drainConfirms()
					return
				}
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
			// Test-only cap (see confirm path); no-confirm has nothing to drain.
			if maxMessages > 0 && st.published.Load() >= int64(maxMessages) {
				return
			}
		}
	}
}

func runConsumer(ctx context.Context, url, queueName string, prefetch int, durable bool, consumerDelay time.Duration, st *stats) {
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
			// Test-only pacing knob (0 in production): throttling the consumer
			// forces a deterministic backlog so the drain path is exercised.
			if consumerDelay > 0 {
				time.Sleep(consumerDelay)
			}
		}
	}
}
