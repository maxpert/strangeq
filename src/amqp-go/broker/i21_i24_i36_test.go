package broker

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRedeliveredFlagOnNackRequeue(t *testing.T) {
	runWithTimeout(t, 5*time.Second, func() {
		qs := NewQueueState(0)
		defer qs.Close()
		stop, cancel := makeStop()
		defer cancel()

		qs.Publish(0)
		tag, _, ok := qs.Claim(stop, testTimer(qs))
		if !ok || tag != 0 {
			t.Fatalf("initial claim = %d (ok=%v), want 0", tag, ok)
		}
		qs.ClaimInflight(tag)
		qs.Requeue(tag)

		rtag, redelivered, ok := qs.Claim(stop, testTimer(qs))
		if !ok || rtag != tag {
			t.Fatalf("requeue claim = %d (ok=%v), want %d", rtag, ok, tag)
		}
		if !redelivered {
			t.Error("redelivered flag should be true after requeue")
		}
	})
}

func TestRedeliveredFlagOnRejectRequeue(t *testing.T) {
	runWithTimeout(t, 5*time.Second, func() {
		qs := NewQueueState(0)
		defer qs.Close()
		stop, cancel := makeStop()
		defer cancel()

		qs.Publish(0)
		tag, redelivered, ok := qs.Claim(stop, testTimer(qs))
		if !ok || tag != 0 {
			t.Fatalf("initial claim = %d (ok=%v), want 0", tag, ok)
		}
		if redelivered {
			t.Error("redelivered should be false on first delivery")
		}
		qs.ClaimInflight(tag)
		qs.Requeue(tag)

		rtag, rredelivered, rok := qs.Claim(stop, testTimer(qs))
		if !rok || rtag != tag {
			t.Fatalf("requeue claim = %d (ok=%v), want %d", rtag, rok, tag)
		}
		if !rredelivered {
			t.Error("redelivered should be true after requeue")
		}
	})
}

func TestRedeliveredFlagOnRecover(t *testing.T) {
	runWithTimeout(t, 5*time.Second, func() {
		qs := NewQueueState(0)
		defer qs.Close()
		stop, cancel := makeStop()
		defer cancel()

		qs.Recover(100, 102, 3)

		for want := uint64(100); want <= 102; want++ {
			tag, redelivered, ok := qs.Claim(stop, testTimer(qs))
			if !ok {
				t.Fatalf("Claim %d returned !ok after Recover", want)
			}
			if tag != want {
				t.Errorf("recovered claim = %d, want %d", tag, want)
			}
			if redelivered {
				t.Errorf("recovered tag %d should have redelivered=false", tag)
			}
		}
	})
}

func TestRedeliveredFlagOnGetAfterRequeue(t *testing.T) {
	runWithTimeout(t, 5*time.Second, func() {
		qs := NewQueueState(0)
		defer qs.Close()

		qs.Publish(0)
		qs.ClaimInflight(0)
		qs.Requeue(0)

		tag, redelivered, ok := qs.tryPopRequeue()
		if !ok || tag != 0 {
			t.Fatalf("tryPopRequeue = %d (ok=%v), want 0", tag, ok)
		}
		if !redelivered {
			t.Error("redelivered should be true after requeue")
		}
	})
}

func TestDispatch_SparseDepthNotInflated(t *testing.T) {
	runWithTimeout(t, 5*time.Second, func() {
		qs := NewQueueState(0)
		defer qs.Close()
		stop, cancel := makeStop()
		defer cancel()

		tags := []uint64{0, 2, 4, 6, 8}
		for _, tag := range tags {
			qs.Publish(tag)
		}

		if d := qs.Depth(); d != 5 {
			t.Fatalf("Depth = %d, want 5 (not 9)", d)
		}

		claimed := make(map[uint64]bool)
		for i := 0; i < len(tags); i++ {
			tag, _, ok := qs.Claim(stop, testTimer(qs))
			if !ok {
				t.Fatalf("claim %d failed", i)
			}
			claimed[tag] = true
			qs.ClaimInflight(tag)
		}

		if d := qs.Depth(); d != 5 {
			t.Errorf("Depth after all claims = %d, want 5", d)
		}

		for tag := range claimed {
			qs.AckAdvance(tag)
		}

		if d := qs.Depth(); d != 0 {
			t.Errorf("Depth after all acks = %d, want 0", d)
		}
	})
}

func TestDispatch_DepthInvariantConcurrent(t *testing.T) {
	runWithTimeout(t, 15*time.Second, func() {
		qs := NewQueueState(1024)
		defer qs.Close()
		stop, cancel := makeStop()
		defer cancel()

		const total = 2000
		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := uint64(0); i < total; i++ {
				if !qs.WaitForCapacity(stop) {
					return
				}
				qs.Publish(i)
			}
		}()

		workers := 4
		wg.Add(workers)
		var done atomic.Int64
		for w := 0; w < workers; w++ {
			go func() {
				defer wg.Done()
				for {
					if done.Add(1) > total {
						return
					}
					tag, _, ok := qs.Claim(stop, testTimer(qs))
					if !ok {
						return
					}
					qs.ClaimInflight(tag)
					if tag%3 == 0 {
						qs.Requeue(tag)
						qs.ClaimInflight(tag)
						qs.AckAdvance(tag)
					} else {
						qs.AckAdvance(tag)
					}
				}
			}()
		}

		go func() {
			for {
				if done.Load() > total && qs.Depth() == 0 {
					cancel()
					qs.WakeAll()
					return
				}
				time.Sleep(time.Millisecond)
			}
		}()

		wg.Wait()
	})
}

func TestDispatch_RequeueRingReuse(t *testing.T) {
	runWithTimeout(t, 10*time.Second, func() {
		qs := NewQueueState(0)
		defer qs.Close()
		stop, cancel := makeStop()
		defer cancel()

		const N = 5000
		for i := uint64(0); i < N; i++ {
			qs.Publish(i)
		}
		tags := make([]uint64, N)
		for i := uint64(0); i < N; i++ {
			tag, _, ok := qs.Claim(stop, testTimer(qs))
			if !ok {
				t.Fatalf("claim %d failed", i)
			}
			if tag != i {
				t.Fatalf("claim %d got tag %d", i, tag)
			}
			tags[i] = tag
			qs.ClaimInflight(tag)
		}
		for i := uint64(0); i < N; i++ {
			qs.Requeue(tags[i])
		}

		for i := uint64(0); i < N; i++ {
			tag, redelivered, ok := qs.Claim(stop, testTimer(qs))
			if !ok {
				t.Fatalf("drain claim %d failed", i)
			}
			if tag != i {
				t.Errorf("FIFO broken: got tag %d, want %d", tag, i)
			}
			if !redelivered {
				t.Errorf("tag %d should have redelivered=true", tag)
			}
			qs.ClaimInflight(tag)
			qs.AckAdvance(tag)
		}

		if qs.RequeueDepth() != 0 {
			t.Errorf("RequeueDepth after drain = %d, want 0", qs.RequeueDepth())
		}
	})
}

func TestDispatch_RequeueRingBoundedAfterBurstDrain(t *testing.T) {
	runWithTimeout(t, 10*time.Second, func() {
		qs := NewQueueState(0)
		defer qs.Close()
		stop, cancel := makeStop()
		defer cancel()

		const burst = 20000
		for i := uint64(0); i < burst; i++ {
			qs.Publish(i)
		}
		for i := uint64(0); i < burst; i++ {
			tag, _, ok := qs.Claim(stop, testTimer(qs))
			if !ok {
				t.Fatalf("claim %d failed", i)
			}
			qs.ClaimInflight(tag)
			qs.Requeue(tag)
		}

		for i := uint64(0); i < burst; i++ {
			_, _, ok := qs.Claim(stop, testTimer(qs))
			if !ok {
				t.Fatalf("drain claim %d failed", i)
			}
		}

		qs.requeueMu.Lock()
		bufCap := cap(qs.requeueBuf)
		qs.requeueMu.Unlock()

		if bufCap > requeueInitialCap*2 {
			t.Errorf("buffer cap after drain = %d, should shrink toward %d", bufCap, requeueInitialCap)
		}
	})
}
