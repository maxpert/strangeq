package broker

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func makeStop() (chan struct{}, func()) {
	stop := make(chan struct{})
	return stop, sync.OnceFunc(func() { close(stop) })
}

func runWithTimeout(t *testing.T, timeout time.Duration, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		fn()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatalf("test timed out after %v", timeout)
	}
}

func TestDispatch_FIFOClaimOrder(t *testing.T) {
	runWithTimeout(t, 5*time.Second, func() {
		qs := NewQueueState(0)
		defer qs.Close()
		stop, cancel := makeStop()
		defer cancel()

		for i := uint64(0); i < 5; i++ {
			qs.Publish(i)
		}
		for want := uint64(0); want < 5; want++ {
			tag, ok := qs.Claim(stop)
			if !ok {
				t.Fatalf("Claim %d returned !ok", want)
			}
			if tag != want {
				t.Errorf("FIFO broken: got tag %d, want %d", tag, want)
			}
		}
	})
}

func TestDispatch_CompetingConsumersDistinct(t *testing.T) {
	runWithTimeout(t, 10*time.Second, func() {
		qs := NewQueueState(0)
		defer qs.Close()
		stop, cancel := makeStop()
		defer cancel()

		const N = 1000
		for i := uint64(0); i < N; i++ {
			qs.Publish(i)
		}

		const workers = 8
		var seen [1000]atomic.Bool
		var wg sync.WaitGroup
		wg.Add(workers)
		claims := int64(N)

		for w := 0; w < workers; w++ {
			go func() {
				defer wg.Done()
				for {
					if atomic.LoadInt64(&claims) <= 0 {
						return
					}
					tag, ok := qs.Claim(stop)
					if !ok {
						return
					}
					if tag >= N {
						t.Errorf("tag %d out of range", tag)
						return
					}
					if !seen[tag].CompareAndSwap(false, true) {
						t.Errorf("duplicate claim of tag %d", tag)
					}
					atomic.AddInt64(&claims, -1)
				}
			}()
		}

		for atomic.LoadInt64(&claims) > 0 {
			time.Sleep(time.Millisecond)
		}
		cancel()
		qs.WakeAll()
		wg.Wait()

		for i := uint64(0); i < N; i++ {
			if !seen[i].Load() {
				t.Errorf("tag %d was never claimed", i)
			}
		}
	})
}

func TestDispatch_ParkWake(t *testing.T) {
	runWithTimeout(t, 5*time.Second, func() {
		qs := NewQueueState(0)
		defer qs.Close()
		stop, cancel := makeStop()
		defer cancel()

		gotTag := make(chan uint64, 1)
		go func() {
			tag, ok := qs.Claim(stop)
			if !ok {
				gotTag <- ^uint64(0)
				return
			}
			gotTag <- tag
		}()

		select {
		case v := <-gotTag:
			t.Fatalf("Claim returned %d before any Publish", v)
		case <-time.After(50 * time.Millisecond):
		}

		qs.Publish(0)

		select {
		case v := <-gotTag:
			if v != 0 {
				t.Errorf("got tag %d, want 0", v)
			}
		case <-time.After(time.Second):
			t.Fatal("consumer not woken within 1s")
		}
	})
}

func TestDispatch_ParkWakeMultiple(t *testing.T) {
	runWithTimeout(t, 5*time.Second, func() {
		qs := NewQueueState(0)
		defer qs.Close()
		stop, cancel := makeStop()
		defer cancel()

		const consumers = 4
		got := make(chan uint64, consumers)
		for i := 0; i < consumers; i++ {
			go func() {
				tag, ok := qs.Claim(stop)
				if !ok {
					got <- ^uint64(0)
					return
				}
				got <- tag
			}()
		}

		<-time.After(50 * time.Millisecond)
		for i := uint64(0); i < consumers; i++ {
			qs.Publish(i)
		}

		seen := make(map[uint64]bool)
		for i := 0; i < consumers; i++ {
			select {
			case v := <-got:
				seen[v] = true
			case <-time.After(time.Second):
				t.Fatal("not all consumers woken within 1s")
			}
		}
		if len(seen) != consumers {
			t.Errorf("got %d distinct tags, want %d", len(seen), consumers)
		}
	})
}

func TestDispatch_ClaimStopPrecheck(t *testing.T) {
	runWithTimeout(t, 5*time.Second, func() {
		qs := NewQueueState(0)
		defer qs.Close()
		stop, cancel := makeStop()
		cancel()

		done := make(chan struct{})
		go func() {
			if _, ok := qs.Claim(stop); ok {
				t.Error("expected !ok when stop already closed")
			}
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("Claim did not return immediately when stop was already closed")
		}
	})
}

func TestDispatch_WakeAllUnblocksParked(t *testing.T) {
	runWithTimeout(t, 5*time.Second, func() {
		qs := NewQueueState(0)
		defer qs.Close()
		stop, cancel := makeStop()
		defer cancel()

		done := make(chan struct{})
		go func() {
			if _, ok := qs.Claim(stop); ok {
				t.Error("expected !ok after WakeAll+stopped")
			}
			close(done)
		}()

		<-time.After(50 * time.Millisecond)
		cancel()
		qs.WakeAll()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("WakeAll did not unblock parked+stopped consumer")
		}
	})
}

func TestDispatch_RequeuePreservesTagAndFIFO(t *testing.T) {
	runWithTimeout(t, 5*time.Second, func() {
		qs := NewQueueState(0)
		defer qs.Close()
		stop, cancel := makeStop()
		defer cancel()

		for i := uint64(0); i < 3; i++ {
			qs.Publish(i)
		}
		t0, _ := qs.Claim(stop)
		t1, _ := qs.Claim(stop)
		t2, _ := qs.Claim(stop)
		if t0 != 0 || t1 != 1 || t2 != 2 {
			t.Fatalf("initial claims %d,%d,%d want 0,1,2", t0, t1, t2)
		}

		qs.Requeue(1)
		qs.Requeue(2)

		r1, ok := qs.Claim(stop)
		if !ok || r1 != 1 {
			t.Errorf("first requeue claim = %d (ok=%v), want 1", r1, ok)
		}
		r2, ok := qs.Claim(stop)
		if !ok || r2 != 2 {
			t.Errorf("second requeue claim = %d (ok=%v), want 2", r2, ok)
		}

		qs.Publish(3)
		r3, ok := qs.Claim(stop)
		if !ok || r3 != 3 {
			t.Errorf("fresh claim after requeue = %d (ok=%v), want 3", r3, ok)
		}
	})
}

func TestDispatch_RequeueWakesParked(t *testing.T) {
	runWithTimeout(t, 5*time.Second, func() {
		qs := NewQueueState(0)
		defer qs.Close()
		stop, cancel := makeStop()
		defer cancel()

		for i := uint64(0); i <= 7; i++ {
			qs.Publish(i)
		}
		for i := uint64(0); i < 7; i++ {
			if _, ok := qs.Claim(stop); !ok {
				t.Fatalf("warmup claim %d failed", i)
			}
		}
		tag, ok := qs.Claim(stop)
		if !ok || tag != 7 {
			t.Fatalf("claim = %d (ok=%v), want 7", tag, ok)
		}
		qs.ClaimInflight(tag)

		got := make(chan uint64, 1)
		go func() {
			t2, _ := qs.Claim(stop)
			got <- t2
		}()

		<-time.After(50 * time.Millisecond)
		qs.Requeue(7)
		select {
		case v := <-got:
			if v != 7 {
				t.Errorf("redelivered tag %d, want 7", v)
			}
		case <-time.After(time.Second):
			t.Fatal("Requeue did not wake parked consumer")
		}
	})
}

func TestDispatch_RecoverRange(t *testing.T) {
	runWithTimeout(t, 5*time.Second, func() {
		qs := NewQueueState(0)
		defer qs.Close()
		stop, cancel := makeStop()
		defer cancel()

		qs.Recover(100, 102)

		for want := uint64(100); want <= 102; want++ {
			tag, ok := qs.Claim(stop)
			if !ok {
				t.Fatalf("Claim %d returned !ok after Recover", want)
			}
			if tag != want {
				t.Errorf("recovered claim = %d, want %d", tag, want)
			}
		}
	})
}

func TestDispatch_RecoverThenPublish(t *testing.T) {
	runWithTimeout(t, 5*time.Second, func() {
		qs := NewQueueState(0)
		defer qs.Close()
		stop, cancel := makeStop()
		defer cancel()

		qs.Recover(10, 10)
		t10, _ := qs.Claim(stop)
		if t10 != 10 {
			t.Fatalf("recovered claim = %d, want 10", t10)
		}

		got := make(chan uint64, 1)
		go func() {
			t2, _ := qs.Claim(stop)
			got <- t2
		}()
		<-time.After(50 * time.Millisecond)
		qs.Publish(11)
		select {
		case v := <-got:
			if v != 11 {
				t.Errorf("post-recover publish claim = %d, want 11", v)
			}
		case <-time.After(time.Second):
			t.Fatal("not woken after Recover-then-Publish")
		}
	})
}

func TestDispatch_BackpressureBlocksPublisher(t *testing.T) {
	runWithTimeout(t, 5*time.Second, func() {
		qs := NewQueueState(2)
		defer qs.Close()
		stop, cancel := makeStop()
		defer cancel()

		qs.Publish(0)
		qs.Publish(1)
		t0, _ := qs.Claim(stop)
		t1, _ := qs.Claim(stop)
		qs.ClaimInflight(t0)
		qs.ClaimInflight(t1)

		blocked := make(chan struct{})
		go func() {
			qs.WaitForCapacity(stop)
			close(blocked)
		}()
		select {
		case <-blocked:
			t.Fatal("WaitForCapacity returned before any ack")
		case <-time.After(50 * time.Millisecond):
		}

		qs.AckAdvance(t0)
		select {
		case <-blocked:
		case <-time.After(time.Second):
			t.Fatal("WaitForCapacity not released within 1s of AckAdvance")
		}
	})
}

func TestDispatch_BackpressureStopExits(t *testing.T) {
	runWithTimeout(t, 5*time.Second, func() {
		qs := NewQueueState(1)
		stop := qs.StopCh()

		qs.Publish(0)
		t0, _ := qs.Claim(stop)
		qs.ClaimInflight(t0)

		done := make(chan bool, 1)
		go func() {
			done <- qs.WaitForCapacity(stop)
		}()
		<-time.After(50 * time.Millisecond)
		qs.Close()
		defer qs.Close()
		select {
		case ok := <-done:
			if ok {
				t.Error("WaitForCapacity returned true after Close")
			}
		case <-time.After(time.Second):
			t.Fatal("blocked publisher not released by Close")
		}
	})
}

func TestDispatch_DepthAccounting(t *testing.T) {
	runWithTimeout(t, 5*time.Second, func() {
		qs := NewQueueState(0)
		defer qs.Close()
		stop, cancel := makeStop()
		defer cancel()

		for i := uint64(0); i < 5; i++ {
			qs.Publish(i)
		}
		if d := qs.Depth(); d != 5 {
			t.Fatalf("Depth after publish 5 = %d, want 5", d)
		}

		for i := uint64(0); i < 5; i++ {
			tag, _ := qs.Claim(stop)
			qs.ClaimInflight(tag)
		}

		qs.AckAdvance(3)
		if d := qs.Depth(); d > 5 {
			t.Errorf("Depth after ack3 = %d, should not exceed 5", d)
		}

		qs.AckAdvance(0)
		if d := qs.Depth(); d != 4 && d != 5 {
			t.Errorf("Depth after ack0 = %d, want 4 or 5", d)
		}

		qs.AckAdvance(1)
		qs.AckAdvance(2)
		qs.AckAdvance(4)
		if d := qs.Depth(); d != 0 {
			t.Errorf("Depth after all acks = %d, want 0", d)
		}
	})
}

func TestDispatch_DepthWithRequeue(t *testing.T) {
	runWithTimeout(t, 5*time.Second, func() {
		qs := NewQueueState(0)
		defer qs.Close()
		stop, cancel := makeStop()
		defer cancel()

		qs.Publish(0)
		tag, _ := qs.Claim(stop)
		qs.ClaimInflight(tag)
		if d := qs.Depth(); d != 1 {
			t.Fatalf("Depth = %d, want 1", d)
		}

		qs.Requeue(tag)
		if d := qs.Depth(); d != 1 {
			t.Errorf("Depth after requeue = %d, want 1", d)
		}
		if qs.InflightCount() != 0 {
			t.Errorf("InflightCount after requeue = %d, want 0", qs.InflightCount())
		}
		if qs.RequeueDepth() != 1 {
			t.Errorf("RequeueDepth = %d, want 1", qs.RequeueDepth())
		}

		tag2, _ := qs.Claim(stop)
		if tag2 != tag {
			t.Errorf("redelivered tag %d, want %d", tag2, tag)
		}
		qs.ClaimInflight(tag2)
		qs.AckAdvance(tag2)
		if d := qs.Depth(); d != 0 {
			t.Errorf("Depth after final ack = %d, want 0", d)
		}
	})
}

func TestDispatch_CloseExits(t *testing.T) {
	runWithTimeout(t, 5*time.Second, func() {
		qs := NewQueueState(0)
		stop, cancel := makeStop()
		defer cancel()

		consDone := make(chan struct{})
		go func() {
			if _, ok := qs.Claim(stop); ok {
				t.Error("Claim returned ok after Close")
			}
			close(consDone)
		}()
		<-time.After(50 * time.Millisecond)
		qs.Close()
		select {
		case <-consDone:
		case <-time.After(time.Second):
			t.Fatal("parked consumer not released by Close")
		}
	})
}

func TestDispatch_DuplicateWakesCoalesce(t *testing.T) {
	runWithTimeout(t, 5*time.Second, func() {
		qs := NewQueueState(0)
		defer qs.Close()
		stop, cancel := makeStop()
		defer cancel()

		got := make(chan uint64, 1)
		go func() {
			tag, _ := qs.Claim(stop)
			got <- tag
		}()

		<-time.After(50 * time.Millisecond)
		for i := 0; i < 100; i++ {
			qs.NotifyNewMessage()
		}
		<-time.After(50 * time.Millisecond)
		if qs.ParkedCount() != 1 {
			t.Errorf("ParkedCount = %d, want 1", qs.ParkedCount())
		}
		qs.Publish(0)
		select {
		case v := <-got:
			if v != 0 {
				t.Errorf("got %d, want 0", v)
			}
		case <-time.After(time.Second):
			t.Fatal("real publish after duplicate wakes did not deliver")
		}
	})
}

func TestDispatch_ConcurrentPublishClaimRequeue(t *testing.T) {
	runWithTimeout(t, 30*time.Second, func() {
		qs := NewQueueState(256)
		defer qs.Close()
		stop, cancel := makeStop()
		defer cancel()

		total := 10000
		var published atomic.Int64
		var claimed atomic.Int64
		var acked atomic.Int64
		var decisions atomic.Int64

		var pubWG sync.WaitGroup
		pubWG.Add(1)
		go func() {
			defer pubWG.Done()
			for i := uint64(0); i < uint64(total); i++ {
				if !qs.WaitForCapacity(stop) {
					return
				}
				qs.Publish(i)
				published.Add(1)
			}
		}()

		var consWG sync.WaitGroup
		workers := 4
		consWG.Add(workers)
		for w := 0; w < workers; w++ {
			go func() {
				defer consWG.Done()
				for {
					tag, ok := qs.Claim(stop)
					if !ok {
						return
					}
					qs.ClaimInflight(tag)
					claimed.Add(1)
					if decisions.Add(1)%20 == 7 {
						qs.Requeue(tag)
					} else {
						qs.AckAdvance(tag)
						acked.Add(1)
					}
				}
			}()
		}

		go func() {
			for {
				if published.Load() >= int64(total) &&
					qs.Depth() == 0 &&
					qs.RequeueDepth() == 0 &&
					qs.InflightCount() == 0 {
					cancel()
					qs.WakeAll()
					return
				}
				time.Sleep(time.Millisecond)
			}
		}()

		pubWG.Wait()
		consWG.Wait()

		if d := qs.Depth(); d != 0 {
			t.Errorf("final Depth = %d, want 0", d)
		}
		if qs.RequeueDepth() != 0 {
			t.Errorf("final RequeueDepth = %d, want 0", qs.RequeueDepth())
		}
	})
}

func TestDispatch_ConcurrentPublishClaimRequeue_Race(t *testing.T) {
	runWithTimeout(t, 30*time.Second, func() {
		qs := NewQueueState(256)
		defer qs.Close()
		stop, cancel := makeStop()
		defer cancel()

		total := 2000
		var published atomic.Int64
		var claimed atomic.Int64
		var acked atomic.Int64
		var decisions atomic.Int64

		var pubWG sync.WaitGroup
		pubWG.Add(1)
		go func() {
			defer pubWG.Done()
			for i := uint64(0); i < uint64(total); i++ {
				if !qs.WaitForCapacity(stop) {
					return
				}
				qs.Publish(i)
				published.Add(1)
			}
		}()

		var consWG sync.WaitGroup
		workers := 4
		consWG.Add(workers)
		for w := 0; w < workers; w++ {
			go func() {
				defer consWG.Done()
				for {
					tag, ok := qs.Claim(stop)
					if !ok {
						return
					}
					qs.ClaimInflight(tag)
					claimed.Add(1)
					if decisions.Add(1)%20 == 7 {
						qs.Requeue(tag)
					} else {
						qs.AckAdvance(tag)
						acked.Add(1)
					}
				}
			}()
		}

		go func() {
			for {
				if published.Load() >= int64(total) &&
					qs.Depth() == 0 &&
					qs.RequeueDepth() == 0 &&
					qs.InflightCount() == 0 {
					cancel()
					qs.WakeAll()
					return
				}
				time.Sleep(time.Millisecond)
			}
		}()

		pubWG.Wait()
		consWG.Wait()

		if d := qs.Depth(); d != 0 {
			t.Errorf("final Depth = %d, want 0", d)
		}
	})
}

func TestDispatch_RequeueUnbounded(t *testing.T) {
	runWithTimeout(t, 10*time.Second, func() {
		qs := NewQueueState(0)
		defer qs.Close()
		stop, cancel := makeStop()
		defer cancel()

		const N = 4096 + 1000
		for i := uint64(0); i < N; i++ {
			qs.Publish(i)
		}
		tags := make([]uint64, N)
		for i := uint64(0); i < N; i++ {
			tag, ok := qs.Claim(stop)
			if !ok {
				t.Fatalf("claim %d failed", i)
			}
			tags[i] = tag
			qs.ClaimInflight(tag)
		}
		for i := uint64(0); i < N; i++ {
			qs.Requeue(tags[i])
		}
		if qs.RequeueDepth() != int64(N) {
			t.Fatalf("RequeueDepth = %d, want %d", qs.RequeueDepth(), N)
		}

		seen := make(map[uint64]bool, N)
		for i := uint64(0); i < N; i++ {
			tag, ok := qs.Claim(stop)
			if !ok {
				t.Fatalf("drain claim %d failed", i)
			}
			if seen[tag] {
				t.Errorf("duplicate redelivery of tag %d", tag)
			}
			seen[tag] = true
			qs.ClaimInflight(tag)
			qs.AckAdvance(tag)
		}
		if len(seen) != N {
			t.Errorf("redelivered %d distinct tags, want %d", len(seen), N)
		}
		if d := qs.Depth(); d != 0 {
			t.Errorf("final Depth = %d, want 0", d)
		}
	})
}

func TestDispatch_NoAllocOnClaim(t *testing.T) {
	runWithTimeout(t, 5*time.Second, func() {
		qs := NewQueueState(0)
		defer qs.Close()
		stop, cancel := makeStop()
		defer cancel()

		const pre = 4000
		for i := uint64(0); i < pre; i++ {
			qs.Publish(i)
		}
		for i := 0; i < 500; i++ {
			if _, ok := qs.Claim(stop); !ok {
				t.Fatal("warmup claim parked")
			}
		}

		allocs := testing.AllocsPerRun(100, func() {
			if _, ok := qs.Claim(stop); !ok {
				t.Fatal("measured claim parked")
			}
		})
		if allocs > 0 {
			t.Errorf("Claim fast path allocated %.1f times, want 0", allocs)
		}
	})
}

func TestDispatch_CloseDuringPublish(t *testing.T) {
	runWithTimeout(t, 5*time.Second, func() {
		qs := NewQueueState(0)

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := uint64(0); i < 100000; i++ {
				qs.Publish(i)
			}
		}()

		time.Sleep(10 * time.Millisecond)
		qs.Close()
		wg.Wait()
	})
}

func TestDispatch_CloseDuringClaim(t *testing.T) {
	runWithTimeout(t, 5*time.Second, func() {
		qs := NewQueueState(0)
		stop, cancel := makeStop()
		defer cancel()

		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					tag, ok := qs.Claim(stop)
					if !ok {
						return
					}
					_ = tag
				}
			}()
		}

		for i := uint64(0); i < 100; i++ {
			qs.Publish(i)
		}
		time.Sleep(10 * time.Millisecond)
		qs.Close()
		qs.WakeAll()
		wg.Wait()
	})
}

func TestDispatch_AckAdvanceWakesPublisher(t *testing.T) {
	runWithTimeout(t, 5*time.Second, func() {
		qs := NewQueueState(1)
		defer qs.Close()
		stop, cancel := makeStop()
		defer cancel()

		qs.Publish(0)
		t0, _ := qs.Claim(stop)
		qs.ClaimInflight(t0)

		unblocked := make(chan struct{})
		go func() {
			qs.WaitForCapacity(stop)
			close(unblocked)
		}()

		<-time.After(50 * time.Millisecond)
		qs.AckAdvance(t0)

		select {
		case <-unblocked:
		case <-time.After(time.Second):
			t.Fatal("AckAdvance did not wake blocked publisher")
		}
	})
}
