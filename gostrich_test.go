package gostrich

import (
	"testing"
	"time"
)

func TestQpsTracker(t *testing.T) {
	tracker := NewQpsTracker(10*time.Millisecond)
	done := make(chan int)
	go func() {
		// in the first 40ms, send 1 tick per ms
		for i := 0; i < 40; i += 1 {
			tracker.Record()
			time.Sleep(time.Millisecond)
		}
		// then in the next 40ms, send 4 ticks per ms
		for i := 0; i < 40; i += 1 {
			tracker.Record()
			tracker.Record()
			tracker.Record()
			tracker.Record()
			time.Sleep(time.Millisecond)
	    }
		done <- 1
	} ()
	// wait for 11ms to by pass cold start
	time.Sleep(11 * time.Millisecond)
	// for the next 30ms, qps should be at about 10qps
	for i := 0; i < 30; i += 1 {
		qps := tracker.Ticks()
		if qps > 20 || qps < 5 {
			t.Errorf("Well, qps is %v, doesn't seem acurate", qps)
		} else {
			t.Logf("qps seems ok at: %v\n", qps)
		}
		time.Sleep(time.Millisecond)
	}
	// wait till qps ramp up to 4 per ms and wait one extra cycle
	time.Sleep(11 * time.Millisecond)
	for i := 0; i < 30; i += 1 {
		qps := tracker.Ticks()
		if qps > 50 || qps < 30 {
			t.Errorf("Well, qps is %v, doesn't seem acurate", qps)
		} else {
			t.Logf("qps seems ok at: %v\n", qps)
		}
		time.Sleep(time.Millisecond)
	}

	<-done
}
