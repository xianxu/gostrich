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
			tracker.Record(false)  // all success
			time.Sleep(time.Millisecond)
		}
		// then in the next 40ms, send 4 ticks per ms
		for i := 0; i < 40; i += 1 {
			// 50% error rate
			tracker.Record(false)
			tracker.Record(true)
			tracker.Record(false)
			tracker.Record(true)
			time.Sleep(time.Millisecond)
	    }
		done <- 1
	} ()
	// wait for 11ms to by pass cold start
	time.Sleep(11 * time.Millisecond)
	// for the next 30ms, qps should be at about 10qps
	for i := 0; i < 30; i += 1 {
		qps, _, _, _ := tracker.Ticks()
		if qps > 20 || qps < 5 {
			t.Errorf("Well, qps is %v, doesn't seem acurate", qps)
		} else {
			t.Logf("qps seems ok at: %v\n", qps)
		}
		time.Sleep(time.Millisecond)
	}
	// wait till qps ramp up to 4 per ms
	time.Sleep(31 * time.Millisecond)
	for i := 0; i < 10; i += 1 {
		qps, eps, _, _ := tracker.Ticks()
		if qps > 50 || qps <= 15 {
			t.Errorf("Well, qps is %v, doesn't seem acurate", qps)
		} else {
			t.Logf("qps seems ok at: %v\n", qps)
		}
		if qps > 2 * eps || eps > 2 * qps {
			t.Errorf("Well, qps is %v and eps is %v, doesn't seem acurate", qps, eps)
		} else {
			t.Logf("ratio of qps and eps seems ok at: %v and %v\n", qps, eps)
		}
		time.Sleep(time.Millisecond)
	}

	<-done
}

func TestDoWithChance(t *testing.T) {
	var s int64
	DoWithChance(1, func(){
		s += 1
	})
	if s != 1 {
		t.Errorf("sum should be 1, but it's not. sum is %v", s)
	}

	s = 0
	DoWithChance(1.5, func(){
			s += 1
		})
	if s != 1 {
		t.Errorf("sum should be 1, but it's not. sum is %v", s)
	}

	s = 0
	DoWithChance(2.2, func(){
			s += 1
		})
	if s != 2 {
		t.Errorf("sum should be 2, but it's not. sum is %v", s)
	}

	s = 0
	for i := 0; i < 10000; i += 1 {
		DoWithChance(0, func(){
			s += 1
		})
	}
	if s != 0 {
		t.Errorf("sum should be 0, but it's not. sum is %v", s)
	}

	s = 0
	for i := 0; i < 1000; i += 1 {
		DoWithChance(0.1, func(){
			s += 1
		})
	}
	if s > 120 || s < 80 {
		t.Errorf("sum should be about 10, but it's not. sum is %v", s)
	}

}
