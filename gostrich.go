package main

/*
 * A file
 * TODO:
 *   - prefer goroutine based over locks? e.g. locks can be avoided if a single goroutine
 *     proxy outside requests to an object.
 */
import (
	"fmt"
	"sync/atomic"
	"sync"
	"math/rand"
	/*"sort"*/
	"net/http"
	"time"
)

var (
	statsSingleton *statsRecord
	statsJson      *statsHttpJson
	statsTxt       *statsHttpTxt
	shutdown       chan int             // send any int to shutdown admin server
)

/*
 * Counter represents a thread safe way of keeping track of a single count.
 */
type Counter interface {
	Incr(by int64) int64
	Get() int64
}

/*
 * Sampler maintains a sample of input stream of numbers.
 */
type Sampler interface {
	Observe(f float64)
	Sampled() []float64
}

/*
 * One implementation of sampler
 */
type sampler struct {
	count int64
	cache []float64
}

/*
 * The interface used to collect various stats
 */
type Stats interface {
	Counter(name string) Counter
	AddGauge(name string, gauge func() float64) bool
	Statistics(name string) Sampler
}

type myInt64 int64

type statsRecord struct {
	lock        sync.Mutex
	counters    map[string]*int64
	gauges      map[string]func() float64
	samplerSize int
	statistics  map[string]Sampler
}

type statsHttpJson struct {
	*statsRecord
}

type statsHttpTxt struct {
	*statsRecord
}

func NewSampler(size int) Sampler {
	return &sampler{0, make([]float64, size)}
}

func (s *sampler) Observe(f float64) {
	count := atomic.AddInt64(&(s.count), 1)
	if count <= int64(len(s.cache)) {
		s.cache[count-1] = f
	} else {
		// if we have enough, we select f with probability len/count
		dice := rand.Int63n(count - 1)
		if dice < int64(len(s.cache)) {
			s.cache[dice] = f
		}
	}
}

func (s *sampler) Sampled() []float64 {
	if s.count < int64(len(s.cache)) {
		return s.cache[0:s.count]
	}
	return s.cache
}

func NewStats(sampleSize int) *statsRecord {
	return &statsRecord{
		sync.Mutex{},
		make(map[string]*int64),
		make(map[string]func() float64),
		sampleSize,
		make(map[string]Sampler),
	}
}

func (sr *statsRecord) Counter(name string) Counter {
	if v, ok := sr.counters[name]; ok {
		return (*myInt64)(v)
	}

	sr.lock.Lock()
	defer sr.lock.Unlock()

	var v int64
	vv := &v
	sr.counters[name] = vv
	return (*myInt64)(vv)
}

func (sr *statsRecord) AddGauge(name string, gauge func() float64) bool {
	if _, ok := sr.gauges[name]; ok {
		return false
	}

	sr.lock.Lock()
	defer sr.lock.Unlock()

	sr.gauges[name] = gauge
	return true
}

func (sr *statsRecord) Statistics(name string) Sampler {
	if v, ok := sr.statistics[name]; ok {
		return (v)
	}

	sr.lock.Lock()
	defer sr.lock.Unlock()

	vv := NewSampler(sr.samplerSize)
	sr.statistics[name] = vv
	return vv
}

func (c *myInt64) Incr(by int64) int64 {
	return atomic.AddInt64((*int64)(c), by)
}

func (c *myInt64) Get() int64 {
	return int64(*c)
}

func (sr *statsHttpJson) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	fmt.Fprintf(w, "{\n")
	first := true
	// counters
	for k, v := range sr.counters {
		if !first {
			fmt.Fprintf(w, ",\n")
		}
		first = false
		fmt.Fprintf(w, "\"%v\": %v", k, *v)
	}
	// gauges
	for k, f := range sr.gauges {
		if !first {
			fmt.Fprintf(w, ",\n")
		}
		first = false
		fmt.Fprintf(w, "\"%v\": %v", k, f())
	}
	//TODO percentile
	fmt.Fprintf(w, "\n}\n")
}

func (sr *statsHttpTxt) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	// counters
	for k, v := range sr.counters {
		fmt.Fprintf(w, "%v %v\n", k, *v)
	}
	// gauges
	for k, f := range sr.gauges {
		fmt.Fprintf(w, "%v %v\n", k, f())
	}
	//TODO percentile
}

func init() {
	statsSingleton = NewStats(1000)
	statsJson = &statsHttpJson{ statsSingleton }
	statsTxt = &statsHttpTxt{ statsSingleton }
    shutdown = make(chan int)

	http.Handle("/stats.json", statsJson)
	http.Handle("/stats.txt", statsTxt)
	http.HandleFunc("/shutdown", func(w http.ResponseWriter, r *http.Request){
		shutdown <- 0
	})

	//TODO: handle the plain text version
	//TODO: where do we block and wait?
	//TODO: the admin interface to shut thing down?
	go http.ListenAndServe("localhost:8000", nil)
}

/*
 * Blocks current coroutine. Call http /shutdown to shutdown.
 */
func StartToLive() {
	<-shutdown
}

func main() {
	stats := statsSingleton

	g1 := float64(0)
	stats.Counter("c1").Incr(1)
	stats.Counter("c1").Incr(1)
	stats.AddGauge("g1", func() float64 {
		return g1
	})

	fmt.Printf("Yo there %d\n", stats.Counter("c1").Get())
	fmt.Printf("Yo there %d\n", stats.Counter("g1").Get())
	s := NewSampler(3)
	s.Observe(1)
	s.Observe(1)
	s.Observe(1)
	s.Observe(2)
	s.Observe(2)
	stats.AddGauge("yo", func() float64 { return float64(time.Now().Second()) })
	fmt.Println(s.Sampled())
	//time.Sleep(100 * time.Second)
	StartToLive()
}
