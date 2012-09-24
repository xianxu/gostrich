package main

/*
 * Ostrich in go, so that we can play go in Twitter's infrastructure.
 */
import (
	"fmt"
	"sync/atomic"
	"sync"
	"math/rand"
	"sort"
	"net/http"
	"time"
	"math"
	"encoding/json"
)

var (
	statsSingleton *statsRecord         // singleton stats
	statsJson      *statsHttpJson       // holder for json admin endpoint
	statsTxt       *statsHttpTxt        // holder for txt admin endpoint
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

/*
 * myInt64 will be a Counter.
 */
type myInt64 int64

/*
 * will be a Stats
 */
type statsRecord struct {
	lock        sync.Mutex
	counters    map[string]*int64
	gauges      map[string]func() float64
	samplerSize int
	statistics  map[string]Sampler
}

/*
 * Stats that can be served through HTTP
 */
type statsHttp struct {
	*statsRecord
	address     string
	//TODO: make stats reporting configurable? E.g. how many p999 to return through HTTP.
}

/*
 * Serves Json endpoint.
 */
type statsHttpJson statsHttp

/*
 * Serves Txt endpoint.
 */
type statsHttpTxt  statsHttp

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

type sortedValues struct {
	name  string
	values []float64
}

/*
 * Output a sorted array of float64 as percentile in Json format.
 */
func sortedToJson(w http.ResponseWriter, array []float64) {
	fmt.Fprintf(w, "{")
	length := len(array)
	l1 := length - 1
	if length > 0 {
		fmt.Fprintf(w, "\"minimum\":%v,",array[0])
		fmt.Fprintf(w, "\"count\":%v,",length)
		sum := 0.0
		for _, v := range array {
			sum += v
		}
		fmt.Fprintf(w, "\"average\":%v,",sum/float64(length))
		fmt.Fprintf(w, "\"sum\":%v,",sum)
		fmt.Fprintf(w, "\"p25\":%v,",array[int(math.Min(0.25 * float64(length), float64(l1)))])
		fmt.Fprintf(w, "\"p50\":%v,",array[int(math.Min(0.50 * float64(length), float64(l1)))])
		fmt.Fprintf(w, "\"p75\":%v,",array[int(math.Min(0.75 * float64(length), float64(l1)))])
		fmt.Fprintf(w, "\"p90\":%v,",array[int(math.Min(0.90 * float64(length), float64(l1)))])
		fmt.Fprintf(w, "\"p99\":%v,",array[int(math.Min(0.99 * float64(length), float64(l1)))])
		fmt.Fprintf(w, "\"p999\":%v,",array[int(math.Min(0.999 * float64(length), float64(l1)))])

		fmt.Fprintf(w, "\"maximum\":%v",array[l1])
	}
	fmt.Fprintf(w, "}")
}

/*
 * Output a sorted array of float64 as percentile in text format.
 */
func sortedToTxt(w http.ResponseWriter, array []float64) {
	length := len(array)
	l1 := length - 1
	fmt.Fprintf(w, "(")
	if length > 0 {
		fmt.Fprintf(w, "minimum=%v, ",array[0])
		fmt.Fprintf(w, "count=%v, ",length)
		sum := 0.0
		for _, v := range array {
			sum += v
		}
		fmt.Fprintf(w, "average=%v, ",sum/float64(length))
		fmt.Fprintf(w, "sum=%v, ",sum)
		fmt.Fprintf(w, "p25=%v, ",array[int(math.Min(0.25 * float64(length), float64(l1)))])
		fmt.Fprintf(w, "p50=%v, ",array[int(math.Min(0.50 * float64(length), float64(l1)))])
		fmt.Fprintf(w, "p75=%v, ",array[int(math.Min(0.75 * float64(length), float64(l1)))])
		fmt.Fprintf(w, "p90=%v, ",array[int(math.Min(0.90 * float64(length), float64(l1)))])
		fmt.Fprintf(w, "p99=%v, ",array[int(math.Min(0.99 * float64(length), float64(l1)))])
		fmt.Fprintf(w, "p999=%v, ",array[int(math.Min(0.999 * float64(length), float64(l1)))])

		fmt.Fprintf(w, "maximum=%v",array[l1])
	}
	fmt.Fprintf(w, ")")
}

/*
 * Kicks off sorting of sampled data on collection on multiple CPUs.
 */
func (sr *statsRecord) sortOnMultipleCPUs(sorted chan sortedValues) {
	numItems := len(sr.statistics)
	if numItems == 0 {
		close(sorted)
		return
	}
	done := make(chan int)
	for k, v := range sr.statistics {
		go func() {
			sampled := v.Sampled()
			sort.Float64s(sampled)
			sorted <- sortedValues{k, sampled}
			done <- 1
		} ()
	}
	for x := range done {
		numItems -= x
		if numItems == 0 {
			close(sorted)
		}
	}
}

func jsonEncode(v interface{}) string {
	if b, err := json.Marshal(v); err == nil {
		return string(b)
	}
	return "bad-key"
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
		fmt.Fprintf(w, "%v: %v", jsonEncode(k), *v)
	}
	// gauges
	for k, f := range sr.gauges {
		if !first {
			fmt.Fprintf(w, ",\n")
		}
		first = false
		fmt.Fprintf(w, "%v: %v", jsonEncode(k), f())
	}
	// stats
	sorted := make(chan sortedValues)
	go sr.sortOnMultipleCPUs(sorted)
	for v := range sorted {
		if !first {
			fmt.Fprintf(w, ",\n")
		}
		first = false
		fmt.Fprintf(w, "%v: ", jsonEncode(v.name))
		sortedToJson(w, v.values)
	}
	fmt.Fprintf(w, "\n}\n")
}

func (sr *statsHttpTxt) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	// counters
	for k, v := range sr.counters {
		fmt.Fprintf(w, "%v: %v\n", k, *v)
	}
	// gauges
	for k, f := range sr.gauges {
		fmt.Fprintf(w, "%v: %v\n", k, f())
	}
	// stats
	sorted := make(chan sortedValues)
	go sr.sortOnMultipleCPUs(sorted)
	for v := range sorted {
		fmt.Fprintf(w, "%v: ", v.name)
		sortedToTxt(w, v.values)
	}
}

func init() {
	statsSingleton = NewStats(1000)
	statsHttpImpl := &statsHttp{ statsSingleton, ":8000" }
	statsJson = (*statsHttpJson)(statsHttpImpl)
	statsTxt = (*statsHttpTxt)(statsHttpImpl)

    shutdown = make(chan int)

	http.Handle("/stats.json", statsJson)
	http.Handle("/stats.txt", statsTxt)
	http.HandleFunc("/shutdown", func(w http.ResponseWriter, r *http.Request){
		shutdown <- 0
	})

	go http.ListenAndServe(statsHttpImpl.address, nil)
}

/*
 * Blocks current coroutine. Call http /shutdown to shutdown.
 */
func StartToLive() {
	<-shutdown
}

/*
 * Some random test code, not sure where to put them yet.
 */
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
	stats.Statistics("tflock").Observe(2)
	stats.Statistics("tflock").Observe(2)
	stats.Statistics("tflock").Observe(4)
	stats.Statistics("tflock").Observe(2)
	stats.Statistics("tflock").Observe(9)
	stats.AddGauge("yo", func() float64 { return float64(time.Now().Second()) })
	fmt.Println(s.Sampled())
	StartToLive()
}
