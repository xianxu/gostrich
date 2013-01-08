package gostrich

/*
 * Ostrich in go, so that we can play go in Twitter's infrastructure.
 */
import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// expose command line and memory stats.
// TODO: move those stats over to gostrich so those can be viz-ed?
import _ "expvar"
import _ "net/http/pprof"

var (
	// command line arguments that can be used to customize this module's singleton
	AdminPort       = flag.Int("admin_port", 8300, "admin port")
	DebugPort       = flag.Int("debug_port", 6300, "debug port")
	JsonLineBreak   = flag.Bool("json_line_break", true, "whether break lines for json")
	StatsSampleSize = flag.Int("stats_sample_size", 1001, "how many samples to keep for stats, "+
		"to compute average etc.")
	PortOffset = flag.Int("port_offset", 0, "Offset serving port by this much. This is used "+
		"to start up multiple services on same host")

	// debugging
	NumCPU     = flag.Int("num_cpu", 1, "Number of cpu to use. Use 0 to use all CPU")
	CpuProfile = flag.String("cpu_profile", "", "Write cpu profile to file")

	StartUpTime = time.Now().Unix() // start up time

	// internal states
	adminLock = sync.Mutex{}
	admin     *adminServer // singleton stats, that's "typically" what you need
)

/*
 * An Admin provides a start up method and get root of stats collector.
 */
type Admin interface {
	StartToLive(registers []func(*http.ServeMux)) error
	GetStats() Stats
}

/*
 * The interface used to collect various stats. It provides counters, gauges, labels and samples.
 * It also provides a way to scope Stats collector to a prefixed domain. All implementation should
 * be thread safe.
 */
type Stats interface {
	Counter(name string) Counter
	AddGauge(name string, gauge func() float64) bool
	AddLabel(name string, label func() string) bool
	Statistics(name string) IntSampler
	Scoped(name string) Stats
}

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
type IntSampler interface {
	Observe(f int64)
	Sampled() []int64
	Clear()
	IsFull() bool
}

/*
 * One implementation of sampler, it does so by keeping track of last n items. It also keeps
 * track of overall count and sum, so historical average can be calculated.
 */
type intSampler struct {
	// historical
	count  int64
	sum    int64
	length int

	// thread safe buffer
	cache []int64
}

/*
 * This is just a intSampler with preallocated buffer that gostrich use internally
 */
type intSamplerWithClone struct {
	*intSampler
	// cloned cache's used to do stats reporting, where we need to sort the content of cache.
	clonedCache []int64
}

/*
 * myInt64 will be a Counter.
 */
type myInt64 int64

/*
 * will be a Stats
 */
type statsRecord struct {
	// Global lock's bad, user should keep references to actual collectors, such as Counters
	// instead of doing name resolution each every time.
	lock sync.RWMutex

	counters map[string]*int64
	gauges   map[string]func() float64
	labels   map[string]func() string

	samplerSize int // val
	statistics  map[string]*intSamplerWithClone
}

type adminServer struct {
	stats        *statsRecord
	shutdownHook func()
}

/*
 * statsRecord with a scope name, it prefix all stats with this scope name.
 */
type scopedStatsRecord struct {
	base  *statsRecord
	scope string
}

/*
 * Stats that can be served through HTTP
 */
type statsHttp struct {
	*statsRecord
	address string
}

/*
 * Serves Json endpoint.
 */
type statsHttpJson struct {
	*statsHttp
	jsonLineBreak bool
}

/*
 * Serves Txt endpoint.
 */
type statsHttpTxt statsHttp

/*
 * Creates a sampler of given size
 */
func NewIntSampler(size int) *intSampler {
	return &intSampler{
		0,
		0,
		size,
		make([]int64, size),
	}
}

func NewIntSamplerWithClone(size int) *intSamplerWithClone {
	return &intSamplerWithClone{
		&intSampler{
			0,
			0,
			size,
			make([]int64, size),
		},
		make([]int64, size),
	}
}

func (s *intSampler) Observe(f int64) {
	count := atomic.AddInt64(&(s.count), 1)
	atomic.AddInt64(&(s.sum), f)
	atomic.StoreInt64(&s.cache[int((count-1)%int64(s.length))], f)
}

// Note: what's returned is not thread safe, caller needs to use thread safe way to access its
// element, such as atomic.LoadInt64 on each element, to be safe. It is provided to avoid
// allocating another cache.
func (s *intSampler) Sampled() []int64 {
	n := atomic.LoadInt64(&(s.count))
	if n < int64(s.length) {
		return s.cache[0:n]
	}
	return s.cache
}

func (s *intSampler) Clear() {
	atomic.StoreInt64(&s.count, 0)
	atomic.StoreInt64(&s.sum, 0)
	for i := range s.cache {
		atomic.StoreInt64(&s.cache[i], 0)
	}
}

func (s *intSampler) IsFull() bool {
	n := atomic.LoadInt64(&(s.count))
	return n >= int64(s.length)
}

/*
 * Create a new stats object
 */
func NewStats(sampleSize int) *statsRecord {
	return &statsRecord{
		sync.RWMutex{},
		make(map[string]*int64),
		make(map[string]func() float64),
		make(map[string]func() string),
		sampleSize,
		make(map[string]*intSamplerWithClone),
	}
}

func (sr *statsRecord) Counter(name string) Counter {
	sr.lock.RLock()
	if v, ok := sr.counters[name]; ok {
		sr.lock.RUnlock()
		return (*myInt64)(v)
	}
	sr.lock.RUnlock()

	sr.lock.Lock()
	defer sr.lock.Unlock()

	if v, ok := sr.counters[name]; ok {
		return (*myInt64)(v)
	}

	var v int64
	vv := &v
	sr.counters[name] = vv
	return (*myInt64)(vv)
}

func (sr *statsRecord) AddGauge(name string, gauge func() float64) bool {
	sr.lock.RLock()
	if _, ok := sr.gauges[name]; ok {
		sr.lock.RUnlock()
		return false
	}
	sr.lock.RUnlock()

	sr.lock.Lock()
	defer sr.lock.Unlock()

	if _, ok := sr.gauges[name]; ok {
		return false
	}

	sr.gauges[name] = gauge
	return true
}

func (sr *statsRecord) AddLabel(name string, label func() string) bool {
	sr.lock.RLock()
	if _, ok := sr.labels[name]; ok {
		sr.lock.RUnlock()
		return false
	}
	sr.lock.RUnlock()

	sr.lock.Lock()
	defer sr.lock.Unlock()

	if _, ok := sr.labels[name]; ok {
		return false
	}

	sr.labels[name] = label
	return true
}

func (sr *statsRecord) Statistics(name string) IntSampler {
	sr.lock.RLock()
	if v, ok := sr.statistics[name]; ok {
		sr.lock.RUnlock()
		return (v)
	}
	sr.lock.RUnlock()

	sr.lock.Lock()
	defer sr.lock.Unlock()

	if v, ok := sr.statistics[name]; ok {
		return (v)
	}

	vv := NewIntSamplerWithClone(sr.samplerSize)
	sr.statistics[name] = vv
	return vv
}

func (sr *statsRecord) Scoped(name string) Stats {
	return &scopedStatsRecord{
		sr,
		name,
	}
}

func (ssr *scopedStatsRecord) Counter(name string) Counter {
	return ssr.base.Counter(ssr.scope + "/" + name)
}

func (ssr *scopedStatsRecord) AddGauge(name string, gauge func() float64) bool {
	return ssr.base.AddGauge(ssr.scope+"/"+name, gauge)
}

func (ssr *scopedStatsRecord) AddLabel(name string, label func() string) bool {
	return ssr.base.AddLabel(ssr.scope+"/"+name, label)
}
func (ssr *scopedStatsRecord) Statistics(name string) IntSampler {
	return ssr.base.Statistics(ssr.scope + "/" + name)
}

func (ssr *scopedStatsRecord) Scoped(name string) Stats {
	return &scopedStatsRecord{
		ssr.base,
		ssr.scope + "/" + name,
	}
}

func (c *myInt64) Incr(by int64) int64 {
	return atomic.AddInt64((*int64)(c), by)
}

func (c *myInt64) Get() int64 {
	return atomic.LoadInt64((*int64)(c))
}

// represent sorted numbers, with a name
type sortedValues struct {
	name   string
	values []float64
}

/*
 * Output a sorted array of float64 as percentile in Json format.
 */
func sortedToJson(w http.ResponseWriter, array []int64, count int64, sum int64) {
	fmt.Fprintf(w, "{")
	length := len(array)
	l1 := length - 1
	if length > 0 {
		// historical
		fmt.Fprintf(w, "\"count\":%v,", count)
		fmt.Fprintf(w, "\"sum\":%v,", sum)
		fmt.Fprintf(w, "\"average\":%v,", float64(sum)/float64(count))

		// percentile
		fmt.Fprintf(w, "\"minimum\":%v,", array[0])
		fmt.Fprintf(w, "\"p25\":%v,", array[int(math.Min(0.25*float64(length), float64(l1)))])
		fmt.Fprintf(w, "\"p50\":%v,", array[int(math.Min(0.50*float64(length), float64(l1)))])
		fmt.Fprintf(w, "\"p75\":%v,", array[int(math.Min(0.75*float64(length), float64(l1)))])
		fmt.Fprintf(w, "\"p90\":%v,", array[int(math.Min(0.90*float64(length), float64(l1)))])
		fmt.Fprintf(w, "\"p99\":%v,", array[int(math.Min(0.99*float64(length), float64(l1)))])
		fmt.Fprintf(w, "\"p999\":%v,", array[int(math.Min(0.999*float64(length), float64(l1)))])
		fmt.Fprintf(w, "\"maximum\":%v", array[l1])
	}
	fmt.Fprintf(w, "}")
}

/*
 * Output a sorted array of float64 as percentile in text format.
 */
func sortedToTxt(w http.ResponseWriter, array []int64, count int64, sum int64) {
	length := len(array)
	l1 := length - 1
	fmt.Fprintf(w, "(")
	if length > 0 {
		// historical
		fmt.Fprintf(w, "count=%v, ", count)
		fmt.Fprintf(w, "sum=%v, ", sum)
		fmt.Fprintf(w, "average=%v, ", float64(sum)/float64(count))

		// percentile
		fmt.Fprintf(w, "minimum=%v, ", array[0])
		fmt.Fprintf(w, "p25=%v, ", array[int(math.Min(0.25*float64(length), float64(l1)))])
		fmt.Fprintf(w, "p50=%v, ", array[int(math.Min(0.50*float64(length), float64(l1)))])
		fmt.Fprintf(w, "p75=%v, ", array[int(math.Min(0.75*float64(length), float64(l1)))])
		fmt.Fprintf(w, "p90=%v, ", array[int(math.Min(0.90*float64(length), float64(l1)))])
		fmt.Fprintf(w, "p99=%v, ", array[int(math.Min(0.99*float64(length), float64(l1)))])
		fmt.Fprintf(w, "p999=%v, ", array[int(math.Min(0.999*float64(length), float64(l1)))])

		fmt.Fprintf(w, "maximum=%v", array[l1])
	}
	fmt.Fprintf(w, ")")
}

func jsonEncode(v interface{}) string {
	if b, err := json.Marshal(v); err == nil {
		return string(b)
	}
	return "bad_json_value"
}

func (sr *statsHttpJson) breakLines() string {
	if sr.jsonLineBreak {
		return "\n"
	}
	return ""
}

/*
 * High perf freeze content of a sampler and sort it
 */
func freezeAndSort(s *intSamplerWithClone) (int64, int64, []int64) {
	// freeze, there might be a drift, we are fine
	count := atomic.LoadInt64(&s.count)
	sum := atomic.LoadInt64(&s.sum)

	// copy cache
	for i := range s.cache {
		s.clonedCache[i] = atomic.LoadInt64(&(s.cache[i]))
	}
	v := s.clonedCache
	if count < int64(s.length) {
		v = s.clonedCache[0:int(count)]
	}
	sort.Sort(Int64Slice(v))
	return count, sum, v
}

/*
 * Admin HTTP handler Json endpoint.
 */
func (sr *statsHttpJson) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// no more stats can be created during reporting, existing stats can be updated.
	sr.lock.RLock()
	defer sr.lock.RUnlock()

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	fmt.Fprintf(w, "{"+sr.breakLines())
	first := true
	// counters
	for k, v := range sr.counters {
		if !first {
			fmt.Fprintf(w, ","+sr.breakLines())
		}
		first = false
		fmt.Fprintf(w, "%v: %v", jsonEncode(k), *v)
	}
	// gauges
	for k, f := range sr.gauges {
		if !first {
			fmt.Fprintf(w, ","+sr.breakLines())
		}
		first = false
		fmt.Fprintf(w, "%v: %v", jsonEncode(k), f())
	}
	// labels
	for k, f := range sr.labels {
		if !first {
			fmt.Fprintf(w, ","+sr.breakLines())
		}
		first = false
		fmt.Fprintf(w, "%v: %v", jsonEncode(k), jsonEncode(f()))
	}
	// stats
	for k, v := range sr.statistics {
		count, sum, vv := freezeAndSort(v)
		if count > 0 {
			if !first {
				fmt.Fprintf(w, ","+sr.breakLines())
			}
			first = false
			fmt.Fprintf(w, "%v: ", jsonEncode(k))
			sortedToJson(w, vv, count, sum)
			fmt.Fprintf(w, "\n")
		}
	}
	fmt.Fprintf(w, sr.breakLines()+"}"+sr.breakLines())
}

/*
 * Admin HTTP handler txt endpoint.
 */
func (sr *statsHttpTxt) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// no more stats can be created during reporting, existing stats can be updated.
	sr.lock.RLock()
	defer sr.lock.RUnlock()

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	// counters
	for k, v := range sr.counters {
		fmt.Fprintf(w, "%v: %v\n", k, *v)
	}
	// gauges
	for k, f := range sr.gauges {
		fmt.Fprintf(w, "%v: %v\n", k, f())
	}
	// labels
	for k, f := range sr.labels {
		fmt.Fprintf(w, "%v: %v\n", k, f())
	}
	// stats
	for k, v := range sr.statistics {
		count, sum, vv := freezeAndSort(v)
		if count > 0 {
			fmt.Fprintf(w, "%v: ", k)
			sortedToTxt(w, vv, count, sum)
			fmt.Fprintf(w, "\n")
		}
	}
}

func init() {
}

type AdminError string

func (e AdminError) Error() string {
	return string(e)
}

func (stats *statsRecord) GetStats() Stats {
	return stats
}

func (admin *adminServer) GetStats() Stats {
	return admin.stats
}

/*
 * Blocks current goroutine. Call http /shutdown to shutdown.
 */
func (admin *adminServer) StartToLive(adminPort int, jsonLineBreak bool, registers []func(*http.ServeMux)) error {
	// only start a single copy
	statsHttpImpl := &statsHttp{admin.stats, ":" + strconv.Itoa(adminPort)}
	statsJson := &statsHttpJson{statsHttpImpl, jsonLineBreak}
	statsTxt := (*statsHttpTxt)(statsHttpImpl)

	shutdown := make(chan int)
	serverError := make(chan error)

	mux := http.NewServeMux()
	mux.Handle("/stats.json", statsJson)
	mux.Handle("/stats.txt", statsTxt)
	mux.HandleFunc("/shutdown", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Bye\n")
		shutdown <- 0
	})

	// register other handlers
	if registers != nil {
		for _, register := range registers {
			register(mux)
		}
	}

	server := http.Server{
		statsHttpImpl.address,
		mux,
		18 * time.Second,
		10 * time.Second,
		0,
		nil,
	}

	go func() {
		serverError <- server.ListenAndServe()
	}()

	select {
	case er := <-serverError:
		return AdminError("Can't start up server, error was: " + er.Error())
	case <-shutdown:
		log.Println("Shutdown requested")
	}

	if admin.shutdownHook != nil {
		admin.shutdownHook()
	}
	return nil
}

func AdminServer() *adminServer {
	adminLock.Lock()
	defer adminLock.Unlock()
	if admin == nil {
		admin = &adminServer{NewStats(*StatsSampleSize), nil}

		// some basic stats
		admin.stats.AddGauge("uptime", func() float64 {
			return float64(time.Now().Unix() - StartUpTime)
		})
		// TODO: other basic stats, such as branch name. How to do that with go's build system?
	}
	return admin
}

/*
 * Main entry function of gostrich
 */
func StartToLive(registers []func(*http.ServeMux)) error {
	ncpu := *NumCPU

	log.Printf("Admin staring to live, with admin port of %v and debug port of %v with %v CPUs", *AdminPort+*PortOffset, *DebugPort+*PortOffset, ncpu)

	if ncpu == 0 {
		ncpu = runtime.NumCPU()
	}
	runtime.GOMAXPROCS(ncpu)

	if *CpuProfile != "" {
		log.Println("Enabling profiling")
		f, err := os.Create(*CpuProfile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
	}

	// starts up debugging server
	go func() {
		log.Println(http.ListenAndServe(":"+strconv.Itoa(*DebugPort+*PortOffset), nil))
	}()
	//making sure stats are created.
	AdminServer()
	admin.shutdownHook = func() {
		if *CpuProfile != "" {
			pprof.StopCPUProfile()
		}
		log.Printf("Shutdown gostrich.")
	}
	return admin.StartToLive(*AdminPort+*PortOffset, *JsonLineBreak, registers)
}

func UpdatePort(address string, offset int) string {
	parts := strings.Split(address, ":")
	if len(parts) == 1 {
		port, err := strconv.Atoi(parts[0])
		if err != nil {
			panic("unknown address format")
		}
		return strconv.Itoa(port + offset)
	} else if len(parts) == 2 {
		port, err := strconv.Atoi(parts[1])
		if err != nil {
			panic("unknown address format")
		}
		return parts[0] + ":" + strconv.Itoa(port+offset)
	} else {
		panic("unknown address format")
	}
	return ""
}

// Int64 slice that allows sorting
type Int64Slice []int64

func (ints Int64Slice) Len() int {
	return len([]int64(ints))
}
func (ints Int64Slice) Less(i, j int) bool {
	slice := []int64(ints)
	return slice[i] < slice[j]
}
func (ints Int64Slice) Swap(i, j int) {
	slice := []int64(ints)
	slice[i], slice[j] = slice[j], slice[i]
}
