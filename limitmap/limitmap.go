// Package limitmap provides map of semaphores to limit concurrency against some string keys.
//
// Usage:
// limits := NewLimitMap()
// func process(url *url.URL, rch chan *http.Response) {
// 	// At most 2 concurrent requests to each host.
// 	limits.Acquire(url.Host, 2)
// 	defer limits.Release(url.Host)
//	r, err := http.Get(url.String())
//	rch <- r
// }
// for url := range urlChan {
// 	go process(url, rch)
// }

package limitmap

import (
	"flag"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
)

func maybeSwitch() {
	// if rand.Intn(20) == 0 {
	if false {
		runtime.Gosched()
	}
}

// Internal structure, may be changed.
// Requirements for this data structure:
// * Acquire() will not block until internal counter reaches set maximum number
// * Release() will decrement internal counter and wake up one goroutine blocked on Acquire().
//   Calling Release() when internal counter is zero is programming error, panic.
type Semaphore struct {
	// Number of Acquires - Releases. When this goes to zero, this structure is removed from map.
	// Only updated inside LimitMap.lk lock.
	refs int

	max   uint
	value uint
	wait  sync.Cond
}

func NewSemaphore(max uint) *Semaphore {
	return &Semaphore{
		max:  max,
		wait: sync.Cond{L: new(sync.Mutex)},
	}
}

func (s *Semaphore) Acquire() uint {
	maybeSwitch()
	s.wait.L.Lock()
	defer s.wait.L.Unlock()
	maybeSwitch()
	for i := 0; ; i++ {
		maybeSwitch()
		if uint(s.value)+1 <= s.max {
			maybeSwitch()
			s.value++
			return s.value
		}
		maybeSwitch()
		s.wait.Wait()
		maybeSwitch()
	}
	panic("Unexpected branch")
}

func (s *Semaphore) Release() (result uint) {
	s.wait.L.Lock()
	defer s.wait.L.Unlock()
	maybeSwitch()
	s.value--
	maybeSwitch()
	if s.value < 0 {
		panic("Semaphore Release without Acquire")
	}
	maybeSwitch()
	s.wait.Signal()
	maybeSwitch()
	return
}

type LimitMap struct {
	lk     sync.Mutex
	limits map[string]*Semaphore
	wg     sync.WaitGroup
}

func NewLimitMap() *LimitMap {
	return &LimitMap{
		limits: make(map[string]*Semaphore),
	}
}

func (m *LimitMap) Acquire(key string, max uint) {
	m.lk.Lock()
	l, ok := m.limits[key]
	if !ok {
		l = NewSemaphore(max)
		m.limits[key] = l
	}
	l.refs++
	m.lk.Unlock()

	m.wg.Add(1)
	if x := l.Acquire(); x < 0 || x > l.max {
		panic("oia")
	}
}

func (m *LimitMap) Release(key string) {
	m.lk.Lock()
	l, ok := m.limits[key]
	if !ok {
		panic("LimitMap: key not in map. Possible reason: Release without Acquire.")
	}
	l.refs--
	if l.refs < 0 {
		panic("LimitMap internal error: refs < 0.")
	}
	if l.refs == 0 {
		delete(m.limits, key)
	}
	m.lk.Unlock()

	if x := l.Release(); x < 0 || x > l.max {
		panic("oir")
	}
	m.wg.Done()
}

// Wait until all released.
func (m *LimitMap) Wait() {
	m.wg.Wait()
}

func (m *LimitMap) Size() (keys int, total int) {
	m.lk.Lock()
	keys = len(m.limits)
	for _, l := range m.limits {
		total += int(l.value)
	}
	m.lk.Unlock()
	return
}

func init() {
	rand.Seed(int64(os.Getpid()))
}

func main() {
	var cpuprofile = flag.String("cpuprofile", "", "Write CPU profile to file")
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			os.Stderr.Write([]byte(err.Error()))
			os.Exit(1)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	const N = 1 << 21
	s := NewSemaphore(5)
	for i := 1; i <= N; i++ {
		s.Acquire()
		s.Acquire()
		s.Acquire()
		s.Acquire()
		s.Acquire()
		s.Release()
		s.Release()
		s.Release()
		s.Release()
		s.Release()
	}
	/*
		done := sync.WaitGroup{}
		limits := NewLimitMap()
		delay := func(n int) {
			for i := 1; i < n; i++ {
				maybeSwitch()
			}
		}
		process := func(key string) {
			limits.Acquire(key, 2)
			delay(5)
			limits.Release(key)
			done.Done()
		}
		done.Add(1)
		go func() {
			for i := 0; i < N; i++ {
				key := "key" + string((i%17)+0x30)
				done.Add(1)
				delay(40)
				go process(key)
			}
			done.Done()
		}()
		maybeSwitch()
		limits.Wait()
		done.Wait()
	*/
}
