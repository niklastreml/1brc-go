package main

import (
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"slices"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/exp/mmap"
)

const (
	filename = "measurements.txt"
)

type TempCity struct {
	Name        string
	Temperature int
}

func main() {
	reader, err := mmap.Open(filename)
	if err != nil {
		panic(err)
	}

	defer reader.Close()

	workers := runtime.NumCPU() * 128

	fmt.Println("Running with", workers, "workers")

	chunkSize := reader.Len() / workers

	fmt.Printf("Using %d chunks of %d bytes\n", workers, chunkSize)

	f, err := os.Create("profile.prof")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	if err := pprof.StartCPUProfile(f); err != nil {
		panic(err)
	}
	defer pprof.StopCPUProfile()

	var wg sync.WaitGroup
	wg.Add(workers)

	results := make(chan map[string]*Result, workers)

	for w := range workers {
		go func(w, start, end int) {
			// fmt.Println("starting worker", w, start, end)
			// move forward to first newline
			if start != 0 {
				for i := start; ; i++ {
					if reader.At(i) == '\n' {
						start = i + 1
						break
					}
				}
			}

			result := map[string]*Result{}

			for i := start; i < end; {
				var b int
				name, number, b := ReadLine(reader, i)
				temperature := parseFloatIntoInt(number)

				if v, ok := result[name]; !ok {
					r := Result{
						temperature, temperature, temperature, 1,
					}

					result[name] = &r
				} else {
					v.Amount++
					v.Sum += temperature
					if v.Min > temperature {
						v.Min = temperature
					} else if v.Max < temperature {
						v.Max = temperature
					}
				}

				i += b + 1

				// reduce for testing
				// if i > chunkSize/3 {
				// 	break
				// }
			}

			results <- result
			wg.Done()
		}(w, w*chunkSize, w*chunkSize+chunkSize)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	// results.Range(func(k string, v Result) bool {
	// 	fmt.Printf("%s;%.2f;%.2f;%.2f\n", k, float32(v.Min)/10, float32(v.Sum/v.Amount)/10, float32(v.Max)/10)
	// 	return true
	// })
	final := map[string]*Result{}

	nDone := 0
	for m := range results {
		nDone++
		fmt.Printf("Got results %d/%d\r", nDone, workers)
		for k, originalV := range m {
			if finalV, ok := final[k]; ok {
				if finalV.Max < originalV.Max {
					finalV.Max = originalV.Max
				}
				if finalV.Min > originalV.Min {
					finalV.Min = originalV.Min
				}

				finalV.Sum += originalV.Sum
				finalV.Amount += originalV.Amount
				final[k] = finalV
			} else {
				final[k] = originalV
			}
		}
	}

	keys := []string{}
	for k := range final {
		keys = append(keys, k)
	}

	slices.Sort(keys)

	for _, k := range keys {
		v, _ := final[k]
		fmt.Printf("%s;%.1f;%.1f;%.1f\n", k, float32(v.Min)/10, float32(v.Sum/v.Amount)/10, float32(v.Max)/10)
	}

}

// ReadLine reads one line from reader and reads it into a name and number string
// start should be the adress of the beginning of the line
func ReadLine(reader *mmap.ReaderAt, start int) (string, [5]byte, int) {
	nameBuilder := strings.Builder{}
	// we need to write this in reverse
	numberBuilder := [5]byte{}

	nameBuilder.Grow(50)
	nameDone := false

	readBytes := 0
	nI := 4
	for ; ; readBytes++ {
		b := reader.At(start + readBytes)
		if b == '\n' {
			break
		}
		if b != ';' {
			if b == '.' {
				continue
			}
			if nameDone {
				numberBuilder[nI] = b
				nI--
				continue
			} else {
				nameBuilder.WriteByte(b)
			}
		} else {
			nameDone = true
		}
	}

	return nameBuilder.String(), numberBuilder, readBytes
}

func parseFloatIntoInt(f [5]byte) int {
	asInt := 0
	var zero byte = '0'

	mult := 0
	negative := false
	for _, b := range f {
		if b == 0 {
			continue
		}
		if b == '-' {
			negative = true
			continue
		}
		scalar := b - zero
		asInt += int(scalar) * ipow(10, mult)
		mult++
	}

	if negative {
		asInt = -asInt
	}

	return asInt
}

func ipow(base, exp int) int {
	result := 1
	for {
		if exp&1 == 1 {
			result *= base
		}
		exp >>= 1
		if exp == 0 {
			break
		}
		base *= base
	}
	return result
}

func buildNumber(num int) string {
	b := strings.Builder{}
	b.Grow(4)
	s := strconv.FormatInt(int64(num/10), 10)

	b.WriteString(s)
	b.WriteByte('.')

	v := (num % 10) + '0'
	b.WriteByte(byte(v))

	return b.String()
}

type Result struct {
	Min    int
	Max    int
	Sum    int
	Amount int
}

func ParseLine(s string) (station string, measurement int64) {
	parts := strings.Split(s, ";")

	station = parts[0]

	num := []rune{}
	for _, c := range parts[1] {
		if c == '.' {
			continue
		}
		num = append(num, c)
	}

	var err error
	if measurement, err = strconv.ParseInt(string(num), 10, 32); err != nil {
		panic(err)
	}
	return station, measurement
}

type Map[K comparable, V any] struct {
	m sync.Map
}

func (m *Map[K, V]) Delete(key K) { m.m.Delete(key) }
func (m *Map[K, V]) Load(key K) (value V, ok bool) {
	v, ok := m.m.Load(key)
	if !ok {
		return value, ok
	}
	return v.(V), ok
}
func (m *Map[K, V]) LoadAndDelete(key K) (value V, loaded bool) {
	v, loaded := m.m.LoadAndDelete(key)
	if !loaded {
		return value, loaded
	}
	return v.(V), loaded
}
func (m *Map[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	a, loaded := m.m.LoadOrStore(key, value)
	return a.(V), loaded
}
func (m *Map[K, V]) Range(f func(key K, value V) bool) {
	m.m.Range(func(key, value any) bool { return f(key.(K), value.(V)) })
}
func (m *Map[K, V]) Store(key K, value V) { m.m.Store(key, value) }
