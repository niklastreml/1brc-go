package main

import (
	"fmt"
	"hash"
	"os"
	"runtime"
	"runtime/pprof"
	"slices"
	"sync"

	"golang.org/x/exp/mmap"
)

const (
	filename        = "measurements.txt"
	numKeys  uint64 = 413
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
	// workers := 3

	fmt.Println("Running with", workers, "workers")

	chunkSize := reader.Len() / workers

	fmt.Printf("Using %d chunks of %d bytes\n", workers, chunkSize)

	// prealloc := chunkSize / 1500

	// fmt.Printf("Pre allocating %d map keys\n", prealloc)

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

	results := make(chan HashMap, workers)

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

			// result := make(map[string]*Result, prealloc) // map[string]*Result{}
			//result := make([]*Result, numKeys)
			result := HashMap{
				Data:   make([]*Result, numKeys),
				Reader: reader,
			}

			for i := start; i < end; {
				var b int
				nameLength, number, b := ReadLine(reader, i)
				temperature := ParseFloatIntoInt(number)

				if v := result.Load(i, nameLength); v == nil {
					r := Result{
						i, nameLength, temperature, temperature, temperature, 1,
					}

					result.Store(&r)
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
				// if i > chunkSize/100 {
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
	final := make([]*Result, numKeys)

	nDone := 0
	for m := range results {
		nDone++
		// fmt.Printf("Got results %d/%d\r", nDone, workers)
		for k, originalV := range m.Data {
			if finalV := final[k]; finalV != nil {
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

	slices.SortFunc(final, func(a, b *Result) int {
		// ensure nil go to the end of the array
		if a == nil {
			return 1
		}
		if b == nil {
			return -1
		}
		swapped := 1
		if a.NameLength > b.NameLength {
			a, b = b, a
			swapped = -1
		}

		for i := range a.NameLength {
			aName := a.NameAddr + i
			bName := b.NameAddr + i

			aByte, bByte := reader.At(aName), reader.At(bName)
			if aByte < bByte {
				return -1 * swapped
			} else if aByte > bByte {
				return 1 * swapped
			}
		}

		if b.NameLength > a.NameLength {
			// special case where a is a substring of b
			// a = "Hamburg"
			// b = "Hamburger"
			// since a is always less than b, we declare that the longer string 'b' should be sorted
			// after the shorter string 'a'
			return -1
		}
		return 0
	})

	// allocate a buffer of 50 bytes for the read at which we can reuse
	b := make([]byte, 50)
	for _, v := range final {
		// if v is nil, no more data will come after it
		if v == nil {
			break
		}
		reader.ReadAt(b, int64(v.NameAddr))
		fmt.Printf("%s;%.1f;%.1f;%.1f\n", b[:v.NameLength], float32(v.Min)/10, float32(v.Sum/v.Amount)/10, float32(v.Max)/10)
	}

}

// ReadLine reads one line from reader and reads it into a name and number string
// start should be the adress of the beginning of the line
// the first is the length of the name
// the second is the read number without decimals and in reverse
// the last is the number of bytes read in total, for advancing the read pointer
func ReadLine(reader *mmap.ReaderAt, start int) (int, [5]byte, int) {
	// we need to write this in reverse
	numberBuilder := [5]byte{}
	nameLength := 0
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
				nameLength++
			}
		} else {
			nameDone = true
		}
	}

	return nameLength, numberBuilder, readBytes
}

func ParseFloatIntoInt(f [5]byte) int {
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

type Result struct {
	NameAddr   int
	NameLength int
	Min        int
	Max        int
	Sum        int
	Amount     int
}

type HashMap struct {
	Data   []*Result
	Reader *mmap.ReaderAt
	h      hash.Hash64
}

func (h *HashMap) Store(d *Result) {
	h.Data[h.hashfnv(d.NameAddr, d.NameLength)] = d
}

func (h *HashMap) Load(addr, length int) *Result {
	return h.Data[h.hashfnv(addr, length)]
}

const prime64 = 1099511628211

func (h *HashMap) hashfnv(addr, length int) uint64 {
	var hash uint64 = 0

	for i := range length {
		hash ^= uint64(h.Reader.At(addr + i))
		hash *= prime64
	}

	return hash % numKeys
}
