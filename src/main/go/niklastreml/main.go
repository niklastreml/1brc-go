package main

import (
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/exp/mmap"
)

const (
	filename = "measurements.txt"
	lines    = 100_000_000
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

	workers :=  runtime.NumCPU()
	cResults := make(chan TempCity, workers)

	chunkSize := lines / workers

	var wg sync.WaitGroup
	wg.Add(workers)

	for w := range workers {
		go func(start, end int) {
			fmt.Println("starting worker", start, end)
			// move forward to first newline
			for i := start; ; i++ {
				if reader.At(i) == '\n' {
					start = i + 1
					break
				}
			}

			for i := start; i < end; {
				var b int
				name, number, b := ReadLine(reader, i)
				temperature := parseFloatIntoInt(number)
				cResults <- TempCity{name, temperature}

				i += b + 1
			}

			wg.Done()
		}(w*chunkSize, w*chunkSize+chunkSize)
	}

	results := map[string]*Result{}
	go func() {
		for tc := range cResults {
			if v, ok := results[tc.Name]; !ok {
				results[tc.Name] = &Result{
					tc.Temperature, tc.Temperature, tc.Temperature, 1,
				}
			} else {
				v.Amount++
				v.Sum += tc.Temperature
				if v.Min > tc.Temperature {
					v.Min = tc.Temperature
				} else if v.Max < tc.Temperature {
					v.Max = tc.Temperature
				}
			}
		}
	}()

	wg.Wait()
	for k, v := range results {
		fmt.Println(k, v.Min, v.Sum/v.Amount, v.Max)
	}
}

// ReadLine reads one line from reader and reads it into a name and number string
// start should be the adress of the beginning of the line
func ReadLine(reader *mmap.ReaderAt, start int) (string, string, int) {
	nameBuilder := strings.Builder{}
	numberBuilder := strings.Builder{}
	nameDone := false

	readBytes := 0
	for ; ; readBytes++ {
		b := reader.At(start + readBytes)
		if b == '\n' {
			break
		}
		if b != ';' {
			if nameDone {
				numberBuilder.WriteByte(b)
				continue
			} else {
				nameBuilder.WriteByte(b)
			}
		} else {
			nameDone = true
		}
	}

	return nameBuilder.String(), numberBuilder.String(), readBytes
}

func parseFloatIntoInt(f string) int {
	s := strings.Builder{}
	for _, c := range f {
		if c != '.' {
			s.WriteRune(c)
		}
	}

	i, err := strconv.ParseInt(s.String(), 10, 32)
	if err != nil {
		panic(err)
	}

	return int(i)
}

func buildNumber(num int64) string {
	b := strings.Builder{}
	b.Grow(4)
	s := strconv.FormatInt(num/10, 10)

	b.WriteString(s)
	b.WriteByte('.')

	v := (num % 10) + '0'
	b.WriteByte(byte(v))

	return b.String()
}

func abs(value int64) int64 {
	// evil bitshift hack fml
	temp := value >> 63
	value = value ^ temp
	value = value + temp&1

	return value
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
