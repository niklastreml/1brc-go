package main

import (
	"fmt"
	"strconv"
	"strings"

	"golang.org/x/exp/mmap"
)

const (
	filename = "measurements.txt"
)

var name, number string

func main() {
	reader, err := mmap.Open(filename)
	if err != nil {
		panic(err)
	}
	defer reader.Close()

	results := map[string]*Result{}

	for i := 0; i < reader.Len(); {
		var b int
		name, number, b = ReadLine(reader, i)
		temperature := parseFloatIntoInt(number)

		if v, ok := results[name]; !ok {
			results[name] = &Result{
				temperature, temperature, temperature, 1,
			}
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
	}

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
