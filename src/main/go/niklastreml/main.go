package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

const (
	filename = "measurements.txt"
)

func main() {
	f, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	s := bufio.NewScanner(f)

	out := map[string]Result{}

	for s.Scan() {
		station, measurement := ParseLine(s.Text())
		if r, ok := out[station]; !ok {
			out[station] = Result{
				measurement, measurement, measurement, 1,
			}
		} else {
			if r.Min > measurement {
				r.Min = measurement
			} else if r.Max < measurement {
				r.Max = measurement
			}

			r.Sum += measurement
			r.Amount++

			out[station] = r
		}
	}

	for k, v := range out {

		meanInt := v.Sum / v.Amount

		mean := buildNumber(meanInt)
		min := buildNumber(v.Min)
		max := buildNumber(v.Max)

		fmt.Printf("%s;%s;%s;%s\n", k, min, mean, max)
	}

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
	Min    int64
	Max    int64
	Sum    int64
	Amount int64
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
