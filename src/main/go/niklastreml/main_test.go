package main

import (
	"fmt"
	"testing"
)

var r string

func benchmarkBuildNumber(max int64, b *testing.B) {

	var v string
	for n := 0; n < b.N; n++ {
		var i int64
		for i = 0 - (max / 2); i < max/2; i++ {
			v = buildNumber(i)
		}
	}
	r = v
}

func BenchmarkBuildNumber10(b *testing.B)        { benchmarkBuildNumber(10, b) }
func BenchmarkBuildNumber100(b *testing.B)       { benchmarkBuildNumber(100, b) }
func BenchmarkBuildNumber1000(b *testing.B)      { benchmarkBuildNumber(1000, b) }
func BenchmarkBuildNumber10000(b *testing.B)     { benchmarkBuildNumber(10000, b) }
func BenchmarkBuildNumber100000(b *testing.B)    { benchmarkBuildNumber(100000, b) }
func BenchmarkBuildNumber1000000(b *testing.B)   { benchmarkBuildNumber(1000000, b) }
func BenchmarkBuildNumber10000000(b *testing.B)  { benchmarkBuildNumber(10000000, b) }
func BenchmarkBuildNumber100000000(b *testing.B) { benchmarkBuildNumber(100000000, b) }

// func BenchmarkBuildNumber1000000000(b *testing.B) { benchmarkBuildNumber(1_000_000_000, b) }

func benchmarkFmtNumber(max int64, b *testing.B) {
	var v string
	for n := 0; n < b.N; n++ {
		for i := 0 - (max / 2); i < max/2; i++ {
			v = fmt.Sprintf("%d.%d", i/10, abs(i%10))
		}
	}
	r = v
}

func BenchmarkFmtNumber10(b *testing.B)        { benchmarkFmtNumber(10, b) }
func BenchmarkFmtNumber100(b *testing.B)       { benchmarkFmtNumber(100, b) }
func BenchmarkFmtNumber1000(b *testing.B)      { benchmarkFmtNumber(1000, b) }
func BenchmarkFmtNumber10000(b *testing.B)     { benchmarkFmtNumber(10000, b) }
func BenchmarkFmtNumber100000(b *testing.B)    { benchmarkFmtNumber(100000, b) }
func BenchmarkFmtNumber1000000(b *testing.B)   { benchmarkFmtNumber(1000000, b) }
func BenchmarkFmtNumber10000000(b *testing.B)  { benchmarkFmtNumber(10000000, b) }
func BenchmarkFmtNumber100000000(b *testing.B) { benchmarkFmtNumber(100000000, b) }

// func BenchmarkFmtNumber1000000000(b *testing.B){benchmarkFmtNumber(1000000000, b)}

func benchmarkConvertNumber(max int64, b *testing.B) {
	var v string
	for n := 0; n < b.N; n++ {
		for i := 0 - (max / 2); i < max/2; i++ {
			f := float64(i)
			f = f / 10
			v = fmt.Sprintf("%.1f", f)
		}
	}
	r = v
}

func BenchmarkConvertNumber10(b *testing.B)        { benchmarkConvertNumber(10, b) }
func BenchmarkConvertNumber100(b *testing.B)       { benchmarkConvertNumber(100, b) }
func BenchmarkConvertNumber1000(b *testing.B)      { benchmarkConvertNumber(1000, b) }
func BenchmarkConvertNumber10000(b *testing.B)     { benchmarkConvertNumber(10000, b) }
func BenchmarkConvertNumber100000(b *testing.B)    { benchmarkConvertNumber(100000, b) }
func BenchmarkConvertNumber1000000(b *testing.B)   { benchmarkConvertNumber(1000000, b) }
func BenchmarkConvertNumber10000000(b *testing.B)  { benchmarkConvertNumber(10000000, b) }
func BenchmarkConvertNumber100000000(b *testing.B) { benchmarkConvertNumber(100000000, b) }

func TestAbs(t *testing.T) {
	type testcase struct {
		in   int64
		want int64
	}

	cases := []testcase{
		{in: 40, want: 40},
		{in: -40, want: 40},
	}

	for _, c := range cases {
		got := abs(c.in)
		if got != c.want {
			t.Errorf("Expected got to be %d but got %d instead", c.want, got)
		}
	}
}
