package streamfieldlimit

import (
	"testing"
	"time"
)

func TestAllowNDisabled(t *testing.T) {
	resetConfigForTest()
	defer resetConfigForTest()

	if n := AllowN("default", 10); n != 10 {
		t.Fatalf("unexpected allowed rows when limiter is disabled; got %d; want 10", n)
	}
}

func TestAllowNLimit(t *testing.T) {
	resetConfigForTest()
	defer resetConfigForTest()
	configureForTest("namespace", 2, time.Hour)

	if n := AllowN("default", 1); n != 1 {
		t.Fatalf("unexpected allowed rows for the first batch; got %d; want 1", n)
	}
	if n := AllowN("default", 10); n != 1 {
		t.Fatalf("unexpected allowed rows for the second batch; got %d; want 1", n)
	}
	if n := AllowN("default", 1); n != 0 {
		t.Fatalf("unexpected allowed rows after limit is reached; got %d; want 0", n)
	}
	if n := AllowN("kube-system", 1); n != 1 {
		t.Fatalf("unexpected allowed rows for another stream field value; got %d; want 1", n)
	}
}

func TestAllowNAccumulatesPerValue(t *testing.T) {
	resetConfigForTest()
	defer resetConfigForTest()
	configureForTest("namespace", 1, time.Hour)

	if n := AllowN("default", 1); n != 1 {
		t.Fatalf("unexpected allowed rows for the first batch; got %d; want 1", n)
	}
	if n := AllowN("default", 1); n != 0 {
		t.Fatalf("unexpected allowed rows for the same stream field value; got %d; want 0", n)
	}
}

func TestAllowNWindowReset(t *testing.T) {
	resetConfigForTest()
	defer resetConfigForTest()
	configureForTest("namespace", 1, time.Nanosecond)

	if n := AllowN("default", 1); n != 1 {
		t.Fatalf("unexpected allowed rows for the first batch; got %d; want 1", n)
	}
	time.Sleep(time.Millisecond)
	if n := AllowN("default", 1); n != 1 {
		t.Fatalf("unexpected allowed rows after window reset; got %d; want 1", n)
	}
}

func configureForTest(field string, rows int, d time.Duration) {
	*key = field
	*rowsPerWindow = rows
	*window = d
	Reset()
}

func resetConfigForTest() {
	*key = ""
	*rowsPerWindow = 0
	*window = 24 * time.Hour
	Reset()
}
