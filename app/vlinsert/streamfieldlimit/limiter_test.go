package streamfieldlimit

import (
	"testing"
	"time"
)

func TestAllowDisabled(t *testing.T) {
	resetConfigForTest()
	defer resetConfigForTest()

	if !Allow("default") {
		t.Fatalf("unexpected reject when limiter is disabled")
	}
}

func TestAllowLimit(t *testing.T) {
	resetConfigForTest()
	defer resetConfigForTest()
	configureForTest("namespace", 2, time.Hour)

	if !Allow("default") {
		t.Fatalf("unexpected reject for the first row")
	}
	if !Allow("default") {
		t.Fatalf("unexpected reject for the second row")
	}
	if Allow("default") {
		t.Fatalf("unexpected allow after limit is reached")
	}
	if !Allow("kube-system") {
		t.Fatalf("unexpected reject for another stream field value")
	}
}

func TestAllowDoesNotIsolateByTenant(t *testing.T) {
	resetConfigForTest()
	defer resetConfigForTest()
	configureForTest("namespace", 1, time.Hour)

	if !Allow("default") {
		t.Fatalf("unexpected reject for the first row")
	}
	if Allow("default") {
		t.Fatalf("unexpected allow for the same stream field value")
	}
}

func TestAllowWindowReset(t *testing.T) {
	resetConfigForTest()
	defer resetConfigForTest()
	configureForTest("namespace", 1, time.Nanosecond)

	if !Allow("default") {
		t.Fatalf("unexpected reject for the first row")
	}
	time.Sleep(time.Millisecond)
	if !Allow("default") {
		t.Fatalf("unexpected reject after window reset")
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
