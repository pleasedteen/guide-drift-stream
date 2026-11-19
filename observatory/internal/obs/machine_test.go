package obs

import (
	"reflect"
	"testing"
)

func reading(seq int64, gw, tel string, ts int64, drift float64) Message {
	return Message{Seq: seq, Gateway: gw, MsgID: "m", Type: TypeReading, Telescope: tel, Ts: ts, Drift: drift}
}

func progress(seq int64, gw string, p int64) Message {
	return Message{Seq: seq, Gateway: gw, MsgID: "m", Type: TypeProgress, Progress: p}
}

var twoGW = []string{"gw-a", "gw-b"}

// Window 600000 covers [600000, 600060); it closes at watermark 600060 and
// finalizes at watermark 600120.

func TestWindowAggregation(t *testing.T) {
	m := NewMachine(twoGW)
	m.Process(reading(1, "gw-a", "T1", 600005, 0.5))
	m.Process(reading(2, "gw-a", "T1", 600010, 0.9))
	m.Process(reading(3, "gw-a", "T1", 600020, 0.4))
	m.Process(reading(4, "gw-a", "T2", 600006, 0.7)) // other telescope
	m.Process(reading(5, "gw-a", "T1", 600061, 1.1)) // other window

	k := WinKey{"T1", 600000}
	if a := m.Aggs[k]; a.Count != 3 || a.MaxDrift != 0.9 {
		t.Fatalf("T1@600000: got %+v, want count=3 max=0.9", a)
	}
	if a := m.Aggs[WinKey{"T2", 600000}]; a.Count != 1 || a.MaxDrift != 0.7 {
		t.Fatalf("T2@600000: got %+v, want count=1 max=0.7", a)
	}
	if a := m.Aggs[WinKey{"T1", 600060}]; a.Count != 1 || a.MaxDrift != 1.1 {
		t.Fatalf("T1@600060: got %+v, want count=1 max=1.1", a)
	}
}

func TestWatermarkNeedsAllGatewaysAndPublishesFirstVersion(t *testing.T) {
	m := NewMachine(twoGW)
	m.Process(reading(1, "gw-a", "T1", 600005, 0.5))
	m.Process(reading(2, "gw-a", "T1", 600010, 0.9))

	// Only one gateway reported: the watermark must not advance.
	d := m.Process(progress(3, "gw-a", 600060))
	if len(d.Versions) != 0 {
		t.Fatalf("published with one gateway silent: %v", d.Versions)
	}
	if _, ok := m.Watermark(); ok {
		t.Fatal("watermark established before all gateways reported")
	}

	// Second gateway reports: watermark = min(600060, 600120) = 600060,
	// which reaches the window end -> first version, triggered by seq 4.
	d = m.Process(progress(4, "gw-b", 600120))
	want := []VersionRow{{Telescope: "T1", WindowStart: 600000, Version: 1, Count: 2, MaxDrift: 0.9, TriggerSeq: 4}}
	if !reflect.DeepEqual(d.Versions, want) {
		t.Fatalf("got %+v, want %+v", d.Versions, want)
	}
	if wm, _ := m.Watermark(); wm != 600060 {
		t.Fatalf("watermark = %d, want 600060", wm)
	}
}

func TestWatermarkIsMinAndPerGatewayProgressIsMax(t *testing.T) {
	m := NewMachine(twoGW)
	m.Process(progress(1, "gw-a", 100))
	m.Process(progress(2, "gw-b", 200))
	if wm, _ := m.Watermark(); wm != 100 {
		t.Fatalf("wm=%d, want 100", wm)
	}
	m.Process(progress(3, "gw-a", 150))
	if wm, _ := m.Watermark(); wm != 150 {
		t.Fatalf("wm=%d, want 150", wm)
	}
	// A gateway re-reporting a lower value must not regress anything.
	m.Process(progress(4, "gw-b", 120))
	if wm, _ := m.Watermark(); wm != 150 {
		t.Fatalf("wm regressed to %d, want 150", wm)
	}
	m.Process(progress(5, "gw-a", 300))
	if wm, _ := m.Watermark(); wm != 200 {
		t.Fatalf("wm=%d, want 200", wm)
	}
}

func TestLateReadingPublishesRevision(t *testing.T) {
	m := NewMachine(twoGW)
	m.Process(reading(1, "gw-a", "T1", 600005, 0.5))
	m.Process(progress(2, "gw-a", 600060))
	m.Process(progress(3, "gw-b", 600060)) // window 600000 closes -> v1

	// Late reading while 600060 <= wm < 600120: immediate revision.
	d := m.Process(reading(4, "gw-a", "T1", 600001, 1.5))
	if d.Disposition != DispApplied {
		t.Fatalf("disposition = %q, want applied", d.Disposition)
	}
	want := []VersionRow{{Telescope: "T1", WindowStart: 600000, Version: 2, Count: 2, MaxDrift: 1.5, TriggerSeq: 4}}
	if !reflect.DeepEqual(d.Versions, want) {
		t.Fatalf("got %+v, want %+v", d.Versions, want)
	}
}

func TestLateReadingCanBeFirstVersion(t *testing.T) {
	m := NewMachine(twoGW)
	m.Process(progress(1, "gw-a", 600060))
	m.Process(progress(2, "gw-b", 600060)) // watermark 600060, window empty so far

	// The first reading of the window arrives late: publish v1 immediately.
	d := m.Process(reading(3, "gw-a", "T1", 600010, 0.7))
	want := []VersionRow{{Telescope: "T1", WindowStart: 600000, Version: 1, Count: 1, MaxDrift: 0.7, TriggerSeq: 3}}
	if !reflect.DeepEqual(d.Versions, want) {
		t.Fatalf("got %+v, want %+v", d.Versions, want)
	}
}

func TestFinalizedWindowIgnoresLateReadings(t *testing.T) {
	m := NewMachine(twoGW)
	m.Process(reading(1, "gw-a", "T1", 600005, 0.5))
	m.Process(progress(2, "gw-a", 600120))
	m.Process(progress(3, "gw-b", 600120)) // wm=600120: window 600000 finalized

	d := m.Process(reading(4, "gw-a", "T1", 600006, 9.9))
	if d.Disposition != DispIgnoredFinalized {
		t.Fatalf("disposition = %q, want %q", d.Disposition, DispIgnoredFinalized)
	}
	if len(d.Versions) != 0 || len(d.Aggs) != 0 {
		t.Fatalf("finalized reading changed state: %+v", d)
	}
	if a := m.Aggs[WinKey{"T1", 600000}]; a.Count != 1 || a.MaxDrift != 0.5 {
		t.Fatalf("aggregate changed after finalization: %+v", a)
	}
}

func TestWatermarkJumpPublishesAndFinalizes(t *testing.T) {
	m := NewMachine(twoGW)
	m.Process(reading(1, "gw-a", "T1", 600005, 0.5))
	// Watermark jumps straight past finalization of window 600000.
	m.Process(progress(2, "gw-a", 600180))
	d := m.Process(progress(3, "gw-b", 600180))
	want := []VersionRow{{Telescope: "T1", WindowStart: 600000, Version: 1, Count: 1, MaxDrift: 0.5, TriggerSeq: 3}}
	if !reflect.DeepEqual(d.Versions, want) {
		t.Fatalf("got %+v, want %+v", d.Versions, want)
	}
	// The window is finalized: a further late reading is ignored.
	d = m.Process(reading(4, "gw-a", "T1", 600006, 9.9))
	if d.Disposition != DispIgnoredFinalized {
		t.Fatalf("disposition = %q, want %q", d.Disposition, DispIgnoredFinalized)
	}
}

func TestEmptyWindowNeverPublished(t *testing.T) {
	m := NewMachine(twoGW)
	m.Process(progress(1, "gw-a", 600120))
	d := m.Process(progress(2, "gw-b", 600120))
	if len(d.Versions) != 0 {
		t.Fatalf("published versions for windows without readings: %v", d.Versions)
	}
}
