package obs

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"
)

func tempStore(t *testing.T, gateways []string, capacity int64) (*Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	s, err := Open(context.Background(), path, gateways, capacity)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s, path
}

func mustReceive(t *testing.T, s *Store, m Message) (int64, bool) {
	t.Helper()
	seq, dup, err := s.Receive(context.Background(), m)
	if err != nil {
		t.Fatalf("receive %+v: %v", m, err)
	}
	return seq, dup
}

func TestReceiveIdempotentAndGapless(t *testing.T) {
	s, _ := tempStore(t, twoGW, 100)
	ctx := context.Background()

	m1 := Message{Gateway: "gw-a", MsgID: "a-1", Type: TypeReading, Telescope: "T1", Ts: 600005, Drift: 0.5}
	seq, dup := mustReceive(t, s, m1)
	if seq != 1 || dup {
		t.Fatalf("first receive: seq=%d dup=%v, want 1,false", seq, dup)
	}
	// Retry of the same message returns the original position.
	seq, dup = mustReceive(t, s, m1)
	if seq != 1 || !dup {
		t.Fatalf("retry: seq=%d dup=%v, want 1,true", seq, dup)
	}
	// Same msg_id from a different gateway is a different message.
	seq, dup = mustReceive(t, s, Message{Gateway: "gw-b", MsgID: "a-1", Type: TypeProgress, Progress: 10})
	if seq != 2 || dup {
		t.Fatalf("other gateway: seq=%d dup=%v, want 2,false", seq, dup)
	}
	seq, _ = mustReceive(t, s, Message{Gateway: "gw-a", MsgID: "a-2", Type: TypeProgress, Progress: 10})
	if seq != 3 {
		t.Fatalf("third receive: seq=%d, want 3 (gapless)", seq)
	}

	info, err := s.Progress(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.Received != 3 || info.Processed != 0 || info.Pending != 3 {
		t.Fatalf("progress = %+v, want received=3 processed=0 pending=3", info)
	}
}

func TestReceiveCapacity(t *testing.T) {
	s, _ := tempStore(t, twoGW, 2)
	ctx := context.Background()
	mk := func(id string) Message {
		return Message{Gateway: "gw-a", MsgID: id, Type: TypeReading, Telescope: "T1", Ts: 600005, Drift: 0.5}
	}
	mustReceive(t, s, mk("a-1"))
	mustReceive(t, s, mk("a-2"))
	// Capacity reached: a new message gets a retryable error...
	if _, _, err := s.Receive(ctx, mk("a-3")); err != ErrBusy {
		t.Fatalf("err = %v, want ErrBusy", err)
	}
	// ...but a retry of an already-received message still returns its position.
	seq, dup, err := s.Receive(ctx, mk("a-1"))
	if err != nil || seq != 1 || !dup {
		t.Fatalf("duplicate at capacity: seq=%d dup=%v err=%v, want 1,true,nil", seq, dup, err)
	}
	// Draining frees capacity.
	if n, err := s.ProcessAll(ctx); err != nil || n != 2 {
		t.Fatalf("ProcessAll: n=%d err=%v, want 2,nil", n, err)
	}
	if _, _, err := s.Receive(ctx, mk("a-3")); err != nil {
		t.Fatalf("receive after drain: %v", err)
	}
}

// buildLog feeds a fixed message script covering: two windows, a late
// revision, an ignored (finalized) reading, and watermark gating.
func buildLog(t *testing.T, s *Store) {
	t.Helper()
	msgs := []Message{
		{Gateway: "gw-a", MsgID: "a-1", Type: TypeReading, Telescope: "T1", Ts: 600005, Drift: 0.5},
		{Gateway: "gw-a", MsgID: "a-2", Type: TypeReading, Telescope: "T1", Ts: 600010, Drift: 0.9},
		{Gateway: "gw-b", MsgID: "b-1", Type: TypeReading, Telescope: "T2", Ts: 600006, Drift: 0.4},
		{Gateway: "gw-a", MsgID: "a-3", Type: TypeReading, Telescope: "T1", Ts: 600061, Drift: 0.7}, // next window
		{Gateway: "gw-a", MsgID: "a-4", Type: TypeProgress, Progress: 600060},                       // no close: gw-b silent
		{Gateway: "gw-b", MsgID: "b-2", Type: TypeProgress, Progress: 600060},                       // closes window 600000
		{Gateway: "gw-a", MsgID: "a-5", Type: TypeReading, Telescope: "T1", Ts: 600001, Drift: 1.5}, // late -> revision
		{Gateway: "gw-a", MsgID: "a-6", Type: TypeProgress, Progress: 600180},
		{Gateway: "gw-b", MsgID: "b-3", Type: TypeProgress, Progress: 600190},                       // closes 600060, finalizes 600000
		{Gateway: "gw-a", MsgID: "a-7", Type: TypeReading, Telescope: "T1", Ts: 600002, Drift: 9.9}, // too late -> ignored
	}
	for _, m := range msgs {
		mustReceive(t, s, m)
	}
}

func TestProcessingAndVersions(t *testing.T) {
	s, _ := tempStore(t, twoGW, 100)
	ctx := context.Background()
	buildLog(t, s)
	if n, err := s.ProcessAll(ctx); err != nil || n != 10 {
		t.Fatalf("ProcessAll: n=%d err=%v", n, err)
	}

	d, err := s.GetWindow(ctx, "T1", 600000)
	if err != nil {
		t.Fatal(err)
	}
	wantHist := []VersionRow{
		{Telescope: "T1", WindowStart: 600000, Version: 1, Count: 2, MaxDrift: 0.9, TriggerSeq: 6},
		{Telescope: "T1", WindowStart: 600000, Version: 2, Count: 3, MaxDrift: 1.5, TriggerSeq: 7},
	}
	if !reflect.DeepEqual(d.History, wantHist) {
		t.Fatalf("T1@600000 history = %+v, want %+v", d.History, wantHist)
	}
	if !d.Finalized {
		t.Fatal("T1@600000 should be finalized at watermark 600180")
	}

	d, err = s.GetWindow(ctx, "T1", 600060)
	if err != nil {
		t.Fatal(err)
	}
	// Watermark 600180 also finalizes window 600060 (600120 + 60).
	if len(d.History) != 1 || d.History[0].TriggerSeq != 9 || !d.Finalized {
		t.Fatalf("T1@600060 = %+v, want one version triggered by seq 9, finalized", d)
	}

	// The too-late reading is in the log, marked ignored, and triggered nothing.
	md, err := s.GetMessage(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if md.Disposition != DispIgnoredFinalized || len(md.Triggered) != 0 {
		t.Fatalf("message 10 = %+v, want ignored_finalized with no triggered versions", md)
	}
	// The progress message that closed window 600000 points at its versions.
	md, err = s.GetMessage(ctx, 6)
	if err != nil {
		t.Fatal(err)
	}
	if len(md.Triggered) != 2 { // T1@600000 v1 and T2@600000 v1
		t.Fatalf("message 6 triggered %d versions, want 2", len(md.Triggered))
	}

	info, err := s.Progress(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.Received != 10 || info.Processed != 10 || info.Watermark == nil || *info.Watermark != 600180 {
		t.Fatalf("progress = %+v, want received=processed=10 watermark=600180", info)
	}
}

func TestRecoveryAcrossReopen(t *testing.T) {
	s, path := tempStore(t, twoGW, 100)
	ctx := context.Background()
	buildLog(t, s)
	if _, err := s.ProcessAll(ctx); err != nil {
		t.Fatal(err)
	}
	before, err := s.AllVersions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	s.Close()

	// Reopen on the same file: state must be fully recovered.
	s2, err := Open(ctx, path, twoGW, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	after, err := s2.AllVersions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("versions changed across reopen:\nbefore=%+v\nafter=%+v", before, after)
	}
	info, err := s2.Progress(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.Received != 10 || info.Processed != 10 {
		t.Fatalf("progress after reopen = %+v", info)
	}

	// Processing continues: window 600120 (end 600180 <= watermark, not yet
	// finalized) gets a late first version and then a revision.
	mustReceive(t, s2, Message{Gateway: "gw-a", MsgID: "a-8", Type: TypeReading, Telescope: "T1", Ts: 600121, Drift: 1.2})
	mustReceive(t, s2, Message{Gateway: "gw-a", MsgID: "a-9", Type: TypeReading, Telescope: "T1", Ts: 600122, Drift: 0.3})
	if _, err := s2.ProcessAll(ctx); err != nil {
		t.Fatal(err)
	}
	d, err := s2.GetWindow(ctx, "T1", 600120)
	if err != nil {
		t.Fatal(err)
	}
	wantHist := []VersionRow{
		{Telescope: "T1", WindowStart: 600120, Version: 1, Count: 1, MaxDrift: 1.2, TriggerSeq: 11},
		{Telescope: "T1", WindowStart: 600120, Version: 2, Count: 2, MaxDrift: 1.2, TriggerSeq: 12},
	}
	if !reflect.DeepEqual(d.History, wantHist) {
		t.Fatalf("T1@600120 after reopen = %+v, want %+v", d.History, wantHist)
	}
}

func TestReplayMatchesLiveAndIsDeterministic(t *testing.T) {
	s, _ := tempStore(t, twoGW, 100)
	ctx := context.Background()
	buildLog(t, s)
	if _, err := s.ProcessAll(ctx); err != nil {
		t.Fatal(err)
	}
	live, err := s.AllVersions(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Replay 1: must reproduce the live versions exactly.
	id1, err := s.CreateReplay(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r1, err := s.GetReplay(ctx, id1)
	if err != nil {
		t.Fatal(err)
	}
	if r1.PrefixSeq != 10 {
		t.Fatalf("replay prefix = %d, want 10", r1.PrefixSeq)
	}
	if !reflect.DeepEqual(r1.Versions, live) {
		t.Fatalf("replay != live:\nreplay=%+v\nlive=%+v", r1.Versions, live)
	}

	// Replay 2 over the same prefix: identical result.
	id2, err := s.CreateReplay(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := s.GetReplay(ctx, id2)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(r1.Versions, r2.Versions) {
		t.Fatalf("same-prefix replays differ:\nr1=%+v\nr2=%+v", r1.Versions, r2.Versions)
	}

	// More live traffic, then a third replay: the first replay is untouched
	// and the live pipeline was never disturbed by replaying.
	mustReceive(t, s, Message{Gateway: "gw-a", MsgID: "a-8", Type: TypeReading, Telescope: "T1", Ts: 600062, Drift: 1.2})
	if _, err := s.ProcessAll(ctx); err != nil {
		t.Fatal(err)
	}
	r1again, err := s.GetReplay(ctx, id1)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(r1again.Versions, r1.Versions) {
		t.Fatal("replay 1 changed after later traffic")
	}
	id3, err := s.CreateReplay(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r3, err := s.GetReplay(ctx, id3)
	if err != nil {
		t.Fatal(err)
	}
	live, err = s.AllVersions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if r3.PrefixSeq != 11 || !reflect.DeepEqual(r3.Versions, live) {
		t.Fatalf("replay 3 prefix=%d, versions differ from live", r3.PrefixSeq)
	}
}

func TestReplayIncludesUnprocessedMessages(t *testing.T) {
	s, _ := tempStore(t, twoGW, 100)
	ctx := context.Background()
	buildLog(t, s)
	// Nothing processed yet: replay still sees the whole confirmed prefix.
	id, err := s.CreateReplay(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.GetReplay(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Versions) == 0 {
		t.Fatal("replay over unprocessed prefix produced no versions")
	}
	// Live pipeline has not been disturbed: still nothing processed.
	info, err := s.Progress(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.Processed != 0 || info.Pending != 10 {
		t.Fatalf("replay disturbed live progress: %+v", info)
	}
	if live, err := s.AllVersions(ctx); err != nil || len(live) != 0 {
		t.Fatalf("replay wrote live versions: %v %v", live, err)
	}
	// Once the live pipeline catches up, it agrees with the replay.
	if _, err := s.ProcessAll(ctx); err != nil {
		t.Fatal(err)
	}
	live, err := s.AllVersions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(live, r.Versions) {
		t.Fatalf("live after catch-up != replay:\nlive=%+v\nreplay=%+v", live, r.Versions)
	}
}
