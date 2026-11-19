// Package obs implements the core of the observatory drift-aggregation
// service: a deterministic stream processor (Machine), its SQLite-backed
// durable store (Store), and the HTTP API (Server).
package obs

import "sort"

// WinKey identifies one aggregation window: a telescope and a UTC minute.
type WinKey struct {
	Telescope   string
	WindowStart int64 // unix seconds, always a multiple of 60
}

// Agg is the running aggregate of one window.
type Agg struct {
	Count    int
	MaxDrift float64
}

// VersionRow is one published version of a window's aggregate.
// TriggerSeq is the receive-log position of the message whose processing
// caused this version to be published.
type VersionRow struct {
	Telescope   string  `json:"telescope"`
	WindowStart int64   `json:"window_start"`
	Version     int     `json:"version"`
	Count       int     `json:"count"`
	MaxDrift    float64 `json:"max_drift"`
	TriggerSeq  int64   `json:"trigger_seq"`
}

// Message types.
const (
	TypeReading  = "reading"
	TypeProgress = "progress"
)

// Dispositions recorded on messages when they are processed.
const (
	// DispApplied means the message was folded into the aggregates.
	DispApplied = "applied"
	// DispIgnoredFinalized means a reading arrived after its window was
	// finalized (watermark >= window_end + 60s): it stays in the receive
	// log but does not change any aggregate or version.
	DispIgnoredFinalized = "ignored_finalized"
)

// Message is one entry of the receive log.
type Message struct {
	Seq       int64
	Gateway   string
	MsgID     string
	Type      string
	Telescope string
	Ts        int64
	Drift     float64
	Progress  int64
}

// Delta is the state change produced by processing one message. The live
// processor persists it transactionally; replay just collects the versions.
type Delta struct {
	Aggs        map[WinKey]Agg
	Versions    []VersionRow
	Progs       map[string]int64
	Disposition string
}

// Machine is the deterministic stream processor shared by live processing
// and replay. Fed the same message sequence, it produces the same window
// versions with the same trigger positions.
type Machine struct {
	gateways []string
	Aggs     map[WinKey]Agg
	Vers     map[WinKey]int // latest published version per window (0 = none)
	Progs    map[string]int64
}

// NewMachine returns an empty machine for the given fixed gateway set.
func NewMachine(gateways []string) *Machine {
	return &Machine{
		gateways: gateways,
		Aggs:     map[WinKey]Agg{},
		Vers:     map[WinKey]int{},
		Progs:    map[string]int64{},
	}
}

// WindowStart returns the start of the UTC minute window containing ts
// (ts is validated to be >= 0 at receive time).
func WindowStart(ts int64) int64 { return ts - ts%60 }

// watermarkOf computes the global watermark: the minimum of the per-gateway
// progress values. The second result is false while any gateway has not
// reported yet, in which case the watermark does not advance at all.
func watermarkOf(gateways []string, progs map[string]int64) (int64, bool) {
	var w int64
	for i, g := range gateways {
		p, ok := progs[g]
		if !ok {
			return 0, false
		}
		if i == 0 || p < w {
			w = p
		}
	}
	return w, true
}

// Watermark returns the current global watermark, see watermarkOf.
func (m *Machine) Watermark() (int64, bool) {
	return watermarkOf(m.gateways, m.Progs)
}

// Process folds one message into the machine state and reports the changes
// as a Delta. Messages must be fed in receive-log (seq) order.
func (m *Machine) Process(msg Message) Delta {
	d := Delta{Aggs: map[WinKey]Agg{}, Progs: map[string]int64{}}
	switch msg.Type {
	case TypeReading:
		key := WinKey{Telescope: msg.Telescope, WindowStart: WindowStart(msg.Ts)}
		wm, ok := m.Watermark()
		if ok && wm >= key.WindowStart+120 {
			// The window was finalized one minute after its end; this late
			// reading only remains in the receive log.
			d.Disposition = DispIgnoredFinalized
			return d
		}
		a := m.Aggs[key]
		a.Count++
		if msg.Drift > a.MaxDrift {
			a.MaxDrift = msg.Drift
		}
		m.Aggs[key] = a
		d.Aggs[key] = a
		if ok && wm >= key.WindowStart+60 {
			// The watermark has already passed the window end but not yet
			// the finalization point: publish immediately (first version
			// or a revision).
			d.Versions = append(d.Versions, m.publish(key, msg.Seq))
		}
		d.Disposition = DispApplied
	case TypeProgress:
		oldW, oldOK := m.Watermark()
		if p, reported := m.Progs[msg.Gateway]; !reported || msg.Progress > p {
			// Per-gateway progress is the max of what it ever reported.
			m.Progs[msg.Gateway] = msg.Progress
			d.Progs[msg.Gateway] = msg.Progress
		}
		newW, newOK := m.Watermark()
		if newOK && (!oldOK || newW > oldW) {
			// The watermark advanced: publish the first version of every
			// window that has readings and whose end the watermark has
			// reached.
			for _, key := range sortedKeys(m.Aggs) {
				if m.Vers[key] == 0 && key.WindowStart+60 <= newW {
					d.Versions = append(d.Versions, m.publish(key, msg.Seq))
				}
			}
		}
		d.Disposition = DispApplied
	}
	return d
}

// publish records the next version of a window and returns its row.
func (m *Machine) publish(key WinKey, triggerSeq int64) VersionRow {
	a := m.Aggs[key]
	v := m.Vers[key] + 1
	m.Vers[key] = v
	return VersionRow{
		Telescope:   key.Telescope,
		WindowStart: key.WindowStart,
		Version:     v,
		Count:       a.Count,
		MaxDrift:    a.MaxDrift,
		TriggerSeq:  triggerSeq,
	}
}

func sortedKeys(aggs map[WinKey]Agg) []WinKey {
	keys := make([]WinKey, 0, len(aggs))
	for k := range aggs {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].WindowStart != keys[j].WindowStart {
			return keys[i].WindowStart < keys[j].WindowStart
		}
		return keys[i].Telescope < keys[j].Telescope
	})
	return keys
}
