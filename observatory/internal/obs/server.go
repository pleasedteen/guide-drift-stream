package obs

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
)

const indexText = `observatory drift-aggregation service

POST   /v1/messages                       receive a gateway message (reading or progress)
GET    /v1/messages/{seq}                 receive-log entry + versions it triggered
GET    /v1/progress                       processing progress and watermark
GET    /v1/windows?telescope=T1           all windows with readings
GET    /v1/windows/{telescope}/{start}    one window with its version history
GET    /v1/versions                       all published live versions
POST   /v1/replays                        replay the confirmed log prefix, returns replay id
GET    /v1/replays                        list replays
GET    /v1/replays/{id}                   replay result: per-window versions + triggers

Message format (JSON):
  {"gateway":"gw-a","msg_id":"a-1","type":"reading","telescope":"T1","ts":1735689605,"drift":0.37}
  {"gateway":"gw-a","msg_id":"a-2","type":"progress","progress":1735689660}
Receive response: {"seq":N,"duplicate":false}; HTTP 503 {"error":...,"retryable":true} at capacity.
`

// NewServer returns the HTTP handler of the service.
func NewServer(s *Store) http.Handler {
	srv := &server{store: s}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/messages", srv.postMessage)
	mux.HandleFunc("GET /v1/messages/{seq}", srv.getMessage)
	mux.HandleFunc("GET /v1/progress", srv.getProgress)
	mux.HandleFunc("GET /v1/windows", srv.listWindows)
	mux.HandleFunc("GET /v1/windows/{telescope}/{window_start}", srv.getWindow)
	mux.HandleFunc("GET /v1/versions", srv.listVersions)
	mux.HandleFunc("POST /v1/replays", srv.postReplay)
	mux.HandleFunc("GET /v1/replays", srv.listReplays)
	mux.HandleFunc("GET /v1/replays/{id}", srv.getReplay)
	mux.HandleFunc("GET /", srv.index)
	return mux
}

type server struct {
	store *Store
}

type incomingMessage struct {
	Gateway   string  `json:"gateway"`
	MsgID     string  `json:"msg_id"`
	Type      string  `json:"type"`
	Telescope string  `json:"telescope"`
	Ts        int64   `json:"ts"`
	Drift     float64 `json:"drift"`
	Progress  int64   `json:"progress"`
}

func (srv *server) validate(in *incomingMessage) (Message, error) {
	m := Message{Gateway: in.Gateway, MsgID: in.MsgID, Type: in.Type}
	known := false
	for _, g := range srv.store.Gateways() {
		if g == in.Gateway {
			known = true
			break
		}
	}
	if !known {
		return m, fmt.Errorf("unknown gateway %q", in.Gateway)
	}
	if in.MsgID == "" || len(in.MsgID) > 128 {
		return m, errors.New("msg_id must be 1..128 characters")
	}
	switch in.Type {
	case TypeReading:
		if in.Telescope == "" || len(in.Telescope) > 64 {
			return m, errors.New("reading requires a telescope name of 1..64 characters")
		}
		if in.Ts < 0 {
			return m, errors.New("reading ts must be >= 0 (unix seconds)")
		}
		if in.Drift < 0 || math.IsNaN(in.Drift) || math.IsInf(in.Drift, 0) {
			return m, errors.New("reading drift must be a finite number >= 0")
		}
		m.Telescope, m.Ts, m.Drift = in.Telescope, in.Ts, in.Drift
	case TypeProgress:
		if in.Progress < 0 {
			return m, errors.New("progress must be >= 0 (unix seconds)")
		}
		m.Progress = in.Progress
	default:
		return m, fmt.Errorf("type must be %q or %q", TypeReading, TypeProgress)
	}
	return m, nil
}

func (srv *server) postMessage(w http.ResponseWriter, r *http.Request) {
	var in incomingMessage
	dec := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&in); err != nil {
		writeError(w, http.StatusBadRequest, "invalid message JSON: "+err.Error())
		return
	}
	msg, err := srv.validate(&in)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	seq, dup, err := srv.store.Receive(r.Context(), msg)
	if errors.Is(err, ErrBusy) {
		w.Header().Set("Retry-After", "1")
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"error":     "receive capacity reached, retry later",
			"retryable": true,
		})
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"seq": seq, "duplicate": dup})
}

func (srv *server) getMessage(w http.ResponseWriter, r *http.Request) {
	seq, err := strconv.ParseInt(r.PathValue("seq"), 10, 64)
	if err != nil || seq < 1 {
		writeError(w, http.StatusBadRequest, "seq must be a positive integer")
		return
	}
	d, err := srv.store.GetMessage(r.Context(), seq)
	if errors.Is(err, ErrNotFound) {
		writeError(w, http.StatusNotFound, "no message with this seq")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, d)
}

func (srv *server) getProgress(w http.ResponseWriter, r *http.Request) {
	info, err := srv.store.Progress(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, info)
}

func (srv *server) listWindows(w http.ResponseWriter, r *http.Request) {
	windows, err := srv.store.ListWindows(r.Context(), r.URL.Query().Get("telescope"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"windows": windows})
}

func (srv *server) getWindow(w http.ResponseWriter, r *http.Request) {
	telescope := r.PathValue("telescope")
	ws, err := strconv.ParseInt(r.PathValue("window_start"), 10, 64)
	if err != nil || ws < 0 || ws%60 != 0 {
		writeError(w, http.StatusBadRequest, "window_start must be a minute-aligned unix timestamp")
		return
	}
	d, err := srv.store.GetWindow(r.Context(), telescope, ws)
	if errors.Is(err, ErrNotFound) {
		writeError(w, http.StatusNotFound, "no such window")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, d)
}

func (srv *server) listVersions(w http.ResponseWriter, r *http.Request) {
	versions, err := srv.store.AllVersions(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"versions": versions})
}

func (srv *server) postReplay(w http.ResponseWriter, r *http.Request) {
	id, err := srv.store.CreateReplay(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	d, err := srv.store.GetReplay(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, d.ReplaySummary)
}

func (srv *server) listReplays(w http.ResponseWriter, r *http.Request) {
	replays, err := srv.store.ListReplays(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"replays": replays})
}

func (srv *server) getReplay(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id < 1 {
		writeError(w, http.StatusBadRequest, "replay id must be a positive integer")
		return
	}
	d, err := srv.store.GetReplay(r.Context(), id)
	if errors.Is(err, ErrNotFound) {
		writeError(w, http.StatusNotFound, "no such replay")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, d)
}

func (srv *server) index(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	io.WriteString(w, indexText)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]any{"error": msg})
}
