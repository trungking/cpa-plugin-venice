package monitor

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const maxRecords = 1000

var seq uint64

var state = struct {
	sync.Mutex
	records []Record
}{}

type Usage struct {
	InputTokens  int64 `json:"input_tokens"`
	OutputTokens int64 `json:"output_tokens"`
	TotalTokens  int64 `json:"total_tokens"`
}

type RequestInfo struct {
	Source      string
	Model       string
	Effort      string
	Tier        string
	InputTokens int64
}

type Result struct {
	Success      bool
	Error        string
	OutputTokens int64
	TotalTokens  int64
}

type Record struct {
	ID        string    `json:"id"`
	Source    string    `json:"source"`
	Provider  string    `json:"provider"`
	Model     string    `json:"model"`
	Effort    string    `json:"effort"`
	Tier      string    `json:"tier"`
	Status    string    `json:"status"`
	Success   bool      `json:"success"`
	Error     string    `json:"error,omitempty"`
	StartedAt time.Time `json:"started_at"`
	EndedAt   time.Time `json:"ended_at"`
	ElapsedMS int64     `json:"elapsed_ms"`
	TTFTMS    int64     `json:"ttft_ms"`
	Usage     Usage     `json:"usage"`
}

type Row struct {
	ID          string   `json:"id"`
	Source      string   `json:"source"`
	Provider    string   `json:"provider"`
	Model       string   `json:"model"`
	Effort      string   `json:"effort"`
	Tier        string   `json:"tier"`
	Recent      []bool   `json:"recent"`
	Status      string   `json:"status"`
	SuccessRate float64  `json:"success_rate"`
	Calls       int64    `json:"calls"`
	Success     int64    `json:"success"`
	Failed      int64    `json:"failed"`
	TPS         float64  `json:"tps"`
	TTFTMS      int64    `json:"ttft_ms"`
	ElapsedMS   int64    `json:"elapsed_ms"`
	Time        string   `json:"time"`
	Usage       Usage    `json:"usage"`
	Cost        string   `json:"cost"`
	Error       string   `json:"error,omitempty"`
	Details     []Record `json:"details,omitempty"`
}

type Snapshot struct {
	Provider string  `json:"provider"`
	Summary  Summary `json:"summary"`
	Rows     []Row   `json:"rows"`
}

type Summary struct {
	Rows     int `json:"rows"`
	Failures int `json:"failures"`
	Accounts int `json:"accounts"`
}

type Span struct {
	id      string
	info    RequestInfo
	started time.Time
	ttft    atomic.Int64
	done    atomic.Bool
}

func Start(info RequestInfo) *Span {
	source := strings.TrimSpace(info.Source)
	if source == "" {
		source = "Venice account"
	}
	model := strings.TrimSpace(info.Model)
	if model == "" {
		model = "unknown"
	}
	effort := strings.TrimSpace(info.Effort)
	if effort == "" {
		effort = "medium"
	}
	tier := strings.TrimSpace(info.Tier)
	if tier == "" {
		tier = "default"
	}
	n := atomic.AddUint64(&seq, 1)
	return &Span{
		id:      fmt.Sprintf("venice-%d-%d", time.Now().UnixNano(), n),
		info:    RequestInfo{Source: source, Model: model, Effort: effort, Tier: tier, InputTokens: info.InputTokens},
		started: time.Now(),
	}
}

func (s *Span) MarkTTFT() {
	if s == nil {
		return
	}
	ms := time.Since(s.started).Milliseconds()
	if ms <= 0 {
		ms = 1
	}
	s.ttft.CompareAndSwap(0, ms)
}

func (s *Span) Finish(result Result) {
	if s == nil || !s.done.CompareAndSwap(false, true) {
		return
	}
	ended := time.Now()
	elapsed := ended.Sub(s.started).Milliseconds()
	if elapsed <= 0 {
		elapsed = 1
	}
	ttft := s.ttft.Load()
	if ttft == 0 {
		ttft = elapsed
	}
	total := result.TotalTokens
	if total <= 0 {
		total = s.info.InputTokens + result.OutputTokens
	}
	record := Record{
		ID:        s.id,
		Source:    s.info.Source,
		Provider:  "venice",
		Model:     s.info.Model,
		Effort:    s.info.Effort,
		Tier:      s.info.Tier,
		Status:    statusText(result.Success),
		Success:   result.Success,
		Error:     strings.TrimSpace(result.Error),
		StartedAt: s.started,
		EndedAt:   ended,
		ElapsedMS: elapsed,
		TTFTMS:    ttft,
		Usage: Usage{
			InputTokens:  s.info.InputTokens,
			OutputTokens: result.OutputTokens,
			TotalTokens:  total,
		},
	}
	state.Lock()
	defer state.Unlock()
	state.records = append(state.records, record)
	if len(state.records) > maxRecords {
		state.records = append([]Record(nil), state.records[len(state.records)-maxRecords:]...)
	}
}

func SnapshotRows(limit int, failuresOnly bool, masked bool) Snapshot {
	state.Lock()
	records := append([]Record(nil), state.records...)
	state.Unlock()
	if limit <= 0 || limit > maxRecords {
		limit = maxRecords
	}
	if len(records) > limit {
		records = records[len(records)-limit:]
	}
	sourceSet := map[string]bool{}
	failures := 0
	rows := make([]Row, 0, len(records))
	for i := len(records) - 1; i >= 0; i-- {
		record := records[i]
		if record.Success {
			if failuresOnly {
				continue
			}
		} else {
			failures++
		}
		sourceSet[record.Source] = true
		rows = append(rows, rowFromRecord(record, masked))
	}
	return Snapshot{
		Provider: "venice",
		Summary: Summary{
			Rows:     len(rows),
			Failures: failures,
			Accounts: len(sourceSet),
		},
		Rows: rows,
	}
}

func ResetForTest() {
	state.Lock()
	defer state.Unlock()
	state.records = nil
	atomic.StoreUint64(&seq, 0)
}

func rowFromRecord(record Record, masked bool) Row {
	success := int64(0)
	failed := int64(0)
	successRate := 0.0
	if record.Success {
		success = 1
		successRate = 100
	} else {
		failed = 1
	}
	tps := 0.0
	if record.ElapsedMS > 0 {
		tps = float64(record.Usage.TotalTokens) / (float64(record.ElapsedMS) / 1000)
	}
	detail := record
	if masked {
		detail.Source = maskSource(detail.Source)
	}
	return Row{
		ID:          record.ID,
		Source:      chooseSource(record.Source, masked),
		Provider:    "venice",
		Model:       record.Model,
		Effort:      record.Effort,
		Tier:        record.Tier,
		Recent:      []bool{record.Success},
		Status:      record.Status,
		SuccessRate: successRate,
		Calls:       1,
		Success:     success,
		Failed:      failed,
		TPS:         tps,
		TTFTMS:      record.TTFTMS,
		ElapsedMS:   record.ElapsedMS,
		Time:        record.EndedAt.Format(time.RFC3339Nano),
		Usage:       record.Usage,
		Cost:        "-",
		Error:       record.Error,
		Details:     []Record{detail},
	}
}

func chooseSource(source string, masked bool) string {
	if masked {
		return maskSource(source)
	}
	return source
}

func maskSource(source string) string {
	source = strings.TrimSpace(source)
	if source == "" {
		return "Venice account"
	}
	at := strings.IndexByte(source, '@')
	if at <= 2 {
		if at > 0 {
			return source[:1] + "***" + source[at:]
		}
		return "***"
	}
	return source[:3] + "***" + source[at:]
}

func statusText(success bool) string {
	if success {
		return "Success"
	}
	return "Failed"
}
