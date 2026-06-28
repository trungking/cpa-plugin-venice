package monitor

import (
	"fmt"
	"sort"
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
	groups := map[string][]Record{}
	sourceSet := map[string]bool{}
	failures := 0
	for _, record := range records {
		if record.Success {
			if failuresOnly {
				continue
			}
		} else {
			failures++
		}
		sourceSet[record.Source] = true
		key := strings.Join([]string{record.Source, record.Model, record.Effort, record.Tier}, "\x00")
		groups[key] = append(groups[key], record)
	}
	rows := make([]Row, 0, len(groups))
	for _, group := range groups {
		rows = append(rows, rowFromRecords(group, masked))
	}
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].Time > rows[j].Time
	})
	return Snapshot{
		Provider: "venice",
		Summary: Summary{
			Rows:     len(records),
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

func rowFromRecords(records []Record, masked bool) Row {
	sort.Slice(records, func(i, j int) bool {
		return records[i].StartedAt.Before(records[j].StartedAt)
	})
	first := records[0]
	last := records[len(records)-1]
	var success, failed, totalElapsed, totalTokens, totalInput, totalOutput, ttft int64
	for _, record := range records {
		if record.Success {
			success++
		} else {
			failed++
		}
		totalElapsed += record.ElapsedMS
		totalTokens += record.Usage.TotalTokens
		totalInput += record.Usage.InputTokens
		totalOutput += record.Usage.OutputTokens
		ttft = record.TTFTMS
	}
	calls := success + failed
	elapsed := totalElapsed / maxInt64(1, calls)
	successRate := 0.0
	if calls > 0 {
		successRate = float64(success) / float64(calls) * 100
	}
	tps := 0.0
	if totalElapsed > 0 {
		tps = float64(totalTokens) / (float64(totalElapsed) / 1000)
	}
	details := append([]Record(nil), records...)
	if len(details) > 5 {
		details = details[len(details)-5:]
	}
	if masked {
		for i := range details {
			details[i].Source = maskSource(details[i].Source)
		}
	}
	return Row{
		Source:      chooseSource(first.Source, masked),
		Provider:    "venice",
		Model:       first.Model,
		Effort:      first.Effort,
		Tier:        first.Tier,
		Recent:      recent(records, 5),
		Status:      last.Status,
		SuccessRate: successRate,
		Calls:       calls,
		Success:     success,
		Failed:      failed,
		TPS:         tps,
		TTFTMS:      ttft,
		ElapsedMS:   elapsed,
		Time:        last.EndedAt.Format(time.RFC3339),
		Usage: Usage{
			InputTokens:  totalInput,
			OutputTokens: totalOutput,
			TotalTokens:  totalTokens,
		},
		Cost:    "-",
		Details: details,
	}
}

func recent(records []Record, count int) []bool {
	if len(records) > count {
		records = records[len(records)-count:]
	}
	out := make([]bool, 0, count)
	for _, record := range records {
		out = append(out, record.Success)
	}
	return out
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

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
