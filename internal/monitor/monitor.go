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
	ClientKey   string
	ClientHash  string
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
	ID         string    `json:"id"`
	Source     string    `json:"source"`
	ClientKey  string    `json:"client_key,omitempty"`
	ClientHash string    `json:"client_key_hash,omitempty"`
	Provider   string    `json:"provider"`
	Model      string    `json:"model"`
	Effort     string    `json:"effort"`
	Tier       string    `json:"tier"`
	Status     string    `json:"status"`
	Success    bool      `json:"success"`
	Error      string    `json:"error,omitempty"`
	StartedAt  time.Time `json:"started_at"`
	EndedAt    time.Time `json:"ended_at"`
	ElapsedMS  int64     `json:"elapsed_ms"`
	TTFTMS     int64     `json:"ttft_ms"`
	Usage      Usage     `json:"usage"`
}

type Row struct {
	ID          string   `json:"id"`
	Source      string   `json:"source"`
	ClientKey   string   `json:"client_key,omitempty"`
	ClientHash  string   `json:"client_key_hash,omitempty"`
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
	Total    int `json:"total"`
	Page     int `json:"page"`
	PageSize int `json:"page_size"`
	Pages    int `json:"pages"`
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
		info:    RequestInfo{Source: source, ClientKey: strings.TrimSpace(info.ClientKey), ClientHash: strings.TrimSpace(info.ClientHash), Model: model, Effort: effort, Tier: tier, InputTokens: info.InputTokens},
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
		ID:         s.id,
		Source:     s.info.Source,
		ClientKey:  s.info.ClientKey,
		ClientHash: s.info.ClientHash,
		Provider:   "venice",
		Model:      s.info.Model,
		Effort:     s.info.Effort,
		Tier:       s.info.Tier,
		Status:     statusText(result.Success),
		Success:    result.Success,
		Error:      strings.TrimSpace(result.Error),
		StartedAt:  s.started,
		EndedAt:    ended,
		ElapsedMS:  elapsed,
		TTFTMS:     ttft,
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
	return SnapshotPage(limit, 1, failuresOnly, masked)
}

func SnapshotPage(pageSize int, page int, failuresOnly bool, masked bool) Snapshot {
	state.Lock()
	records := append([]Record(nil), state.records...)
	state.Unlock()
	if pageSize <= 0 || pageSize > maxRecords {
		pageSize = maxRecords
	}
	if page <= 0 {
		page = 1
	}
	filtered := make([]Record, 0, len(records))
	for _, record := range records {
		if record.Success && failuresOnly {
			continue
		}
		filtered = append(filtered, record)
	}
	total := len(filtered)
	pages := 0
	if total > 0 {
		pages = (total + pageSize - 1) / pageSize
	}
	if pages > 0 && page > pages {
		page = pages
	}
	start := total - page*pageSize
	if start < 0 {
		start = 0
	}
	end := total - (page-1)*pageSize
	if end < start {
		end = start
	}
	sourceSet := map[string]bool{}
	failures := 0
	for _, record := range filtered {
		if !record.Success {
			failures++
		}
		sourceSet[record.Source] = true
	}
	rows := make([]Row, 0, end-start)
	for i := end - 1; i >= start; i-- {
		if i < 0 || i >= len(filtered) {
			continue
		}
		record := filtered[i]
		rows = append(rows, rowFromRecord(record, masked))
	}
	return Snapshot{
		Provider: "venice",
		Summary: Summary{
			Rows:     len(rows),
			Failures: failures,
			Accounts: len(sourceSet),
			Total:    total,
			Page:     page,
			PageSize: pageSize,
			Pages:    pages,
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
		detail.ClientKey = maskKey(detail.ClientKey)
		detail.ClientHash = maskKey(detail.ClientHash)
	}
	return Row{
		ID:          record.ID,
		Source:      chooseSource(record.Source, masked),
		ClientKey:   chooseKey(record.ClientKey, masked),
		ClientHash:  chooseHash(record.ClientHash, masked),
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

func chooseKey(key string, masked bool) string {
	key = strings.TrimSpace(key)
	if key == "" {
		return "unknown"
	}
	if masked {
		return maskKey(key)
	}
	return key
}

func chooseHash(hash string, masked bool) string {
	hash = strings.TrimSpace(hash)
	if hash == "" {
		return ""
	}
	return hash
}

func maskKey(key string) string {
	key = strings.TrimSpace(key)
	if key == "" {
		return "unknown"
	}
	if strings.Contains(key, "@") {
		return maskSource(key)
	}
	if strings.HasPrefix(strings.ToLower(key), "bearer ") {
		key = strings.TrimSpace(key[7:])
	}
	if len(key) <= 8 {
		return "***"
	}
	return key[:4] + "***" + key[len(key)-4:]
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
