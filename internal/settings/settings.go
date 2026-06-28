package settings

import (
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	FileName = "cpa-plugin-venice-settings.json"
	Type     = "cpa-plugin-venice-settings"
)

type Config struct {
	ToolRepairEnabled bool `json:"tool_repair_enabled"`
}

type Stats struct {
	ToolRepairApplied   int64 `json:"tool_repair_applied"`
	ToolCallConversions int64 `json:"tool_call_conversions"`
	ToolCallsEmitted    int64 `json:"tool_calls_emitted"`
}

type storage struct {
	Type string `json:"type"`
	Config
}

var stats struct {
	toolRepairApplied   atomic.Int64
	toolCallConversions atomic.Int64
	toolCallsEmitted    atomic.Int64
}

var current = struct {
	sync.RWMutex
	config Config
}{}

func Get() Config {
	current.RLock()
	defer current.RUnlock()
	return current.config
}

func Set(config Config) {
	current.Lock()
	current.config = config
	current.Unlock()
}

func SnapshotStats() Stats {
	return Stats{
		ToolRepairApplied:   stats.toolRepairApplied.Load(),
		ToolCallConversions: stats.toolCallConversions.Load(),
		ToolCallsEmitted:    stats.toolCallsEmitted.Load(),
	}
}

func MarkToolRepairApplied() {
	stats.toolRepairApplied.Add(1)
}

func MarkToolCallConversion(calls int) {
	stats.toolCallConversions.Add(1)
	if calls > 0 {
		stats.toolCallsEmitted.Add(int64(calls))
	}
}

func ResetStatsForTest() {
	stats.toolRepairApplied.Store(0)
	stats.toolCallConversions.Store(0)
	stats.toolCallsEmitted.Store(0)
}

func Parse(raw []byte) (Config, bool) {
	if len(raw) == 0 {
		return Config{}, false
	}
	var decoded storage
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return Config{}, false
	}
	if !strings.EqualFold(strings.TrimSpace(decoded.Type), Type) {
		return Config{}, false
	}
	return decoded.Config, true
}

func Marshal(config Config) json.RawMessage {
	raw, _ := json.Marshal(storage{Type: Type, Config: config})
	return raw
}
