package settings

import (
	"encoding/json"
	"strings"
	"sync"
)

const (
	FileName = "cpa-plugin-venice-settings.json"
	Type     = "cpa-plugin-venice-settings"
)

type Config struct {
	ToolRepairEnabled bool `json:"tool_repair_enabled"`
}

type storage struct {
	Type string `json:"type"`
	Config
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
