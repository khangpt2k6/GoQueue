package consumer

import "sync"

// Manager stores committed offsets for consumer groups in-memory.
// It is intentionally simple for now; persistence can be added later.
type Manager struct {
	mu      sync.RWMutex
	offsets map[string]int64
}

func NewManager() *Manager {
	return &Manager{
		offsets: make(map[string]int64),
	}
}

func key(topic, group string) string {
	return topic + "::" + group
}

func (m *Manager) Get(topic, group string) (int64, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, ok := m.offsets[key(topic, group)]
	return v, ok
}

func (m *Manager) Commit(topic, group string, offset int64) {
	m.mu.Lock()
	m.offsets[key(topic, group)] = offset
	m.mu.Unlock()
}
