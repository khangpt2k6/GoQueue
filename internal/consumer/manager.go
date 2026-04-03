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

func key(topic, group string, partition int) string {
	return topic + "::" + group + "::p" + itoa(partition)
}

func (m *Manager) Get(topic, group string) (int64, bool) {
	return m.GetPartition(topic, group, 0)
}

func (m *Manager) GetPartition(topic, group string, partition int) (int64, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, ok := m.offsets[key(topic, group, partition)]
	return v, ok
}

func (m *Manager) Commit(topic, group string, offset int64) {
	m.CommitPartition(topic, group, 0, offset)
}

func (m *Manager) CommitPartition(topic, group string, partition int, offset int64) {
	m.mu.Lock()
	m.offsets[key(topic, group, partition)] = offset
	m.mu.Unlock()
}

func itoa(v int) string {
	if v == 0 {
		return "0"
	}
	sign := ""
	if v < 0 {
		sign = "-"
		v = -v
	}
	buf := [20]byte{}
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return sign + string(buf[i:])
}
