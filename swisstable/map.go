package swisstable

import "hash/maphash"

const (
	maxLoadFactor = float32(maxAvgGroupLoad) / float32(groupSize)
)

// Map is an open-addressing hash map
// based on Abseil's flat_hash_map.
type Map struct {
	ctrl     []metadata
	groups   []group
	resident uint32
	dead     uint32
	limit    uint32
}

// metadata is the h2 metadata array for a group.
// find operations first probe the controls bytes
// to filter candidates before matching keys
type metadata [groupSize]int8

// group is a group of 16 key-value pairs
type group struct {
	keys   [groupSize]string
	values [groupSize]string
}

const (
	h1Mask    uint64 = 0xffff_ffff_ffff_ff80
	h2Mask    uint64 = 0x0000_0000_0000_007f
	empty     int8   = -128 // 0b1000_0000
	tombstone int8   = -2   // 0b1111_1110
)

// h1 is a 57 bit hash prefix
type h1 uint64

// h2 is a 7 bit hash suffix
type h2 int8

// NewMap constructs a Map.
func NewMap(sz uint32) (m *Map) {
	groups := numGroups(sz)
	m = &Map{
		ctrl:   make([]metadata, groups),
		groups: make([]group, groups),
		limit:  groups * maxAvgGroupLoad,
	}
	for i := range m.ctrl {
		m.ctrl[i] = newEmptyMetadata()
	}
	return
}

var seed = maphash.MakeSeed()

func (m *Map) hash(key string) uint64 {
	return maphash.String(seed, key)
}

// Get returns the |value| mapped by |key| if one exists.
func (m *Map) Get(key string) (value string, ok bool) {
	hi, lo := splitHash(m.hash(key))
	g := probeStart(hi, len(m.groups))
	for { // inlined find loop
		matches := metaMatchH2(&m.ctrl[g], lo)
		for matches != 0 {
			s := nextMatch(&matches)
			if key == m.groups[g].keys[s] {
				value, ok = m.groups[g].values[s], true
				return
			}
		}
		// |key| is not in group |g|,
		// stop probing if we see an empty slot
		matches = metaMatchEmpty(&m.ctrl[g])
		if matches != 0 {
			ok = false
			return
		}
		g += 1 // linear probing
		if g >= uint32(len(m.groups)) {
			g = 0
		}
	}
}

// Put attempts to insert |key| and |value|
func (m *Map) Put(key, value string) {
	if m.resident >= m.limit {
		m.rehash(m.nextSize())
	}
	hi, lo := splitHash(m.hash(key))
	g := probeStart(hi, len(m.groups))
	for { // inlined find loop
		matches := metaMatchH2(&m.ctrl[g], lo)
		for matches != 0 {
			s := nextMatch(&matches)
			if key == m.groups[g].keys[s] { // update
				m.groups[g].keys[s] = key
				m.groups[g].values[s] = value
				return
			}
		}
		// |key| is not in group |g|,
		// stop probing if we see an empty slot
		matches = metaMatchEmpty(&m.ctrl[g])
		if matches != 0 { // insert
			s := nextMatch(&matches)
			m.groups[g].keys[s] = key
			m.groups[g].values[s] = value
			m.ctrl[g][s] = int8(lo)
			m.resident++
			return
		}
		g += 1 // linear probing
		if g >= uint32(len(m.groups)) {
			g = 0
		}
	}
}

// find returns the location of |key| if present, or its insertion location if absent.
// for performance, find is manually inlined into public methods.
func (m *Map) find(key string, hi h1, lo h2) (g, s uint32, ok bool) {
	g = probeStart(hi, len(m.groups))
	for {
		matches := metaMatchH2(&m.ctrl[g], lo)
		for matches != 0 {
			s = nextMatch(&matches)
			if key == m.groups[g].keys[s] {
				return g, s, true
			}
		}
		// |key| is not in group |g|,
		// stop probing if we see an empty slot
		matches = metaMatchEmpty(&m.ctrl[g])
		if matches != 0 {
			s = nextMatch(&matches)
			return g, s, false
		}
		g += 1 // linear probing
		if g >= uint32(len(m.groups)) {
			g = 0
		}
	}
}

func (m *Map) nextSize() (n uint32) {
	n = uint32(len(m.groups)) * 2
	if m.dead >= (m.resident / 2) {
		n = uint32(len(m.groups))
	}
	return
}

func (m *Map) rehash(n uint32) {
	groups, ctrl := m.groups, m.ctrl
	m.groups = make([]group, n)
	m.ctrl = make([]metadata, n)
	for i := range m.ctrl {
		m.ctrl[i] = newEmptyMetadata()
	}
	m.limit = n * maxAvgGroupLoad
	m.resident, m.dead = 0, 0
	for g := range ctrl {
		for s := range ctrl[g] {
			c := ctrl[g][s]
			if c == empty || c == tombstone {
				continue
			}
			m.Put(groups[g].keys[s], groups[g].values[s])
		}
	}
}

func (m *Map) loadFactor() float32 {
	slots := float32(len(m.groups) * groupSize)
	return float32(m.resident-m.dead) / slots
}

// numGroups returns the minimum number of groups needed to store |n| elems.
func numGroups(n uint32) (groups uint32) {
	groups = (n + maxAvgGroupLoad - 1) / maxAvgGroupLoad
	if groups == 0 {
		groups = 1
	}
	return
}

func newEmptyMetadata() (meta metadata) {
	for i := range meta {
		meta[i] = empty
	}
	return
}

func splitHash(h uint64) (h1, h2) {
	return h1((h & h1Mask) >> 7), h2(h & h2Mask)
}

func probeStart(hi h1, groups int) uint32 {
	return fastModN(uint32(hi), uint32(groups))
}

// lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
func fastModN(x, n uint32) uint32 {
	return uint32((uint64(x) * uint64(n)) >> 32)
}

// randIntN returns a random number in the interval [0, n).
func randIntN(n int) uint32 {
	return fastModN(fastrand(), uint32(n))
}

