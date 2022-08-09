package api

import (
	"bytes"
	"encoding/gob"
	"sort"
	"time"
)

type Snapshot struct {
	Pk       int      `json:"-"`
	Time     int64    `json:"time"`
	Status   *Status  `json:"status"`
	Entities Entities `json:"entities"`
}

func NewSnapshot() *Snapshot {
	return &Snapshot{
		Pk:       0,
		Time:     time.Now().UTC().Unix(),
		Status:   nil,
		Entities: nil,
	}
}

func (s *Snapshot) SetStatus(value *Status) *Snapshot {
	s.Status = value
	return s
}

func (s *Snapshot) SetEntities(value Entities) *Snapshot {
	s.Entities = value
	return s
}
func (s *Snapshot) ToBinary() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(s)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
func SnapshotFromBinary(data []byte) (*Snapshot, error) {
	s := &Snapshot{}
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(s)
	if err != nil {
		return nil, err
	}
	return s, nil
}

type Entities map[string]map[string]*Entity

func NewEntities() Entities {
	return map[string]map[string]*Entity{}
}

func (e Entities) GetEntity(family, name string) (*Entity, bool) {
	enFamily, ok := e[family]
	if ok {
		en, found := enFamily[name]
		return en, found
	}
	return nil, ok
}

func (e Entities) GetFamily(family string) (map[string]*Entity, bool) {
	enFamily, ok := e[family]
	return enFamily, ok
}
func (e Entities) AddEntity(family string, entity *Entity) {
	enFamily, ok := e[family]
	if ok {
		enFamily[entity.Name] = entity
	} else {
		enFamily = map[string]*Entity{}
		enFamily[entity.Name] = entity
	}
	e[family] = enFamily
}
func (e Entities) List() []*Entity {
	var list []*Entity
	for _, enFamily := range e {
		for _, en := range enFamily {
			list = append(list, en)
		}
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].Time < list[j].Time
	})
	return list
}

func (e Entities) Diff(other Entities) Entities {
	diff := NewEntities()
	for family, enFamily := range e {
		for name, en := range enFamily {
			otherEn, found := other[family][name]
			if found {
				if !en.IsEqual(otherEn) {

				}
				diff[family][name] = en.Diff(otherEn)
			} else {
				diff[family][name] = en
			}
		}
	}
	return diff
}
