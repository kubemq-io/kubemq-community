package api

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"sort"
	"time"
)

type EntitiesGroup struct {
	Created  int64                      `json:"created"`
	LastSeen int64                      `json:"last_seen"`
	Families map[string]*EntitiesFamily `json:"entities"`
}

func NewEntitiesGroup() *EntitiesGroup {
	return &EntitiesGroup{
		Created:  time.Now().UTC().UnixMilli(),
		LastSeen: 0,
		Families: map[string]*EntitiesFamily{},
	}
}

func (e *EntitiesGroup) GetEntity(family, name string) (*Entity, bool) {
	enFamily, ok := e.Families[family]
	if ok {
		en, found := enFamily.Entities[name]
		return en, found
	}
	return nil, ok
}

func (e *EntitiesGroup) GetFamily(family string) (*EntitiesFamily, bool) {
	enFamily, ok := e.Families[family]
	return enFamily, ok
}
func (e *EntitiesGroup) AddEntity(family string, entity *Entity) {
	enFamily, ok := e.Families[family]
	if ok {
		enFamily.AddEntity(entity)
	} else {
		enFamily = NewEntitiesFamily(family)
		enFamily.AddEntity(entity)
	}
	e.Families[family] = enFamily
	if entity.LastSeen > e.LastSeen {
		e.LastSeen = entity.LastSeen
	}
}
func (e *EntitiesGroup) List() []*Entity {
	var list []*Entity
	for _, enFamily := range e.Families {
		for _, en := range enFamily.List() {
			list = append(list, en)
		}
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].LastSeen > list[j].LastSeen
	})
	return list
}

func (e *EntitiesGroup) ToBinary() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(e); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
func EntitiesGroupFromBinary(data []byte) (*EntitiesGroup, error) {
	e := NewEntitiesGroup()
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&e); err != nil {
		return nil, err
	}
	e.ReCalcLastSeen()
	return e, nil
}
func (e *EntitiesGroup) Clone() *EntitiesGroup {
	data, _ := e.ToBinary()
	newEntities, _ := EntitiesGroupFromBinary(data)
	return newEntities
}

func (e *EntitiesGroup) Merge(other *EntitiesGroup) *EntitiesGroup {
	if e.LastSeen < other.LastSeen {
		e.LastSeen = other.LastSeen
		for family, enFamily := range other.Families {
			currentEnFamily, ok := e.Families[family]
			if !ok {
				e.Families[family] = enFamily
			} else {
				currentEnFamily.Merge(enFamily)
			}
		}
	}
	return e
}

func (e *EntitiesGroup) Key() string {
	return time.UnixMilli(e.Created).Format(time.RFC3339)
}

func (e *EntitiesGroup) String() string {
	data, _ := json.MarshalIndent(e, "", "  ")
	return string(data)
}
func (e *EntitiesGroup) ReCalcLastSeen() {
	lastSeen := int64(0)
	for _, family := range e.Families {
		family.ReCalcLastSeen()
		if family.LastSeen > lastSeen {
			lastSeen = family.LastSeen
		}
	}
	e.LastSeen = lastSeen
}
