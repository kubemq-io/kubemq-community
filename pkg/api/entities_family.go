package api

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"sort"
)

type EntitiesFamily struct {
	LastSeen int64              `json:"last_seen"`
	Name     string             `json:"name"`
	Entities map[string]*Entity `json:"entities"`
}

func NewEntitiesFamily(name string) *EntitiesFamily {
	return &EntitiesFamily{
		LastSeen: 0,
		Name:     name,
		Entities: map[string]*Entity{},
	}
}

func (e *EntitiesFamily) AddEntity(entity *Entity) {
	e.Entities[entity.Name] = entity
	if entity.LastSeen > e.LastSeen {
		e.LastSeen = entity.LastSeen
	}
}
func (e *EntitiesFamily) List() []*Entity {
	var list []*Entity
	for _, en := range e.Entities {
		list = append(list, en)
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].LastSeen > list[j].LastSeen
	})
	return list
}
func (e *EntitiesFamily) GetActiveEntitiesCount() int {
	active := 0
	for _, en := range e.Entities {

		if en.IsActive() {
			active++
		}
	}
	return active
}

func (e *EntitiesFamily) ToBinary() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(e); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
func EntitiesFamilyFromBinary(data []byte) (*EntitiesFamily, error) {
	e := NewEntitiesFamily("")
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&e); err != nil {
		return nil, err
	}
	return e, nil
}
func (e *EntitiesFamily) Clone() *EntitiesFamily {
	data, _ := e.ToBinary()
	newEntities, _ := EntitiesFamilyFromBinary(data)
	return newEntities
}

func (e *EntitiesFamily) Merge(other *EntitiesFamily) *EntitiesFamily {
	if e.LastSeen < other.LastSeen {
		e.LastSeen = other.LastSeen
		for name, entity := range other.Entities {
			currentEntity, ok := e.Entities[name]
			if !ok {
				e.Entities[name] = entity
			} else {
				currentEntity.Merge(entity)
			}
		}
	}
	return e
}
func (e *EntitiesFamily) String() string {
	data, _ := json.MarshalIndent(e, "", "  ")
	return string(data)
}

func (e *EntitiesFamily) ReCalcLastSeen() {
	lastSeen := int64(0)
	for _, en := range e.Entities {
		if en.LastSeen > lastSeen {
			lastSeen = en.LastSeen
		}
	}
	e.LastSeen = lastSeen
}
