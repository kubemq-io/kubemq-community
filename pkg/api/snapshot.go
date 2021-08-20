package api

import "time"

type Snapshot struct {
	Pk       int      `json:"-" storm:"id,increment"`
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
