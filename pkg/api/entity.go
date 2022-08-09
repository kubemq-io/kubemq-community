package api

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

type Entity struct {
	Time int64       `json:"time"`
	Type string      `json:"type"`
	Name string      `json:"name"`
	In   *BaseValues `json:"in"`
	Out  *BaseValues `json:"out"`
}

func NewEntity(_type, name string) *Entity {
	return &Entity{
		Time: time.Now().UTC().Unix(),
		Type: _type,
		Name: name,
		In:   NewBaseValues(),
		Out:  NewBaseValues(),
	}
}

func (e *Entity) SetIn(value *BaseValues) *Entity {
	e.In = value
	return e
}
func (e *Entity) SetOut(value *BaseValues) *Entity {
	e.Out = value
	return e
}
func (e *Entity) getBaseValues(side string) *BaseValues {
	if side == "send" {
		return e.In
	}
	return e.Out

}
func (e *Entity) SetValues(side, kind string, value int64) *Entity {
	if side == "send" {
		switch kind {
		case "messages":
			e.In.Messages += value
		case "volume":
			e.In.Volume += value
		case "errors":
			e.In.Errors += value
		case "waiting":
			e.In.SetWaiting(value)
		}
	} else {
		switch kind {
		case "messages":
			e.Out.Messages += value
		case "volume":
			e.Out.Volume += value
		case "errors":
			e.Out.Errors += value
		case "waiting":
			e.Out.SetWaiting(value)
		}
	}
	return e
}
func (e *Entity) SetClient(side, value string) *Entity {
	if side == "send" {
		e.In.AddClient(value)
	} else {
		e.Out.AddClient(value)
	}
	return e
}

func (e *Entity) Diff(other *Entity) *Entity {
	newEntity := NewEntity(e.Type, e.Name)
	newEntity.Time = e.Time
	newEntity.In = e.In.Diff(other.In)
	newEntity.Out = e.Out.Diff(other.Out)
	return newEntity
}
func (e *Entity) IsEqual(other *Entity) bool {
	return e.In.IsEqual(other.In) && e.Out.IsEqual(other.Out)
}
func (e *Entity) Key() string {
	return fmt.Sprintf("%s-%s-%d", e.Type, e.Name, e.Time)
}
func (e *Entity) Bytes() []byte {
	data, _ := json.Marshal(e)
	return data
}
func (e *Entity) ToBinary() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(e); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
func EntityFromBinary(data []byte) (*Entity, error) {
	e := &Entity{}
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(e); err != nil {
		return nil, err
	}
	return e, nil
}
func ParseEntity(data []byte) (*Entity, error) {
	var entity *Entity
	err := json.Unmarshal(data, &entity)
	return entity, err
}
func (e *Entity) String() string {
	data, _ := json.Marshal(e)
	return string(data)
}

func Aggregate(entities []*Entity) *Entity {
	var entity *Entity
	if len(entities) > 0 {
		entity = entities[0]
	} else {
		entity = NewEntity("", "")
	}
	sort.Slice(entities, func(i, j int) bool {
		return entities[i].Time < entities[j].Time
	})
	for _, e := range entities {
		entity.In.AddValues(e.In)
		entity.Out.AddValues(e.Out)
	}
	return entity
}
