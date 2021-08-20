package api

import "github.com/kubemq-io/kubemq-community/config"

var host = config.Host()

type Status struct {
	Host     string        `json:"host"`
	System   *System       `json:"system"`
	Entities EntitiesGroup `json:"entities"`
}

func NewStatus() *Status {
	return &Status{
		Host: host,
	}
}
func (s *Status) SetSystem(value *System) *Status {
	s.System = value
	return s
}

func (s *Status) SetEntities(value EntitiesGroup) *Status {
	s.Entities = value
	return s
}

type EntitiesGroup map[string]*Group

func NewEntitiesGroup() EntitiesGroup {
	return map[string]*Group{}
}

func (eg EntitiesGroup) GroupEntities(family string, entities map[string]*Entity) EntitiesGroup {
	group, ok := eg[family]
	if !ok {
		group = NewGroup()
		eg[family] = group
	}
	group.SetTotal(len(entities))
	for _, entity := range entities {
		group.SetValues("send", entity.In)
		group.SetValues("receive", entity.Out)
	}
	return eg
}
