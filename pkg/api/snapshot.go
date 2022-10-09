package api

import (
	"bytes"
	"encoding/gob"
	"github.com/kubemq-io/kubemq-community/config"
	"time"
)

type Snapshot struct {
	Pk       int                       `json:"-"`
	Time     int64                     `json:"time"`
	Host     string                    `json:"host"`
	System   *System                   `json:"system"`
	Entities map[string]*EntitiesGroup `json:"entities"`
}

func NewSnapshot() *Snapshot {
	return &Snapshot{
		Pk:       0,
		Time:     time.Now().UTC().UnixMilli(),
		Entities: map[string]*EntitiesGroup{},
		Host:     config.GetHostname(),
	}
}

func (s *Snapshot) SetChannelEntities(value *EntitiesGroup) *Snapshot {
	s.Entities["channels"] = value
	return s
}
func (s *Snapshot) SetClientsEntities(value *EntitiesGroup) *Snapshot {
	s.Entities["clients"] = value
	return s
}
func (s *Snapshot) SetSystem(value *System) *Snapshot {
	s.System = value
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
func (s *Snapshot) Clone() *Snapshot {
	data, _ := s.ToBinary()
	newSnapshot, _ := SnapshotFromBinary(data)
	return newSnapshot
}
