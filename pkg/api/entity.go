package api

type Entity struct {
	Name string      `json:"name"`
	In   *BaseValues `json:"in"`
	Out  *BaseValues `json:"out"`
}

func NewEntity(name string) *Entity {
	return &Entity{
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
	bv := e.getBaseValues(side)
	switch kind {
	case "messages":
		bv.SetMessages(value)
	case "volume":
		bv.SetVolume(value)
	case "errors":
		bv.SetErrors(value)
	case "waiting":
		bv.SetWaiting(value)
	}
	return e
}
