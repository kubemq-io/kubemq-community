package api

type Group struct {
	Total  int         `json:"total"`
	Active int         `json:"active"`
	In     *BaseValues `json:"in"`
	Out    *BaseValues `json:"out"`
}

func NewGroup() *Group {
	return &Group{
		Total:  0,
		Active: 0,
		In:     NewBaseValues(),
		Out:    NewBaseValues(),
	}
}

func (g *Group) SetTotal(value int) *Group {
	g.Total = value
	return g
}

func (g *Group) SetActive(value int) *Group {
	g.Active = value
	return g
}

func (g *Group) SetIn(value *BaseValues) *Group {
	g.In = value
	return g
}
func (g *Group) SetOut(value *BaseValues) *Group {
	g.Out = value
	return g
}

func (g *Group) getBaseValues(side string) *BaseValues {
	if side == "send" {
		return g.In
	}
	return g.Out
}

func (g *Group) SetValues(side string, value *BaseValues) {
	if side == "send" {
		g.In = g.In.AddValues(value)
	} else {
		g.Out = g.Out.AddValues(value)
	}
}
