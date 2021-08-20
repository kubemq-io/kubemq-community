package api

type BaseValues struct {
	Messages int64 `json:"messages"`
	Volume   int64 `json:"volume"`
	Errors   int64 `json:"errors,omitempty"`
	Waiting  int64 `json:"waiting,omitempty"`
}

func NewBaseValues() *BaseValues {
	return &BaseValues{}
}

func (b *BaseValues) SetMessages(value int64) *BaseValues {
	b.Messages = value
	return b
}
func (b *BaseValues) SetVolume(value int64) *BaseValues {
	b.Volume = value
	return b
}

func (b *BaseValues) SetErrors(value int64) *BaseValues {
	b.Errors = value
	return b
}
func (b *BaseValues) SetWaiting(value int64) *BaseValues {
	b.Waiting = value
	return b
}
