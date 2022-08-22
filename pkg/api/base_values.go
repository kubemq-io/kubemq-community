package api

type BaseValues struct {
	Messages  int64             `json:"messages"`
	Volume    int64             `json:"volume"`
	Errors    int64             `json:"errors"`
	Waiting   int64             `json:"waiting"`
	Clients   int64             `json:"clients"`
	LastSeen  int64             `json:"last_seen"`
	ClientMap map[string]string `json:"client_map"`
	Expired   int64             `json:"expired"`
	Delayed   int64             `json:"delayed"`
}

func NewBaseValues() *BaseValues {
	return &BaseValues{
		Messages:  0,
		Volume:    0,
		Errors:    0,
		Waiting:   0,
		Clients:   0,
		LastSeen:  0,
		Expired:   0,
		Delayed:   0,
		ClientMap: make(map[string]string),
	}
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
func (b *BaseValues) SetLastSeen(value int64) *BaseValues {
	b.LastSeen = value
	return b
}
func (b *BaseValues) SetExpired(value int64) *BaseValues {
	b.Expired = value
	return b
}

func (b *BaseValues) SetDelayed(value int64) *BaseValues {
	b.Delayed = value
	return b
}

func (b *BaseValues) AddClient(value string) *BaseValues {
	b.ClientMap[value] = value
	b.Clients = int64(len(b.ClientMap))
	return b
}

func (b *BaseValues) Add(value *BaseValues) *BaseValues {
	b.Messages += value.Messages
	b.Volume += value.Volume
	b.Errors += value.Errors
	b.Waiting += value.Waiting
	b.Expired += value.Expired
	b.Delayed += value.Delayed
	for k, v := range value.ClientMap {
		b.ClientMap[k] = v
	}
	b.Clients = int64(len(b.ClientMap))
	if value.LastSeen > b.LastSeen {
		b.LastSeen = value.LastSeen
	}
	return b
}

func (b *BaseValues) Merge(other *BaseValues) *BaseValues {
	b.Messages += other.Messages
	b.Volume += other.Volume
	b.Errors += other.Errors
	b.Waiting += other.Waiting
	b.Expired += other.Expired
	b.Delayed += other.Delayed
	for k, v := range other.ClientMap {
		b.ClientMap[k] = v
	}
	b.Clients = int64(len(b.ClientMap))
	if other.LastSeen > b.LastSeen {
		b.LastSeen = other.LastSeen
	}
	return b
}

func (b *BaseValues) CombineWIth(other *BaseValues) *BaseValues {
	newBase := NewBaseValues()
	newBase.LastSeen = b.LastSeen
	newBase.Messages = b.Messages + other.Messages
	newBase.Volume = b.Volume + other.Volume
	newBase.Errors = b.Errors + other.Errors
	newBase.Waiting = b.Waiting + other.Waiting
	newBase.Expired = b.Expired + other.Expired
	newBase.Delayed = b.Delayed + other.Delayed
	if other.LastSeen > b.LastSeen {
		newBase.LastSeen = other.LastSeen
	}
	for k, v := range b.ClientMap {
		newBase.ClientMap[k] = v
	}
	for k, v := range other.ClientMap {
		newBase.ClientMap[k] = v
	}
	newBase.Clients = int64(len(newBase.ClientMap))
	return newBase
}
