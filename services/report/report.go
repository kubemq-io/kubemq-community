package report

type Report struct {
	Fingerprint   string `json:"fingerprint,omitempty"`
	OriginHost    string `json:"origin_host,omitempty" `
	OriginEnv     string `json:"origin_env,omitempty"`
	Messages      int64  `json:"messages,omitempty"`
	Volume        int64  `json:"volume,omitempty"`
	UpdateCounter int64  `json:"update_counter,omitempty"`
	StartCounter  int64  `json:"start_counter,omitempty"`
	Version       string `json:"version,omitempty"`
}

func NewReport() *Report {
	return &Report{}
}

func (t *Report) SetFingerprint(value string) *Report {
	t.Fingerprint = value
	return t
}

func (t *Report) SetOriginHost(value string) *Report {
	t.OriginHost = value
	return t
}

func (t *Report) SetOriginEnv(value string) *Report {
	t.OriginEnv = value
	return t
}

func (t *Report) SetMessages(value int64) *Report {
	t.Messages = value
	return t
}

func (t *Report) SetVolume(value int64) *Report {
	t.Volume = value
	return t
}

func (t *Report) SetUpdateCounter(value int64) *Report {
	t.UpdateCounter = value
	return t
}

func (t *Report) SetStartCounter(value int64) *Report {
	t.StartCounter = value
	return t
}

func (t *Report) SetVersion(value string) *Report {
	t.Version = value
	return t
}
