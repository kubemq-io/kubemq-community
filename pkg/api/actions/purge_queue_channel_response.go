package actions

type PurgeQueueChannelResponse struct {
	Count uint64 `json:"count"`
}

func NewPurgeQueueChannelResponse() *PurgeQueueChannelResponse {
	return &PurgeQueueChannelResponse{
		Count: 0,
	}
}
func (r *PurgeQueueChannelResponse) SetCount(count uint64) *PurgeQueueChannelResponse {
	r.Count = count
	return r
}
