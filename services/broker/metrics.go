package broker

import (
	"strings"
	"time"
)

type channels struct {
	ClusterID string    `json:"cluster_id"`
	ServerID  string    `json:"server_id"`
	Now       time.Time `json:"now"`
	Offset    int64     `json:"offset"`
	Limit     int64     `json:"limit"`
	Count     int64     `json:"count"`
	Total     int64     `json:"total"`
	Channels  []struct {
		Name          string `json:"name"`
		Msgs          int64  `json:"msgs"`
		Bytes         int64  `json:"bytes"`
		FirstSeq      int64  `json:"first_seq"`
		LastSeq       int64  `json:"last_seq"`
		Subscriptions []struct {
			ClientID     string `json:"client_id"`
			Inbox        string `json:"inbox"`
			AckInbox     string `json:"ack_inbox"`
			QueueName    string `json:"queue_name"`
			IsDurable    bool   `json:"is_durable"`
			IsOffline    bool   `json:"is_offline"`
			MaxInflight  int64  `json:"max_inflight"`
			AckWait      int64  `json:"ack_wait"`
			LastSent     int64  `json:"last_sent"`
			PendingCount int64  `json:"pending_count"`
			IsStalled    bool   `json:"is_stalled"`
		} `json:"subscriptions"`
	} `json:"channels"`
}

func (ch *channels) toQueues(chType string) *Queues {
	queues := &Queues{
		Now:         time.Now(),
		TotalQueues: 0,
		Waiting:     0,
		Delivered:   0,
		Queues:      nil,
	}
	if len(ch.Channels) == 0 {
		return queues
	}

	for _, item := range ch.Channels {
		if strings.Contains(item.Name, chType) {
			q := &Queue{
				Name:          strings.Trim(item.Name, chType),
				Messages:      item.Msgs,
				Bytes:         item.Bytes,
				FirstSequence: item.FirstSeq,
				LastSequence:  item.LastSeq,
				clients:       nil,
			}
			if q.FirstSequence <= q.LastSequence {
				for _, sub := range item.Subscriptions {
					c := &Client{
						ClientId:         sub.ClientID,
						Active:           !sub.IsOffline,
						LastSequenceSent: sub.LastSent,
						IsStalled:        sub.IsStalled,
						Pending:          sub.PendingCount,
					}
					q.clients = append(q.clients, c)
				}
				q.Calc()
				queues.Sent += q.Sent
				queues.Delivered += q.Delivered
				queues.Waiting += q.Waiting
				queues.Queues = append(queues.Queues, q)
			}
		}
	}
	queues.TotalQueues = len(queues.Queues)
	return queues
}

type Queue struct {
	Name          string `json:"name"`
	Messages      int64  `json:"messages"`
	Bytes         int64  `json:"bytes"`
	FirstSequence int64  `json:"first_sequence"`
	LastSequence  int64  `json:"last_sequence"`
	Sent          int64  `json:"sent"`
	Subscribers   int    `json:"subscribers"`
	Waiting       int64  `json:"waiting"`
	Delivered     int64  `json:"delivered"`
	clients       []*Client
}

func (q *Queue) Calc() {
	if q.FirstSequence > q.LastSequence {
		return
	}
	q.Sent = q.LastSequence - q.FirstSequence + 1
	if len(q.clients) == 0 {
		q.Waiting = q.LastSequence - q.FirstSequence + 1
		return
	}
	q.Subscribers = len(q.clients)
	maxSequenceSent := int64(0)
	pendingCount := int64(0)

	for _, c := range q.clients {
		if c.LastSequenceSent <= q.LastSequence {
			pendingCount += c.Pending
		}
		if c.LastSequenceSent > maxSequenceSent {
			maxSequenceSent = c.LastSequenceSent
		}
		if c.LastSequenceSent >= q.FirstSequence {
			q.Delivered += c.LastSequenceSent - q.FirstSequence + 1
		}
	}
	q.Waiting = pendingCount + q.LastSequence - maxSequenceSent
}

type Client struct {
	ClientId         string `json:"client_id"`
	Active           bool   `json:"active"`
	LastSequenceSent int64  `json:"last_sequence_sent"`
	IsStalled        bool   `json:"is_stalled"`
	Pending          int64  `json:"pending"`
}

type Queues struct {
	Now         time.Time `json:"now"`
	TotalQueues int       `json:"total_queues"`
	Sent        int64     `json:"sent"`
	Waiting     int64     `json:"waiting"`
	Delivered   int64     `json:"delivered"`
	Queues      []*Queue  `json:"queues"`
}

func (q *Queues) Filter(exp string) *Queues {
	if exp == "" {
		return q
	}
	results := &Queues{
		Now:         time.Now(),
		TotalQueues: 0,
		Sent:        0,
		Waiting:     0,
		Delivered:   0,
		Queues:      nil,
	}
	for _, queue := range q.Queues {
		for _, param := range strings.Split(exp, ";") {
			if queue.Name == param {
				results.Queues = append(results.Queues, queue)
				continue
			}
		}
	}
	for _, queue := range results.Queues {
		results.TotalQueues++
		results.Sent += queue.Sent
		results.Waiting += queue.Waiting
		results.Delivered += queue.Delivered
	}
	return results
}
