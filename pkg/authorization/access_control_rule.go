package authorization

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

type AccessControlRule struct {
	Events      bool
	EventsStore bool
	Commands    bool
	Queries     bool
	Queues      bool
	ClientID    string
	Channel     string
	Read        bool
	Write       bool
}

func (a *AccessControlRule) Rules() []string {

	fields := []string{}
	fields = append(fields, "%s", a.ClientID, a.Channel)
	var action string
	if a.Read && !a.Write {
		action = "read"
	}
	if a.Write && !a.Read {
		action = "write"
	}
	if a.Write && a.Read {
		action = ".*"
	}
	if !a.Write && !a.Read {
		action = "deny"
	}
	fields = append(fields, action)
	rule := strings.Join(fields, ",")
	set := []string{}
	if a.Events {
		set = append(set, fmt.Sprintf(rule, "events"))
	}
	if a.EventsStore {
		set = append(set, fmt.Sprintf(rule, "events_store"))
	}
	if a.Commands {
		set = append(set, fmt.Sprintf(rule, "commands"))
	}
	if a.Queries {
		set = append(set, fmt.Sprintf(rule, "queries"))
	}
	if a.Queues {
		set = append(set, fmt.Sprintf(rule, "queues"))
	}

	return set
}
func (a *AccessControlRule) Validate() error {
	var err error
	if a.ClientID == "" {
		return fmt.Errorf("invalid clinet_id regular expression cannot be empty")
	}
	_, err = regexp.Compile(a.ClientID)
	if err != nil {
		return fmt.Errorf("invalid clinet_id regular expression, error: %s", err.Error())
	}
	if a.Channel == "" {
		return fmt.Errorf("invalid channel regular expression cannot be empty")
	}
	_, err = regexp.Compile(a.Channel)
	if err != nil {
		return fmt.Errorf("invalid channel regular expression, error: %s", err.Error())
	}
	return nil
}

type AccessControlList []*AccessControlRule

func (acl AccessControlList) Validate() error {
	if len(acl) == 0 {
		return fmt.Errorf("empty access control list")
	}
	for i, record := range acl {
		if err := record.Validate(); err != nil {
			return fmt.Errorf("error on rule: %d, error: %s", i, err.Error())
		}
	}
	return nil
}

func (acl AccessControlList) RuleSet() string {
	set := []string{}
	for _, record := range acl {
		set = append(set, record.Rules()...)
	}
	return strings.Join(set, "\n")
}
func (acl AccessControlList) Marshal() []byte {
	data, _ := json.Marshal(acl)
	return data
}

func UnmarshalAccessControlList(data []byte) (AccessControlList, error) {
	buff := data
	base64Buff, err := base64.StdEncoding.DecodeString(string(data))
	if err == nil {
		buff = base64Buff
	}
	acl := AccessControlList{}
	err = json.Unmarshal(buff, &acl)
	return acl, err
}
