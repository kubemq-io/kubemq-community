package authorization

import (
	"fmt"
)

type MemoryProvider struct {
	data []byte
	acl  AccessControlList
}

func (m *MemoryProvider) GetPolicy() string {

	return m.acl.RuleSet()
}
func (m *MemoryProvider) Load() error {
	var err error
	m.acl, err = UnmarshalAccessControlList(m.data)
	if err != nil {
		return fmt.Errorf("error unmarshaling access control list file: %s", err.Error())
	}
	err = m.acl.Validate()
	if err != nil {
		return fmt.Errorf("access control list validation error: %s", err.Error())
	}
	return validatePolicy(m.acl.RuleSet())
}

func NewMemoryProvider(data []byte) *MemoryProvider {
	return &MemoryProvider{
		data: data,
	}
}
