package authorization

import (
	"fmt"
	"io/ioutil"
)

type FileProvider struct {
	path string
	acl  AccessControlList
}

func (f *FileProvider) GetPolicy() string {
	return f.acl.RuleSet()
}
func (f *FileProvider) Load() error {
	data, err := ioutil.ReadFile(f.path)
	if err != nil {
		return fmt.Errorf("error loading access contorl list file: %s", err.Error())
	}
	f.acl, err = UnmarshalAccessControlList(data)
	if err != nil {
		return fmt.Errorf("error unmarshaling access contorl list file: %s", err.Error())
	}

	err = f.acl.Validate()
	if err != nil {
		return fmt.Errorf("access control list validation error: %s", err.Error())
	}
	return validatePolicy(f.acl.RuleSet())

}

func NewFileProvider(path string) *FileProvider {
	return &FileProvider{
		path: path,
	}
}
