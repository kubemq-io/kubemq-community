package authorization

type Provider interface {
	GetPolicy() string
	Load() error
}
