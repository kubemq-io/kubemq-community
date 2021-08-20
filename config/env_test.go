package config

import "testing"

func Test_convertEnvFormat(t *testing.T) {

	tests := []struct {
		name string
		str  string
		want string
	}{
		{
			name: "test",
			str:  "Foo.Bar.OneOfMany",
			want: "FOO_BAR_ONE_OF_MANY",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := convertEnvFormat(tt.str); got != tt.want {
				t.Errorf("convertEnvFormat() = %v, want %v", got, tt.want)
			}
		})
	}
}
