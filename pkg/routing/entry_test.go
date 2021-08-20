package routing

import "testing"

func TestRouteTableEntry_Validate(t *testing.T) {
	type fields struct {
		Key    string
		Routes string
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name:    "good entry",
			fields:  fields{"some-key", "some-route"},
			wantErr: false,
		},
		{
			name:    "empty key",
			fields:  fields{"", "some-route"},
			wantErr: true,
		},
		{
			name:    "empty route",
			fields:  fields{"some-key", ""},
			wantErr: true,
		},
		{
			name:    "bad regex key",
			fields:  fields{"*", "some-route"},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rt := &RouteTableEntry{
				Key:    tt.fields.Key,
				Routes: tt.fields.Routes,
			}
			if err := rt.Validate(); (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
