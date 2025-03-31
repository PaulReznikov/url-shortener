package random

import "testing"

func TestNewRandomString(t *testing.T) {
	type args struct {
		aliasLength int
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewRandomString(tt.args.aliasLength); got != tt.want {
				t.Errorf("NewRandomString() = %v, want %v", got, tt.want)
			}
		})
	}
}
