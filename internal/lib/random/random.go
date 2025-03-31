package random

import "math/rand"

func NewRandomString(aliasLength int) string {
	symbols := []rune("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789")
	
	alias := make([]rune, aliasLength)
	for idx := range alias {
		alias[idx] = symbols[rand.Intn(len(symbols))]
	}

	return string(alias)
}
