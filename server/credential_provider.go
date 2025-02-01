package server

import (
	"github.com/go-mysql-org/go-mysql/server"
)

func NewCredentialProvider() server.CredentialProvider {
	inMemProvider := server.NewInMemoryProvider()

	for user, password := range dbUsers() {
		inMemProvider.AddUser(user, password)
	}

	return inMemProvider
}

func dbUsers() map[string]string {
	return map[string]string{
		"root":   "",
		"orange": "",
	}
}
