package main

import (
	listener "go-queue/listeners"
	connectionsmanager "go-queue/managers/connectionsManager"
	"go-queue/managers/jobsManager"
	"go-queue/managers/listenersManager"
	"go-queue/providers"
	"log"

	"github.com/joho/godotenv"
)

func main() {
	envVariables, err := godotenv.Read()
	failOnError(err, "Error to get params in env file: ")

	connManager := connectionsmanager.Manager{Env: envVariables}

	err = connManager.GraylogHook()
	failOnError(err, "Error to create GraylogHook")

	connManager.SetDatabaseClients()

	lstnManager := listenersManager.ListenerManager{
		Providers:   providers.GetAllJobs(),
		Listeners:   listener.Listener{},
		ConnManager: &connManager,
		JobsManager: &jobsManager.Manager{},
	}
	lstnManager.RunListeners()
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
