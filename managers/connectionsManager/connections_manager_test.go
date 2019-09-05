package connectionsmanager

import "testing"

func TestGetJobDatabaseManagers(t *testing.T) {
	jobsConnections := []string{"mysql", "mongo", "redis"}
	dbClients := make(map[string]interface{})

	dbClients["mysql"] = "teste"
	dbClients["mongo"] = "teste"
	dbClients["redis"] = "teste"

	connManager := Manager{DBClients: dbClients}
	returnedConnections := connManager.GetJobDatabaseManagers(jobsConnections)

	if len(returnedConnections) != 3 {
		t.Errorf("Expected retuned 3 connections but got %v", len(returnedConnections))
	}

	for _, value := range returnedConnections {
		if value.(string) != "teste" {
			t.Errorf("The connections content is diferent from expected")
		}
	}
}
