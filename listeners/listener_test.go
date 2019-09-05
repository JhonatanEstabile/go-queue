package listener

import (
	"errors"
	connectionsmanager "go-queue/managers/connectionsManager"
	"go-queue/managers/jobsManager"
	"go-queue/providers"
	"testing"
	"time"

	"github.com/go-redis/redis"
)

//------------------------- MOCK FUNCTIONS ---------------------
func HandlerTest(paramTest interface{}, paramTestConn map[string]interface{}) error {
	return nil
}

func HandlerTest2(paramTest interface{}, paramTestConn map[string]interface{}) error {
	return errors.New("test")
}

//------------------------- REDIS MOCK ------------------------
type redisClientMock struct {
	rounds int
}

func (r *redisClientMock) LLen(queueName string) *redis.IntCmd {
	err := errors.New("LLen")
	if queueName == "1" {
		return redis.NewIntResult(0, err)
	}

	if queueName == "2" {
		if r.rounds < 1 {
			r.rounds = 2
			return redis.NewIntResult(0, nil)
		}
	}

	return redis.NewIntResult(1, nil)
}

func (r *redisClientMock) BLPop(timeVar time.Duration, args ...string) *redis.StringSliceCmd {
	if args[0] == "3" && r.rounds == 0 {
		r.rounds++
		return redis.NewStringSliceResult([]string{"teste"}, nil)
	}

	err := errors.New("BLPop")
	return redis.NewStringSliceResult([]string{"teste"}, err)
}

func (r *redisClientMock) LPush(key string, values ...interface{}) *redis.IntCmd {
	return nil
}

//------------------------ JOBS MANAGER MOCK -----------------------
type JobsManagerMock struct{}

func (j *JobsManagerMock) SetConnManager(connManager *connectionsmanager.Manager) {}
func (j *JobsManagerMock) SetJob(job providers.JobsConfigs)                       {}
func (j *JobsManagerMock) SetClient(client interface{})                           {}
func (j *JobsManagerMock) SetQueueData(queueData interface{})                     {}
func (j *JobsManagerMock) GetJobConnections() map[string]interface{} {
	connections := make(map[string]interface{})
	connections["teste"] = "teste"
	return connections
}
func (j *JobsManagerMock) GetJob() providers.JobsConfigs {
	return providers.JobsConfigs{
		QueueName:   "3",
		Driver:      "test",
		Handle:      HandlerTest,
		Attempts:    float64(1),
		Connections: []string{"test"}}
}

func (j *JobsManagerMock) GetClient() interface{} {
	redisClient := redisClientMock{}
	redisClient.rounds = 0
	return &redisClient
}
func (j *JobsManagerMock) GetQueueData() interface{} {
	return nil
}
func (j *JobsManagerMock) CallDynamically() error {
	return errors.New("Test")
}

//------------------------------ TESTS ---------------------------------
func TestListenRedisReturnLLenError(t *testing.T) {
	listeners := Listener{}

	var teste = []providers.JobsConfigs{
		providers.JobsConfigs{"1", "test", "", float64(1), []string{"test"}},
	}

	redisClient := redisClientMock{}

	connections := make(map[string]interface{})
	connections["teste"] = "teste"

	connManager := connectionsmanager.Manager{DBClients: connections}

	jobManager := jobsManager.Manager{
		Job:         teste[0],
		Client:      &redisClient,
		ConnManager: &connManager,
	}

	var err error
	err = listeners.ListenRedis(&jobManager)

	if err == nil {
		t.Errorf("Expected an error but got %v", err)
	}

	if err.Error() != "LLen" {
		t.Errorf("Expected an error equal 'LLEN' but got %v", err)
	}
}

func TestListenRedisReturnBLPopError(t *testing.T) {
	listeners := Listener{}
	var teste = []providers.JobsConfigs{
		providers.JobsConfigs{"2", "test", "", float64(1), []string{"test"}},
	}

	redisClient := redisClientMock{}
	redisClient.rounds = 0
	connections := make(map[string]interface{})
	connections["teste"] = "teste"

	connManager := connectionsmanager.Manager{DBClients: connections}

	jobManager := jobsManager.Manager{
		Job:         teste[0],
		Client:      &redisClient,
		ConnManager: &connManager,
	}

	var err error
	err = listeners.ListenRedis(&jobManager)
	if err == nil {
		t.Errorf("Expected an error but got %v", err)
	}

	if err.Error() != "BLPop" {
		t.Errorf("Expected an error equal 'BLPop' but got %v", err)
	}
}

func TestListenRedisReturnBLPopCallFunction(t *testing.T) {
	listeners := Listener{}
	var teste = []providers.JobsConfigs{
		providers.JobsConfigs{"3", "test", HandlerTest, float64(1), []string{"test"}},
	}

	redisClient := redisClientMock{}
	redisClient.rounds = 0
	connections := make(map[string]interface{})
	connections["teste"] = "teste"

	connManager := connectionsmanager.Manager{DBClients: connections}

	jobManager := jobsManager.Manager{
		Job:         teste[0],
		Client:      &redisClient,
		ConnManager: &connManager,
	}

	var err error
	err = listeners.ListenRedis(&jobManager)
	if err == nil {
		t.Errorf("Expected an error but got %v", err)
	}

	if err.Error() != "BLPop" {
		t.Errorf("Expected an error equal 'BLPop' but got %v", err)
	}
}

func TestListenRedisCallJobAndReturnBlpopError(t *testing.T) {
	listeners := Listener{}
	var teste = []providers.JobsConfigs{
		providers.JobsConfigs{"3", "test", HandlerTest, float64(1), []string{"test"}},
	}

	redisClient := redisClientMock{}
	redisClient.rounds = 0
	connections := make(map[string]interface{})
	connections["teste"] = "teste"

	connManager := connectionsmanager.Manager{DBClients: connections}

	jobManager := jobsManager.Manager{
		Job:         teste[0],
		Client:      &redisClient,
		ConnManager: &connManager,
	}

	var err error
	err = listeners.ListenRedis(&jobManager)
	if err == nil {
		t.Errorf("Expected an error but got %v", err)
	}

	if err.Error() != "BLPop" {
		t.Errorf("Expected an error equal 'BLPop' but got %v", err)
	}
}

func TestListenRedisCallJobAndReturnError(t *testing.T) {
	listeners := Listener{}

	var err error
	err = listeners.ListenRedis(&JobsManagerMock{})
	if err == nil {
		t.Errorf("Expected an error but got %v", err)
	}

	if err.Error() != "Test" {
		t.Errorf("Expected an error equal 'BLPop' but got %v", err)
	}
}
