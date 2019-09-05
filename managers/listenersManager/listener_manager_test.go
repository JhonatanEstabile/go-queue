package listenersManager

import (
	"errors"
	"go-queue/interfaces"
	connectionsmanager "go-queue/managers/connectionsManager"
	"go-queue/managers/jobsManager"
	"go-queue/providers"
	"testing"
	"time"

	"github.com/go-redis/redis"
)

//--------------------------- MOCK FUNCTIONS ------------------------\\
/***************** Redis Mock ***************/
type redisClientMock struct {
	rounds int
}

func (r *redisClientMock) LLen(queueName string) *redis.IntCmd {
	return redis.NewIntResult(1, nil)
}

func (r *redisClientMock) BLPop(timeVar time.Duration, args ...string) *redis.StringSliceCmd {
	return redis.NewStringSliceResult([]string{"teste"}, nil)
}

func (r *redisClientMock) LPush(key string, values ...interface{}) *redis.IntCmd {
	return nil
}

/***************** Listener Mock ***************/
type ListenerDoNotReturnErrorMock struct{}

func (l ListenerDoNotReturnErrorMock) ListenRedis(jobManager interfaces.JobsManagerInterface) error {
	return nil
}

type ListenerRedisReturnErrorMock struct{}

func (l ListenerRedisReturnErrorMock) ListenRedis(jobManager interfaces.JobsManagerInterface) error {
	return errors.New("Test")
}

//--------------------------- TEST FUNCTIONS ------------------------//
func TestLaunchListenerDoNotEnterInIf(t *testing.T) {
	dbConnection := make(map[string]interface{})
	connManager := connectionsmanager.Manager{DBClients: dbConnection}

	lm := ListenerManager{
		Listeners:   ListenerDoNotReturnErrorMock{},
		ConnManager: &connManager,
		JobsManager: &jobsManager.Manager{},
	}

	jbc := providers.JobsConfigs{"test", "test", "test", 3, []string{"test"}}
	err := lm.LaunchListener(jbc)

	if err != nil {
		t.Errorf("Error in function %v", err)
	}
}

func TestLaunchListenerEnterInRedisIf(t *testing.T) {
	dbConnection := make(map[string]interface{})
	redisMock := redisClientMock{}
	dbConnection["redis"] = &redisMock

	connManager := connectionsmanager.Manager{DBClients: dbConnection}

	lm := ListenerManager{
		Listeners:   ListenerDoNotReturnErrorMock{},
		ConnManager: &connManager,
		JobsManager: &jobsManager.Manager{},
	}

	jbc := providers.JobsConfigs{"test", "redis", "test", 3, []string{"test"}}

	err := lm.LaunchListener(jbc)

	if err != nil {
		t.Errorf("Error in function %v", err)
	}
}

func TestRunListersSuccessfull(t *testing.T) {
	lMock := ListenerDoNotReturnErrorMock{}

	dbConnection := make(map[string]interface{})
	redisMock := redisClientMock{}
	dbConnection["redis"] = &redisMock

	connManager := connectionsmanager.Manager{DBClients: dbConnection}

	jProviders := []providers.JobsConfigs{
		providers.JobsConfigs{"test", "redis", "test", 3, []string{"test"}},
		providers.JobsConfigs{"test", "redis", "test", 3, []string{"test"}},
		providers.JobsConfigs{"test", "redis", "test", 3, []string{"test"}},
	}

	lm := ListenerManager{
		Providers:   jProviders,
		Listeners:   lMock,
		ConnManager: &connManager,
		JobsManager: &jobsManager.Manager{},
	}

	lm.RunListeners()
}

func TestRunListersReturnError(t *testing.T) {
	lMock := ListenerRedisReturnErrorMock{}

	dbConnection := make(map[string]interface{})
	redisMock := redisClientMock{}
	dbConnection["redis"] = &redisMock

	connManager := connectionsmanager.Manager{DBClients: dbConnection}

	jProviders := []providers.JobsConfigs{
		providers.JobsConfigs{"test", "redis", "test", 3, []string{"test"}},
	}

	lm := ListenerManager{
		Providers:   jProviders,
		Listeners:   lMock,
		ConnManager: &connManager,
		JobsManager: &jobsManager.Manager{},
	}

	lm.RunListeners()
}
