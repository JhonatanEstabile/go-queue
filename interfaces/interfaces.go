package interfaces

import (
	connectionsmanager "go-queue/managers/connectionsManager"
	"go-queue/providers"
	"time"

	"github.com/go-redis/redis"
)

//RedisInterface interface
type RedisInterface interface {
	LLen(string) *redis.IntCmd
	BLPop(time.Duration, ...string) *redis.StringSliceCmd
	LPush(key string, values ...interface{}) *redis.IntCmd
}

//JobsManagerInterface is a interface for JobsManager
type JobsManagerInterface interface {
	SetConnManager(*connectionsmanager.Manager)
	GetJob() providers.JobsConfigs
	SetJob(job providers.JobsConfigs)
	GetClient() interface{}
	SetClient(client interface{})
	SetQueueData(queueData interface{})
	GetQueueData() interface{}
	CallDynamically() error
}
