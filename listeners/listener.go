package listener

import (
	"go-queue/interfaces"
	"log"
	"time"
)

//Listener is the listeners struct
type Listener struct{}

//ListenRedis listen queues
func (l Listener) ListenRedis(jobManager interfaces.JobsManagerInterface) error {
	for {
		redisClient := jobManager.GetClient().(interfaces.RedisInterface)
		job := jobManager.GetJob()

		items, err := redisClient.LLen(job.QueueName).Result()
		if err != nil {
			log.Printf("Error to check length of the queue: %v in redis: %v", job.QueueName, err)
			return err
		}

		if items < 1 {
			time.Sleep(3 * time.Second)
			continue
		}

		queueData, err := redisClient.BLPop(0, job.QueueName).Result()
		if err == nil {
			jobManager.SetQueueData(queueData)
			err = jobManager.CallDynamically()
			if err != nil {
				return err
			}

			continue
		}

		log.Printf("Error to pop queue with key: %v, error: %v", job.QueueName, err)
		return err
	}
}
