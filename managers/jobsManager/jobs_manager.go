package jobsManager

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"go-queue/interfaces"
	connectionsmanager "go-queue/managers/connectionsManager"
	"go-queue/providers"
	"log"
	"strings"
	"time"

	//mysql driver
	_ "github.com/go-sql-driver/mysql"
)

//Manager Struct with connections data
type Manager struct {
	Client      interface{}
	Job         providers.JobsConfigs
	ConnManager *connectionsmanager.Manager
	QueueData   interface{}
}

//SetConnManager sets connection manager
func (jobsManager *Manager) SetConnManager(connManager *connectionsmanager.Manager) {
	jobsManager.ConnManager = connManager
}

//GetClient return job client
func (jobsManager *Manager) GetClient() interface{} {
	return jobsManager.Client
}

//SetClient return job client
func (jobsManager *Manager) SetClient(client interface{}) {
	jobsManager.Client = client
}

//GetJob return job config
func (jobsManager *Manager) GetJob() providers.JobsConfigs {
	return jobsManager.Job
}

//SetJob sets job configs
func (jobsManager *Manager) SetJob(job providers.JobsConfigs) {
	jobsManager.Job = job
}

//SetQueueData sets job data
func (jobsManager *Manager) SetQueueData(queueData interface{}) {
	jobsManager.QueueData = queueData
}

//GetQueueData sets job data
func (jobsManager *Manager) GetQueueData() interface{} {
	return jobsManager.QueueData
}

//CallDynamically call the jobs functions by name
func (jobsManager *Manager) CallDynamically() error {
	jobConnections := jobsManager.ConnManager.GetJobDatabaseManagers(jobsManager.GetJob().Connections)
	err := jobsManager.Job.Handle.(func(interface{}, map[string]interface{}) error)(jobsManager.QueueData, jobConnections)
	return jobsManager.ValidateIfJobWasProcessed(err, jobsManager.Job.QueueName)
}

//ValidateIfJobWasProcessed check if job was successfuly
func (jobsManager *Manager) ValidateIfJobWasProcessed(jobError error, queueName string) error {
	if jobError == nil {
		fmt.Printf("%v... [Processed]\n", queueName)
		return nil
	}

	fmt.Printf("%v... [Failed]\n", queueName)
	log.Printf("error: %v", jobError)

	convertedQueueData := jobsManager.GetQueueData().([]string)
	queueData := unMarshalJobdata(convertedQueueData[1])

	err := jobsManager.ReenqueueJob(queueName, queueData)
	if err == nil {
		return err
	}

	err = jobsManager.SaveFailedJobInMysql(queueName, jobError.Error())
	if err != nil {
		log.Printf("Failed to save failed job %v", err)
		return err
	}

	return nil
}

//ReenqueueJob increase attempts number and reenqueue job in redis
func (jobsManager *Manager) ReenqueueJob(queueKey string, queueData map[string]interface{}) error {
	var requeue bool
	queueData["attempts"], requeue = jobsManager.CheckAttempts(queueData)
	if requeue {
		fmt.Printf("Requeueing job...\n")
		convertedQueueData := jobsManager.GetQueueData().([]string)
		marsheledData, _ := json.Marshal(queueData)
		convertedQueueData[1] = string(marsheledData)

		err := jobsManager.pushDataToQueue(convertedQueueData[1])
		if err != nil {
			log.Printf("error to requeue job: %v", err)
			return err
		}

		return nil
	}

	log.Printf("\nThe job with ID:%v failed more than %v times",
		queueData["id"].(string),
		jobsManager.GetJob().Attempts)

	return errors.New("Job failed many times")
}

//CheckAttempts the attempts of the job
func (jobsManager *Manager) CheckAttempts(queueData map[string]interface{}) (interface{}, bool) {
	requeue := false
	if _, ok := queueData["attempts"]; ok {
		queueData["attempts"] = queueData["attempts"].(float64) + 1
	} else {
		queueData["attempts"] = float64(1)
	}

	if queueData["attempts"].(float64) <= jobsManager.GetJob().Attempts {
		requeue = true
	}

	return queueData["attempts"], requeue
}

//SaveFailedJobInMysql save the data off job failed in database
func (jobsManager *Manager) SaveFailedJobInMysql(queue string, jobError string) error {
	db := jobsManager.ConnManager.DBClients["mysql"].(*sql.DB)

	failedJobsRepository := FailedJobsRepository{DB: db}
	convertedQueueData := jobsManager.GetQueueData().([]string)

	queueName := strings.Split(convertedQueueData[0], ":")
	nowTime := time.Now()

	err := failedJobsRepository.InsertQuery(queueName[1], queueName[1], convertedQueueData[1], jobError, nowTime.Format("20060102150405"))
	if err != nil {
		return err
	}

	return nil
}

func unMarshalJobdata(jobData string) map[string]interface{} {
	queueData := make(map[string]interface{})
	errNew := json.Unmarshal([]byte(jobData), &queueData)

	if errNew != nil {
		log.Printf("Erro: %v\n", errNew)
	}

	return queueData
}

func (jobsManager *Manager) pushDataToQueue(data string) error {
	var err error

	switch driver := jobsManager.GetJob().Driver; driver {
	case "redis":
		err = jobsManager.pushRedis(data)
	default:
		err = errors.New("Job type connection invalid")
	}

	return err
}

func (jobsManager *Manager) pushRedis(data string) error {
	redisClient := jobsManager.GetClient().(interfaces.RedisInterface)
	err := redisClient.LPush(jobsManager.Job.QueueName, data).Err()
	if err != nil {
		log.Printf("Error to reenqueue job: %v", err)
		return err
	}

	return nil
}
