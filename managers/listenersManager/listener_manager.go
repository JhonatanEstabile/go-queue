package listenersManager

import (
	"go-queue/interfaces"
	connectionsmanager "go-queue/managers/connectionsManager"
	"go-queue/managers/jobsManager"
	"go-queue/providers"
	"log"
)

//ListenerInterface is a interface to mock listeners methods
type ListenerInterface interface {
	ListenRedis(interfaces.JobsManagerInterface) error
}

//ListenerManager is a struct with camps to manage listeners
type ListenerManager struct {
	Providers   []providers.JobsConfigs
	Listeners   ListenerInterface
	ConnManager *connectionsmanager.Manager
	JobsManager interfaces.JobsManagerInterface
}

func printOnError(err error, msg string) {
	if err != nil {
		log.Printf("%s: %s", msg, err)
	}
}

//RunListeners process listeners
func (l *ListenerManager) RunListeners() {
	qttJobs := len(l.Providers)

	for key, job := range l.Providers {
		if key == qttJobs-1 {
			err := l.LaunchListener(job)
			printOnError(err, "Fail on listener execution")
		}

		go func(job providers.JobsConfigs) {
			err := l.LaunchListener(job)
			printOnError(err, "Fail on listener execution")
		}(job)
	}
}

//LaunchListener launch the respective listener for the jobs
func (l *ListenerManager) LaunchListener(job providers.JobsConfigs) error {
	var err error
	l.JobsManager.SetJob(job)

	log.Printf("Launching listener for queue: '%v' in: '%v'\n", job.QueueName, job.Driver)
	if job.Driver == "redis" {
		l.JobsManager.SetClient(l.ConnManager.DBClients["redis"].(interfaces.RedisInterface))
		cloneJ := l.cloneJobManager(l.JobsManager)
		err = l.Listeners.ListenRedis(cloneJ)
	}

	return err
}

func (l *ListenerManager) cloneJobManager(jobManager interfaces.JobsManagerInterface) interfaces.JobsManagerInterface {
	clonedJobManager := jobsManager.Manager{}
	clonedJobManager.SetClient(jobManager.GetClient())
	clonedJobManager.SetJob(jobManager.GetJob())
	clonedJobManager.SetConnManager(l.ConnManager)
	clonedJobManager.SetQueueData(jobManager.GetQueueData())

	return &clonedJobManager
}
