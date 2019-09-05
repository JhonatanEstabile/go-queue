package providers

import "go-queue/jobs/sampleJob"

//JobsConfigs configurations struct off the jobs
type JobsConfigs struct {
	QueueName   string
	Driver      string
	Handle      interface{}
	Attempts    float64
	Connections []string
}

var providers = []JobsConfigs{
	//Add your job configuration here
	JobsConfigs{"queues:sample", "redis", sampleJob.Handle, 3, []string{"mongo"}},
}

//GetAllJobs Return all jobs in funcMap
func GetAllJobs() []JobsConfigs {
	return providers
}
