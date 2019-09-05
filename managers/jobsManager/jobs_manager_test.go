package jobsManager

import (
	"errors"
	"fmt"
	connectionsmanager "go-queue/managers/connectionsManager"
	"go-queue/providers"
	"reflect"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-redis/redis"
)

/************************ Mocks ******************/
func HandlerTest(paramTest interface{}, paramTestConn map[string]interface{}) error {
	return nil
}

type RedisMock struct{}

func (r *RedisMock) LPush(key string, values ...interface{}) *redis.IntCmd {

	if key == "test1" {
		return redis.NewIntResult(1, nil)
	}

	return redis.NewIntResult(0, errors.New("Test"))
}

func (r *RedisMock) LLen(queueName string) *redis.IntCmd {
	return redis.NewIntResult(1, nil)
}

func (r *RedisMock) BLPop(timeVar time.Duration, args ...string) *redis.StringSliceCmd {
	return redis.NewStringSliceResult([]string{"teste"}, nil)
}

/*********************** TESTS ******************/
func TestGetAndSetClient(t *testing.T) {
	jobManager := Manager{}

	jobManager.SetClient("test")
	client := jobManager.GetClient().(string)

	if client != "test" {
		t.Errorf("Client returned is different from setted")
	}
}

func TestGetAndSetJob(t *testing.T) {
	jobManager := &Manager{}

	jobConfig := providers.JobsConfigs{}

	jobManager.SetJob(jobConfig)
	job := jobManager.GetJob()

	if !reflect.DeepEqual(job, jobConfig) {
		t.Errorf("Job returned is different from setted")
	}
}

func TestSetConnManager(t *testing.T) {
	jobManager := &Manager{}
	jobManager.SetConnManager(&connectionsmanager.Manager{})
}

func TestGetAndSetQueueData(t *testing.T) {
	jobManager := &Manager{}

	jobManager.SetQueueData("test")
	queue := jobManager.GetQueueData()

	if queue.(string) != "test" {
		t.Errorf("Queue data returned is different from setted")
	}
}

func TestCallDynamically(t *testing.T) {
	var job = Manager{}
	job.Client = "teste"
	job.Job = providers.JobsConfigs{"test", "test", HandlerTest, float64(1), []string{"teste"}}
	job.ConnManager = &connectionsmanager.Manager{DBClients: make(map[string]interface{})}
	job.QueueData = "teste"

	returned := job.CallDynamically()

	if returned != nil {
		t.Errorf("Expected than CallDinamically return true but %v is giver", returned)
	}
}

func TestCheckAttemptsWithoutAttempts(t *testing.T) {
	var job = Manager{}
	job.Client = "teste"
	job.Job = providers.JobsConfigs{"test", "test", HandlerTest, float64(1), []string{"teste"}}
	job.ConnManager = &connectionsmanager.Manager{DBClients: make(map[string]interface{})}
	job.QueueData = "teste"

	returned, requeue := job.CheckAttempts(make(map[string]interface{}))

	if !requeue {
		t.Errorf("Expected requeue param to be true but false is given")
	}

	if returned.(float64) > 1 {
		t.Errorf("Expected attempts param to be equal to number 1")
	}
}

func TestCheckAttemptsWithAttempts(t *testing.T) {
	var job = Manager{}
	job.Client = "teste"
	job.Job = providers.JobsConfigs{"test", "test", HandlerTest, float64(4), []string{"teste"}}
	job.ConnManager = &connectionsmanager.Manager{DBClients: make(map[string]interface{})}
	job.QueueData = "teste"

	queueData := make(map[string]interface{})
	queueData["attempts"] = float64(1)

	returned, requeue := job.CheckAttempts(queueData)

	if !requeue {
		t.Errorf("Expected requeue param to be true but false is given")
	}

	if returned.(float64) < 1 {
		t.Errorf("Expected attempts param to be equal to number 1")
	}
}

func TestValidateIfWasProcessedRequeueReturnError(t *testing.T) {
	jobManager := &Manager{}
	jobManager.Job = providers.JobsConfigs{
		QueueName: "test1",
		Driver:    "redis",
		Attempts:  float64(3),
	}

	jobManager.Client = &RedisMock{}

	jobManager.QueueData = []string{"", `{"id": "test", "attempts":0}`}
	errTest := errors.New("test")

	err := jobManager.ValidateIfJobWasProcessed(errTest, "test")

	if err != nil {
		t.Errorf("Expected error is nil but got %v", err)
	}
}

func TestValidateIfWasProcessedRequeueEnterInSaveInMysqlAndReturnError(t *testing.T) {
	db, mock, err := sqlmock.New()

	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	defer db.Close()

	mock.ExpectPrepare("INSERT failed_jobs SET connection=\\?, queue=\\?, payload=\\?, exception=\\?, failed_at=\\?")
	mock.ExpectExec("INSERT failed_jobs SET connection=\\?, queue=\\?, payload=\\?, exception=\\?, failed_at=\\?").WillReturnError(errors.New("Test"))
	mock.ExpectClose()

	dbClientsMock := make(map[string]interface{})
	dbClientsMock["mysql"] = db

	connManager := connectionsmanager.Manager{DBClients: dbClientsMock}

	jobManager := &Manager{}
	jobManager.Job = providers.JobsConfigs{
		QueueName: "test1",
		Driver:    "redis",
		Attempts:  float64(0),
	}

	jobManager.ConnManager = &connManager

	jobManager.Client = &RedisMock{}

	jobManager.QueueData = []string{"queue:test", `{"id": "test", "attempts":0}`}
	errTest := errors.New("test")

	err = jobManager.ValidateIfJobWasProcessed(errTest, "test")

	if err == nil {
		t.Errorf("Expected error is nil but got %v", err)
	}
}

func TestValidateIfWasProcessedRequeueEnterInSaveInMysqlAndReturnNil(t *testing.T) {
	db, mock, err := sqlmock.New()

	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	defer db.Close()

	mock.ExpectPrepare("INSERT failed_jobs SET connection=\\?, queue=\\?, payload=\\?, exception=\\?, failed_at=\\?")
	mock.ExpectExec("INSERT failed_jobs SET connection=\\?, queue=\\?, payload=\\?, exception=\\?, failed_at=\\?").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectClose()

	dbClientsMock := make(map[string]interface{})
	dbClientsMock["mysql"] = db

	connManager := connectionsmanager.Manager{DBClients: dbClientsMock}

	jobManager := &Manager{}
	jobManager.Job = providers.JobsConfigs{
		QueueName: "test1",
		Driver:    "redis",
		Attempts:  float64(0),
	}

	jobManager.ConnManager = &connManager

	jobManager.Client = &RedisMock{}

	jobManager.QueueData = []string{"queue:test", `{"id": "test", "attempts":0}`}
	errTest := errors.New("test")

	err = jobManager.ValidateIfJobWasProcessed(errTest, "test")

	if err != nil {
		t.Error(err)
	}
}

func TestSaveFailedJobInMysqlReturnNil(t *testing.T) {
	db, mock, err := sqlmock.New()

	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	defer db.Close()

	mock.ExpectPrepare("INSERT failed_jobs SET connection=\\?, queue=\\?, payload=\\?, exception=\\?, failed_at=\\?")
	mock.ExpectExec("INSERT failed_jobs SET connection=\\?, queue=\\?, payload=\\?, exception=\\?, failed_at=\\?").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectClose()

	dbManager := make(map[string]interface{})
	dbManager["mysql"] = db

	connManager := connectionsmanager.Manager{
		DBClients: dbManager,
	}

	jobManager := Manager{
		ConnManager: &connManager,
	}

	jobManager.SetQueueData([]string{"queue:test", "test"})

	err = jobManager.SaveFailedJobInMysql("queue:test", "test")

	if err != nil {
		t.Errorf("Expected error is nil but got %v", err)
	}
}

func TestSaveFailedJobInMysqlReturnError(t *testing.T) {
	db, mock, err := sqlmock.New()

	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	defer db.Close()

	mock.ExpectPrepare("INSERT failed_jobs SET connection=\\?, queue=\\?, payload=\\?, exception=\\?, failed_at=\\?")
	mock.ExpectExec("INSERT failed_jobs SET connection=\\?, queue=\\?, payload=\\?, exception=\\?, failed_at=\\?").WillReturnError(errors.New("Test"))
	mock.ExpectClose()

	dbManager := make(map[string]interface{})
	dbManager["mysql"] = db

	connManager := connectionsmanager.Manager{
		DBClients: dbManager,
	}

	jobManager := Manager{
		ConnManager: &connManager,
	}

	jobManager.SetQueueData([]string{"queue:test", "test"})

	err = jobManager.SaveFailedJobInMysql("queue:test", "test")

	if err.Error() != "Test" {
		t.Errorf("An different error off expected was given %v", err)
	}
}

func TestRequeueJobEnterInRequeueIfAndReturnError(t *testing.T) {
	redisMock := &RedisMock{}
	jobManager := &Manager{}

	dbClientsMock := make(map[string]interface{})
	dbClientsMock["redis"] = redisMock

	connManager := connectionsmanager.Manager{DBClients: dbClientsMock}

	jobManager.SetConnManager(&connManager)
	jobManager.SetJob(providers.JobsConfigs{QueueName: "test", Driver: "redis", Attempts: float64(1)})
	jobManager.SetClient(redisMock)
	jobManager.SetQueueData([]string{"", ""})

	queueData := make(map[string]interface{})
	queueData["id"] = "teste"

	err := jobManager.ReenqueueJob("queue:test", queueData)

	if err == nil {
		t.Error("Expected an error but got nil")
	}
}

func TestRequeueJobDoNotEnterInRequeueIfReturnError(t *testing.T) {
	redisMock := &RedisMock{}
	jobManager := &Manager{}

	dbClientsMock := make(map[string]interface{})
	dbClientsMock["redis"] = redisMock

	connManager := connectionsmanager.Manager{DBClients: dbClientsMock}

	jobManager.SetConnManager(&connManager)
	jobManager.SetJob(providers.JobsConfigs{QueueName: "test", Driver: "redis", Attempts: float64(0)})
	jobManager.SetClient(redisMock)
	jobManager.SetQueueData([]string{"", ""})

	queueData := make(map[string]interface{})
	queueData["id"] = "teste"

	err := jobManager.ReenqueueJob("queue:test", queueData)

	if err == nil {
		t.Error("Expected an error but got nil")
	}
}

func TestUnMarshalJobDataReturnError(t *testing.T) {
	mapTest := unMarshalJobdata("")
	fmt.Println(mapTest)
}

func TestPushDataToQueueReturnWrongData(t *testing.T) {
	jobManager := Manager{}
	err := jobManager.pushDataToQueue("test")
	if err == nil {
		t.Error("Expected an error but got nil")
	}
}
