package connectionsmanager

import (
	"context"
	"flag"
	"io"
	"log"
	"os"

	"github.com/Graylog2/go-gelf/gelf"
	"github.com/go-redis/redis"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"database/sql"
	//mysql driver
	_ "github.com/go-sql-driver/mysql"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

//Manager struct off connections
type Manager struct {
	Env       map[string]string
	DBClients map[string]interface{}
}

//SetDatabaseClients get all DB clients setted on env file
func (connManager *Manager) SetDatabaseClients() {
	connectionsMap := make(map[string]interface{})

	if connManager.Env["REDIS_URL"] != "" {
		redisClient := connManager.GetRedisClient()
		connectionsMap["redis"] = redisClient
	}

	if connManager.Env["MONGO_URL"] != "" {
		mongoClient := connManager.GetMongoClient()
		connectionsMap["mongo"] = mongoClient
	}

	if connManager.Env["MYSQL_URL"] != "" {
		mysqlClient := connManager.GetMysqlClient()
		connectionsMap["mysql"] = mysqlClient
	}

	connManager.DBClients = connectionsMap
}

//GetJobDatabaseManagers return the connections that the job will use
func (connManager *Manager) GetJobDatabaseManagers(jobsConnections []string) map[string]interface{} {
	var connectionsToReturn = make(map[string]interface{})

	for _, value := range jobsConnections {
		if value == "mysql" {
			connectionsToReturn["mysql"] = connManager.DBClients["mysql"]
		}

		if value == "mongo" {
			connectionsToReturn["mongo"] = connManager.DBClients["mongo"]
		}

		if value == "redis" {
			connectionsToReturn["redis"] = connManager.DBClients["redis"]
		}
	}

	return connectionsToReturn
}

//GetRedisClient return redis connected client
func (connManager *Manager) GetRedisClient() *redis.Client {
	redisClient := redis.NewClient(&redis.Options{
		Addr:     connManager.Env["REDIS_URL"],
		Password: connManager.Env["REDIS_PASS"],
		DB:       0,
	})
	_, err := redisClient.Ping().Result()
	failOnError(err, "Failed to connect to redis")

	return redisClient
}

//GetMongoClient return mongo connected client
func (connManager *Manager) GetMongoClient() *mongo.Client {
	clientOptions := options.Client().ApplyURI(connManager.Env["MONGO_URL"])
	client, err := mongo.Connect(context.TODO(), clientOptions)
	failOnError(err, "Error to connect to Mongo")
	return client
}

//GetMysqlClient return MySQL connection
func (connManager *Manager) GetMysqlClient() *sql.DB {
	db, err := sql.Open("mysql", connManager.Env["MYSQL_URL"])
	failOnError(err, "Error to connect to MySQL")
	return db
}

//GraylogHook attached log with graylog
func (connManager *Manager) GraylogHook() error {
	graylogHost := connManager.Env["GRAYLOG_HOST"]
	if graylogHost != "" {
		var graylogAddr string

		flag.StringVar(&graylogAddr, "graylog", graylogHost, "")
		flag.Parse()

		gelfWriter, err := gelf.NewWriter(graylogAddr)
		if err != nil {
			return err
		}
		// log to both stderr and graylog2
		log.SetOutput(io.MultiWriter(os.Stderr, gelfWriter))
	}

	return nil
}
