package jobsManager

import (
	"database/sql"
	"log"
)

// SQLDB ... Interface for *slq.DB
type SQLDB interface {
	Prepare(string) (*sql.Stmt, error)
}

//FailedJobsRepository struct
type FailedJobsRepository struct {
	DB SQLDB
}

//InsertQuery return Insert prepared query of failed_jobs table
func (failedJobs *FailedJobsRepository) InsertQuery(params ...interface{}) error {
	stmt, err := failedJobs.DB.Prepare("INSERT failed_jobs SET connection=?, queue=?, payload=?, exception=?, failed_at=?")
	if err != nil {
		log.Printf("Error to create InsertQuery of failed jobs error: %v", err)
		return err
	}

	_, err = stmt.Exec(params...)
	if err != nil {
		log.Printf("Error to execute insert query in FaileJobsRepository error: %v", err)
		return err
	}

	return nil
}
