package jobsManager

import (
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

//SQLMock struct to mock SQL
type SQLMock struct{}

func (s SQLMock) Prepare(string) (*sql.Stmt, error) {
	return nil, errors.New("Test")
}

func TestInsertQueryReturnErrorInPrepare(t *testing.T) {
	failedRepository := FailedJobsRepository{DB: SQLMock{}}

	returned := failedRepository.InsertQuery()

	if returned == nil {
		t.Errorf("Want error got %v", returned)
	}
}

func TestInsertQueryReturnNil(t *testing.T) {
	db, mock, err := sqlmock.New()

	defer db.Close()

	if err != nil {
		fmt.Println(err)
	}

	mock.ExpectPrepare("INSERT failed_jobs SET connection=\\?, queue=\\?, payload=\\?, exception=\\?, failed_at=\\?")
	mock.ExpectExec("INSERT failed_jobs SET connection=\\?, queue=\\?, payload=\\?, exception=\\?, failed_at=\\?").WillReturnResult(sqlmock.NewResult(1, 1))

	failedRepository := FailedJobsRepository{DB: db}
	err = failedRepository.InsertQuery(1, 2, 3, 4, 5)

	if err != nil {
		t.Errorf("Error to run query %v", err)
	}
}

func TestInsertQueryExecReturnError(t *testing.T) {
	db, mock, err := sqlmock.New()

	defer db.Close()

	if err != nil {
		fmt.Println(err)
	}

	mock.ExpectPrepare("INSERT failed_jobs SET connection=\\?, queue=\\?, payload=\\?, exception=\\?, failed_at=\\?")
	mock.ExpectExec("INSERT failed_jobs SET connection=\\?, queue=\\?, payload=\\?, exception=\\?, failed_at=\\?").WillReturnError(errors.New("test"))

	failedRepository := FailedJobsRepository{DB: db}
	err = failedRepository.InsertQuery(1, 2, 3, 4, 5)

	if err.Error() != "test" {
		t.Errorf("Error returned is different off expected %v", err)
	}
}
