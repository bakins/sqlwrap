package sqlwrap

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"testing"

	"github.com/stretchr/testify/require"

	sqlmock "gopkg.in/DATA-DOG/go-sqlmock.v1"
)

var sqlmockDriver driver.Driver

func init() {
	db, _, _ := sqlmock.New()
	defer db.Close()
	sqlmockDriver = db.Driver()
}

// not thread safe test recorder
type testRecorder struct {
	operations []*recorderOperation
}

type recorderOperation struct {
	recorder  *testRecorder
	operation SQLOperation
	query     string
	err       error
}

func (r *testRecorder) Start(ctx context.Context, o SQLOperation, query string) (Operation, context.Context) {
	return &recorderOperation{
		recorder:  r,
		operation: o,
		query:     query,
	}, ctx
}

func (o *recorderOperation) Finish(err error) {
	o.err = err
	o.recorder.operations = append(o.recorder.operations, o)
}

func TestDriver(t *testing.T) {
	r := &testRecorder{}

	w := WrapDriver(sqlmockDriver, r)
	require.NotNil(t, w)

	sql.Register("wrapped", w)

	_, mock, err := sqlmock.NewWithDSN("testdb")
	require.NoError(t, err)

	db, err := sql.Open("wrapped", "testdb")
	require.NoError(t, err)
	require.NotNil(t, db)
	require.Equal(t, w, db.Driver())

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE products").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO product_viewers").WithArgs(2, 3).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	ctx := context.Background()

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NotNil(t, tx)

	_, err = tx.ExecContext(ctx, "UPDATE products")
	require.NoError(t, err)

	_, err = tx.ExecContext(ctx, "INSERT INTO product_viewers", 2, 3)
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	require.Equal(t, 4, len(r.operations))

	for i, o := range []SQLOperation{OperationBeginTx, OperationExecContext, OperationExecContext, OperationCommit} {
		require.Equal(t, o, r.operations[i].operation)
	}
}
