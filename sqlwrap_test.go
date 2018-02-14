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
	tests := []struct {
		name     string
		mask     []SQLOperation
		expected []SQLOperation
	}{
		{
			name:     "record all",
			expected: []SQLOperation{OperationBeginTx, OperationExecContext, OperationExecContext, OperationQueryContext, OperationNext, OperationNext, OperationNext, OperationCommit},
		},
		{
			name:     "mask next",
			mask:     []SQLOperation{OperationNext},
			expected: []SQLOperation{OperationBeginTx, OperationExecContext, OperationExecContext, OperationQueryContext, OperationCommit},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			r := &testRecorder{}

			w := WrapDriver(sqlmockDriver, OperationMask(r, test.mask...))
			require.NotNil(t, w)

			sql.Register(test.name, w)

			d, mock, err := sqlmock.NewWithDSN(test.name)
			require.NoError(t, err)
			defer d.Close()

			db, err := sql.Open(test.name, test.name)
			require.NoError(t, err)
			require.NotNil(t, db)
			require.Equal(t, w, db.Driver())
			defer db.Close()

			mock.ExpectBegin()
			mock.ExpectExec("UPDATE products").WillReturnResult(sqlmock.NewResult(1, 1))
			mock.ExpectExec("INSERT INTO product_viewers").WithArgs(2, 3).WillReturnResult(sqlmock.NewResult(1, 1))
			rows := sqlmock.NewRows([]string{"id", "title"}).AddRow(1, "one").AddRow(2, "two")
			mock.ExpectQuery("SELECT").WillReturnRows(rows)
			mock.ExpectCommit()

			ctx := context.Background()

			tx, err := db.BeginTx(ctx, nil)
			require.NoError(t, err)
			require.NotNil(t, tx)

			_, err = tx.ExecContext(ctx, "UPDATE products")
			require.NoError(t, err)

			_, err = tx.ExecContext(ctx, "INSERT INTO product_viewers", 2, 3)
			require.NoError(t, err)

			rs, err := db.QueryContext(ctx, "SELECT")
			require.NoError(t, err)
			require.NotNil(t, rs)
			defer rs.Close()

			for rs.Next() {
				var id int
				var title string
				err = rs.Scan(&id, &title)
				require.NoError(t, err)
			}

			err = tx.Commit()
			require.NoError(t, err)

			require.Equal(t, len(test.expected), len(r.operations))

			for i := range test.expected {
				require.Equal(t, test.expected[i], r.operations[i].operation)
			}
		})
	}
}
