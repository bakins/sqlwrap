package sqlwrap

import (
	"context"
)

type nullOperation struct{}

var defaultNullOperation = nullOperation{}

func (n nullOperation) Finish(err error) {

}

// OperationMask returns an OperationStarter which wraps another OperationStarter.
// This will pass along all SQLoperations except those listed
func OperationMask(next OperationStarter, ops ...SQLOperation) *MaskStarter {
	operations := map[SQLOperation]bool{
		OperationBeginTx:          true,
		OperationPrepareContext:   true,
		OperationExecContext:      true,
		OperationPing:             true,
		OperationQueryContext:     true,
		OperationCommit:           true,
		OperationRollback:         true,
		OperationStmtClose:        true,
		OperationStmtExecContext:  true,
		OperationStmtQueryContext: true,
		OperationLastInsertId:     true,
		OperationRowsAffected:     true,
		OperationNext:             true,
	}

	for _, o := range ops {
		operations[o] = false
	}
	return &MaskStarter{
		operations: operations,
		next:       next,
	}
}

type MaskStarter struct {
	operations map[SQLOperation]bool
	next       OperationStarter
}

func (m *MaskStarter) Start(ctx context.Context, o SQLOperation, query string) (Operation, context.Context) {
	if !m.operations[o] {
		return defaultNullOperation, ctx
	}
	return m.next.Start(ctx, o, query)
}
