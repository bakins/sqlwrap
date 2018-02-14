package sqlwrap

import (
	"context"
)

type nullOperation struct{}

var defaultNullOperation = nullOperation{}

func (n nullOperation) Finish(err error) {

}

var allOperations = []SQLOperation{
	OperationBeginTx,
	OperationPrepareContext,
	OperationExecContext,
	OperationPing,
	OperationQueryContext,
	OperationCommit,
	OperationRollback,
	OperationStmtClose,
	OperationStmtExecContext,
	OperationStmtQueryContext,
	OperationLastInsertId,
	OperationRowsAffected,
	OperationNext,
}

// OperationExclude returns an OperationStarter which wraps another OperationStarter.
// This will pass along all SQLoperations except those listed
func OperationExclude(next OperationStarter, ops ...SQLOperation) *MaskStarter {
	operations := make(map[SQLOperation]bool, len(allOperations))
	for _, o := range allOperations {
		operations[o] = true
	}

	for _, o := range ops {
		operations[o] = false
	}
	return &MaskStarter{
		operations: operations,
		next:       next,
	}
}

// OperationInclude returns an OperationStarter which wraps another OperationStarter.
// This will pass along only the SQLoperations  listed
func OperationInclude(next OperationStarter, ops ...SQLOperation) *MaskStarter {
	operations := make(map[SQLOperation]bool, len(allOperations))
	for _, o := range allOperations {
		operations[o] = false
	}

	for _, o := range ops {
		operations[o] = true
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
