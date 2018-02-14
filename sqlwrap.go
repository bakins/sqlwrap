package sqlwrap

import (
	"context"
	"database/sql/driver"
	"errors"
)

type SQLOperation string

const EmptyStatement = ""

const (
	OperationBeginTx          SQLOperation = "BeginTx"
	OperationPrepareContext   SQLOperation = "PrepareContext"
	OperationExecContext      SQLOperation = "ExecContext"
	OperationPing             SQLOperation = "Ping"
	OperationQueryContext     SQLOperation = "QueryContext"
	OperationCommit           SQLOperation = "Commit"
	OperationRollback         SQLOperation = "Rollback"
	OperationStmtClose        SQLOperation = "Stmt.Close"
	OperationStmtExecContext  SQLOperation = "Stmt.ExecContext"
	OperationStmtQueryContext SQLOperation = "Stmt.QueryContext"
	OperationLastInsertId     SQLOperation = "LastInsertId"
	OperationRowsAffected     SQLOperation = "RowsAffected"
	OperationNext             SQLOperation = "Next"
)

var (
	ErrUnsupportedMethod = errors.New("unsupported method. Use the Context variant.")
	ErrDriverUnsupported = errors.New("unsupported driver. Does not support the Context variant.")
)

type OperationStarter interface {
	Start(ctx context.Context, o SQLOperation, query string) (Operation, context.Context)
}

type Operation interface {
	Finish(err error)
}

type Operations []Operation

func (o Operations) finish(err error) {
	for _, f := range o {
		defer f.Finish(err)
	}
}

type Driver struct {
	parent            driver.Driver
	operationStarters []OperationStarter
}

type wrappedConn struct {
	driver *Driver
	parent driver.Conn
}

type wrappedTx struct {
	driver *Driver
	ctx    context.Context
	parent driver.Tx
}

type wrappedResult struct {
	driver *Driver
	ctx    context.Context
	parent driver.Result
}

type wrappedStmt struct {
	driver *Driver
	ctx    context.Context
	query  string
	parent driver.Stmt
}

type wrappedRows struct {
	driver *Driver
	ctx    context.Context
	parent driver.Rows
}

func WrapDriver(driver driver.Driver, operationStarters ...OperationStarter) driver.Driver {
	d := Driver{
		parent:            driver,
		operationStarters: operationStarters,
	}
	return &d
}

func (d *Driver) startOperation(ctx context.Context, o SQLOperation, query string) (Operations, context.Context) {
	out := make([]Operation, 0, len(d.operationStarters))
	for _, s := range d.operationStarters {
		var op Operation
		// notice we pass ctx from one to the next
		op, ctx = s.Start(ctx, o, query)
		out = append(out, op)
	}

	return out, ctx
}

func (d *Driver) Open(name string) (driver.Conn, error) {
	conn, err := d.parent.Open(name)
	if err != nil {
		return nil, err
	}

	return &wrappedConn{driver: d, parent: conn}, nil
}

func (c *wrappedConn) Close() error {
	return c.parent.Close()
}

func (c wrappedConn) Prepare(query string) (driver.Stmt, error) {
	return nil, ErrUnsupportedMethod
}

func (c *wrappedConn) Begin() (driver.Tx, error) {
	return nil, ErrUnsupportedMethod
}

func (c *wrappedConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if x, ok := c.parent.(driver.ConnBeginTx); ok {
		o, ctx := c.driver.startOperation(ctx, OperationBeginTx, EmptyStatement)

		tx, err := x.BeginTx(ctx, opts)
		o.finish(err)
		if err != nil {
			return nil, err
		}

		return &wrappedTx{driver: c.driver, ctx: ctx, parent: tx}, nil
	}
	return nil, ErrDriverUnsupported

}

func (c *wrappedConn) PrepareContext(ctx context.Context, query string) (stmt driver.Stmt, err error) {
	if x, ok := c.parent.(driver.ConnPrepareContext); ok {
		o, ctx := c.driver.startOperation(ctx, OperationPrepareContext, query)
		stmt, err := x.PrepareContext(ctx, query)
		o.finish(err)
		if err != nil {
			return nil, err
		}

		return &wrappedStmt{driver: c.driver, ctx: ctx, parent: stmt}, nil
	}
	return nil, ErrDriverUnsupported
}

func (c *wrappedConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	return nil, ErrDriverUnsupported
}

func (c *wrappedConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if x, ok := c.parent.(driver.ExecerContext); ok {
		o, ctx := c.driver.startOperation(ctx, OperationExecContext, query)
		res, err := x.ExecContext(ctx, query, args)
		o.finish(err)
		if err != nil {
			return nil, err
		}
		return &wrappedResult{driver: c.driver, ctx: ctx, parent: res}, nil
	}
	return nil, ErrDriverUnsupported

}

func (c *wrappedConn) Ping(ctx context.Context) error {
	if x, ok := c.parent.(driver.Pinger); ok {
		o, ctx := c.driver.startOperation(ctx, OperationPing, EmptyStatement)
		err := x.Ping(ctx)
		o.finish(err)
		return err
	}

	return nil
}

func (c *wrappedConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	return nil, ErrUnsupportedMethod
}

func (c *wrappedConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if x, ok := c.parent.(driver.QueryerContext); ok {
		o, ctx := c.driver.startOperation(ctx, OperationQueryContext, query)

		rows, err := x.QueryContext(ctx, query, args)
		o.finish(err)
		if err != nil {
			return nil, err
		}

		return &wrappedRows{driver: c.driver, ctx: ctx, parent: rows}, nil
	}
	return nil, ErrDriverUnsupported
}

func (t *wrappedTx) Commit() error {
	o, _ := t.driver.startOperation(t.ctx, OperationCommit, EmptyStatement)
	err := t.parent.Commit()
	o.finish(err)
	return err
}

func (t *wrappedTx) Rollback() error {
	o, _ := t.driver.startOperation(t.ctx, OperationRollback, EmptyStatement)
	err := t.parent.Rollback()
	o.finish(err)
	return err
}

func (s *wrappedStmt) Close() error {
	o, _ := s.driver.startOperation(s.ctx, OperationStmtClose, EmptyStatement)
	err := s.parent.Close()
	o.finish(err)
	return err
}

func (s *wrappedStmt) NumInput() int {
	return s.parent.NumInput()
}

func (s *wrappedStmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, ErrUnsupportedMethod
}

func (s *wrappedStmt) Query(args []driver.Value) (driver.Rows, error) {
	return nil, ErrUnsupportedMethod
}

func (s *wrappedStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if x, ok := s.parent.(driver.StmtExecContext); ok {
		o, ctx := s.driver.startOperation(ctx, OperationStmtExecContext, s.query)
		res, err := x.ExecContext(ctx, args)
		o.finish(err)
		if err != nil {
			return nil, err
		}

		return &wrappedResult{driver: s.driver, ctx: ctx, parent: res}, nil
	}
	return nil, ErrDriverUnsupported
}

func (s *wrappedStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if x, ok := s.parent.(driver.StmtQueryContext); ok {
		o, ctx := s.driver.startOperation(ctx, OperationStmtQueryContext, s.query)
		rows, err := x.QueryContext(ctx, args)
		o.finish(err)
		if err != nil {
			return nil, err
		}

		return &wrappedRows{driver: s.driver, ctx: ctx, parent: rows}, nil
	}

	return nil, ErrDriverUnsupported

}

func (r *wrappedResult) LastInsertId() (int64, error) {
	o, _ := r.driver.startOperation(r.ctx, OperationLastInsertId, EmptyStatement)
	id, err := r.parent.LastInsertId()
	o.finish(err)
	return id, err
}

func (r *wrappedResult) RowsAffected() (int64, error) {
	o, _ := r.driver.startOperation(r.ctx, OperationRowsAffected, EmptyStatement)
	num, err := r.parent.RowsAffected()
	o.finish(err)
	return num, err
}

func (r *wrappedRows) Columns() []string {
	return r.parent.Columns()
}

func (r *wrappedRows) Close() error {
	return r.parent.Close()
}

func (r *wrappedRows) Next(dest []driver.Value) error {
	o, _ := r.driver.startOperation(r.ctx, OperationNext, EmptyStatement)
	err := r.parent.Next(dest)
	o.finish(err)
	return err
}
