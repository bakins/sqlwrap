package sqlwrap

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync"
)

// based on stdlib sql
var (
	driversMu   sync.RWMutex
	drivers     = make(map[string]driver.Driver)
	driverCount = 0
)

// Register makes a database driver available by the provided name.
// If Register is called twice with the same name or if driver is nil,
// it will overwrite the last value.
func Register(name string, driver driver.Driver) {
	driversMu.Lock()
	defer driversMu.Unlock()
	if driver == nil {
		panic("sqlwrap: Register driver is nil")
	}
	drivers[name] = driver
}

func getDriverName(driver string, operationStarters ...OperationStarter) (string, error) {
	driversMu.Lock()
	defer driversMu.Unlock()
	d, ok := drivers[driver]
	if !ok {
		return "", fmt.Errorf("sql: unknown driver %q (forgotten Register?)", driver)
	}
	name := fmt.Sprintf("sqlwrap-%s-%d", driver, driverCount)
	driverCount++
	w := WrapDriver(d, operationStarters...)
	sql.Register(name, w)
	return name, nil
}

// Open will wrap the driver and call sql.Open
// The wrapping is handled internally.
// The driver must have been registered by a call to Register
func Open(driver string, dsn string, operationStarters ...OperationStarter) (*sql.DB, error) {
	name, err := getDriverName(driver, operationStarters...)
	if err != nil {
		return nil, err
	}
	return sql.Open(name, dsn)
}
