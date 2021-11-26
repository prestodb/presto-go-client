package presto

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
)

type driverTx struct {
	conn *Conn
}

func (t *driverTx) Commit() error {
	if t.conn == nil {
		return driver.ErrBadConn
	}

	ctx := context.Background()
	stmt := &driverStmt{conn: t.conn, query: "COMMIT"}
	_, err := stmt.QueryContext(ctx, []driver.NamedValue{})
	if err != nil {
		return err
	}

	t.conn = nil
	return nil
}

func (t *driverTx) Rollback() error {
	if t.conn == nil {
		return driver.ErrBadConn
	}

	ctx := context.Background()
	stmt := &driverStmt{conn: t.conn, query: "ROLLBACK"}
	_, err := stmt.QueryContext(ctx, []driver.NamedValue{})
	if err != nil {
		return err
	}

	t.conn = nil
	return nil
}

func verifyIsolationLevel(level sql.IsolationLevel) error {
	switch level {
	case sql.LevelRepeatableRead, sql.LevelReadCommitted, sql.LevelReadUncommitted, sql.LevelSerializable:
		return nil
	default:
		return fmt.Errorf("presto: unsupported isolation level: %v", level)
	}
}
