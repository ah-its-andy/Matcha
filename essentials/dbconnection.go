package essentials

import (
	"context"
	"database/sql"
)

type DbConnection struct {
	sess               *Session
	internalConnection *sql.DB
}

func (conn *DbConnection) Close() error {
	err := conn.internalConnection.Close()
	if err != nil {
		return err
	}
	return nil
}

func (conn *DbConnection) Exec(query string, args ...interface{}) (int64, error) {
	conn.sess.Logger().Infoln(query)
	result, err := conn.internalConnection.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	affectedRows, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return affectedRows, nil
}

func (conn *DbConnection) ExecScript(name string, args ...interface{}) (int64, error) {
	script, err := conn.sess.Script(name)
	if err != nil {
		return 0, WrapError("DbConnection.ExecScript:", err)
	}
	sql, err := script.Compile()
	if err != nil {
		return 0, WrapError("DbConnection.ExecScript:", err)
	}
	return conn.Exec(sql, args...)
}

func (conn *DbConnection) Query(query string, args ...interface{}) (*sql.Rows, error) {
	conn.sess.Logger().Infoln(query)
	return conn.internalConnection.Query(query, args...)
}

func (conn *DbConnection) GetInternalConnection() *sql.DB {
	return conn.internalConnection
}

func (conn *DbConnection) BeginTx(level sql.IsolationLevel) (*DbTransaction, error) {
	tx, err := conn.internalConnection.BeginTx(context.Background(), &sql.TxOptions{
		Isolation: level,
		ReadOnly:  false,
	})

	if err != nil {
		return nil, err
	}

	return &DbTransaction{
		sess:                conn.sess,
		internalTransaction: tx,
		connection:          conn,
	}, nil
}

func (conn *DbConnection) QueryRow(query string, args ...interface{}) *sql.Row {
	return conn.GetInternalConnection().QueryRow(query, args...)
}

func (conn *DbConnection) QueryScript(name string, args ...interface{}) (*sql.Rows, error) {
	script, err := conn.sess.Script(name)
	if err != nil {
		return nil, WrapError("DbConnection.QueryScript:", err)
	}
	sql, err := script.Compile()
	if err != nil {
		return nil, WrapError("DbConnection.QueryScript:", err)
	}
	return conn.Query(sql, args...)
}

func (conn *DbConnection) QueryScriptRow(name string, args ...interface{}) (*sql.Row, error) {
	script, err := conn.sess.Script(name)
	if err != nil {
		return nil, WrapError("DbConnection.QueryScriptRow:", err)
	}
	sql, err := script.Compile()
	if err != nil {
		return nil, WrapError("DbConnection.QueryScriptRow:", err)
	}
	return conn.QueryRow(sql, args...), nil
}
