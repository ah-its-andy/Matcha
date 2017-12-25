package essentials

import (
	"database/sql"

	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

type DbExecutor interface {
	Exec(query string, args ...interface{}) (int64, error)
	ExecScript(name string, args ...interface{}) (int64, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	QueryScript(name string, args ...interface{}) (*sql.Rows, error)
	QueryScriptRow(name string, args ...interface{}) (*sql.Row, error)
}

type MockDbExecutor struct {
	sess               *Session
	internalConnection *sql.DB
	Mock               sqlmock.Sqlmock
}

func NewMockDbExecutor(sess *Session) (*MockDbExecutor, error) {
	conn, mock, err := sqlmock.New()
	if err != nil {
		return nil, err
	}
	return &MockDbExecutor{
		sess:               sess,
		internalConnection: conn,
		Mock:               mock,
	}, nil
}

func (m *MockDbExecutor) DbExecutor() DbExecutor {
	return m
}

func (m *MockDbExecutor) Exec(query string, args ...interface{}) (int64, error) {
	result, err := m.internalConnection.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	affectedRows, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return affectedRows, nil
}
func (m *MockDbExecutor) ExecScript(name string, args ...interface{}) (int64, error) {
	script, err := m.sess.Script(name)
	if err != nil {
		return 0, WrapError("MockDbExecutor.ExecScript:", err)
	}
	sql, err := script.Compile()
	if err != nil {
		return 0, WrapError("MockDbExecutor.ExecScript:", err)
	}
	return m.Exec(sql, args...)
}
func (m *MockDbExecutor) Query(query string, args ...interface{}) (*sql.Rows, error) {
	m.sess.Logger().Infoln(query)
	return m.internalConnection.Query(query, args...)
}
func (m *MockDbExecutor) QueryRow(query string, args ...interface{}) *sql.Row {
	return m.internalConnection.QueryRow(query, args...)
}
func (m *MockDbExecutor) QueryScript(name string, args ...interface{}) (*sql.Rows, error) {
	script, err := m.sess.Script(name)
	if err != nil {
		return nil, WrapError("DbConnection.QueryScript:", err)
	}
	sql, err := script.Compile()
	if err != nil {
		return nil, WrapError("DbConnection.QueryScript:", err)
	}
	return m.Query(sql, args...)
}
func (m *MockDbExecutor) QueryScriptRow(name string, args ...interface{}) (*sql.Row, error) {
	script, err := m.sess.Script(name)
	if err != nil {
		return nil, WrapError("DbConnection.QueryScriptRow:", err)
	}
	sql, err := script.Compile()
	if err != nil {
		return nil, WrapError("DbConnection.QueryScriptRow:", err)
	}
	return m.QueryRow(sql, args...), nil
}
