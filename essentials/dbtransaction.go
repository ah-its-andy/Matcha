package essentials

import (
	"database/sql"
)

type DbTransaction struct {
	sess                *Session
	internalTransaction *sql.Tx
	connection          *DbConnection
}

func (transaction *DbTransaction) Rollback() error {
	return transaction.internalTransaction.Rollback()
}
func (transaction *DbTransaction) Exec(query string, args ...interface{}) (int64, error) {
	transaction.sess.Logger().Infoln(query)
	result, err := transaction.internalTransaction.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	affectedRows, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return affectedRows, nil
}

func (transaction *DbTransaction) ExecScript(name string, args ...interface{}) (int64, error) {
	script, err := transaction.sess.Script(name)
	if err != nil {
		return 0, WrapError("DbTransaction.ExecScript:", err)
	}
	sql, err := script.Compile()
	if err != nil {
		return 0, WrapError("DbTransaction.ExecScript:", err)
	}
	return transaction.Exec(sql, args...)
}

func (transaction *DbTransaction) Query(query string, args ...interface{}) (*sql.Rows, error) {
	transaction.sess.Logger().Infoln(query)
	return transaction.internalTransaction.Query(query, args...)
}
func (transaction *DbTransaction) GetInternalTx() *sql.Tx {
	return transaction.internalTransaction
}
func (transaction *DbTransaction) Commit() error {
	return transaction.internalTransaction.Commit()
}

func (transaction *DbTransaction) QueryRow(query string, args ...interface{}) *sql.Row {
	return transaction.GetInternalTx().QueryRow(query, args...)
}

func (transaction *DbTransaction) QueryScript(name string, args ...interface{}) (*sql.Rows, error) {
	script, err := transaction.sess.Script(name)
	if err != nil {
		return nil, WrapError("DbTransaction.QueryScript:", err)
	}
	sql, err := script.Compile()
	if err != nil {
		return nil, WrapError("DbTransaction.QueryScript:", err)
	}
	return transaction.Query(sql, args...)
}

func (transaction *DbTransaction) QueryScriptRow(name string, args ...interface{}) (*sql.Row, error) {
	script, err := transaction.sess.Script(name)
	if err != nil {
		return nil, WrapError("DbTransaction.QueryScriptRow:", err)
	}
	sql, err := script.Compile()
	if err != nil {
		return nil, WrapError("DbTransaction.QueryScriptRow:", err)
	}
	return transaction.QueryRow(sql, args...), nil
}
