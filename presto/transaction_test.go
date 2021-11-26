package presto

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
)

type queryHandler struct {
	url     string
	body    string
	handler func(w http.ResponseWriter, r *http.Request) (string, error)
	matched bool
}

type testServer struct {
	expectedQueries []*queryHandler
}

func (srv *testServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	bodyBytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(&stmtResponse{
			Error: stmtError{
				ErrorName: "BAD QUERY",
			},
		})
		return
	}

	var nextURI string
	body := string(bodyBytes)
	err = fmt.Errorf("unexpected query %s", body)
	for _, query := range srv.expectedQueries {
		if query.url == r.RequestURI && query.body == body {
			query.matched = true
			nextURI, err = query.handler(w, r)
			break
		}
	}

	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(&stmtResponse{
			Error: stmtError{
				ErrorName: err.Error(),
			},
		})
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(&stmtResponse{
		ID:      "id",
		NextURI: nextURI,
	})
}

func (srv *testServer) verifyExpectedQueries() error {
	for _, query := range srv.expectedQueries {
		if !query.matched {
			return fmt.Errorf("expected query not matched. url: %s, body: %s", query.body, query.url)
		}
	}

	return nil
}

func checkRequestTransactionHeader(r *http.Request, id string) error {
	headerValue := r.Header.Get(prestoTransactionHeader)
	if headerValue == id {
		return nil
	}

	return fmt.Errorf("unexpected transaction id in header. got: %s, expected: %s", headerValue, id)
}

func TestTransactionCommit(t *testing.T) {
	server := &testServer{}
	ts := httptest.NewServer(server)
	defer ts.Close()

	transactionID := "123"
	server.expectedQueries = []*queryHandler{
		{
			url:  "/v1/statement",
			body: "START TRANSACTION READ ONLY, ISOLATION LEVEL Read Uncommitted",
			handler: func(w http.ResponseWriter, r *http.Request) (string, error) {
				if err := checkRequestTransactionHeader(r, "NONE"); err != nil {
					return "", err
				}

				return fmt.Sprintf("%s/%s", ts.URL, "start"), nil
			},
		},
		{
			url:  "/start",
			body: "",
			handler: func(w http.ResponseWriter, r *http.Request) (string, error) {
				if err := checkRequestTransactionHeader(r, "NONE"); err != nil {
					return "", err
				}

				w.Header().Set(prestoStartedTransactionHeader, transactionID)
				return "", nil
			},
		},
		{
			url:  "/v1/statement",
			body: "SELECT * FROM TransactionTable",
			handler: func(w http.ResponseWriter, r *http.Request) (string, error) {
				if err := checkRequestTransactionHeader(r, transactionID); err != nil {
					return "", err
				}

				return fmt.Sprintf("%s/%s", ts.URL, "select_transaction"), nil
			},
		},
		{
			url:  "/select_transaction",
			body: "",
			handler: func(w http.ResponseWriter, r *http.Request) (string, error) {
				if err := checkRequestTransactionHeader(r, transactionID); err != nil {
					return "", err
				}

				return "", nil
			},
		},
		{
			url:  "/v1/statement",
			body: "COMMIT",
			handler: func(w http.ResponseWriter, r *http.Request) (string, error) {
				if err := checkRequestTransactionHeader(r, transactionID); err != nil {
					return "", err
				}

				return fmt.Sprintf("%s/%s", ts.URL, "commit"), nil
			},
		},
		{
			url:  "/commit",
			body: "",
			handler: func(w http.ResponseWriter, r *http.Request) (string, error) {
				if err := checkRequestTransactionHeader(r, transactionID); err != nil {
					return "", err
				}

				w.Header().Set(prestoClearTransactionHeader, "true")
				return "", nil
			},
		},
		{
			url:  "/v1/statement",
			body: "SELECT * FROM NoTransactionTable",
			handler: func(w http.ResponseWriter, r *http.Request) (string, error) {
				if err := checkRequestTransactionHeader(r, ""); err != nil {
					return "", err
				}

				return fmt.Sprintf("%s/%s", ts.URL, "select_no_transaction"), nil
			},
		},
		{
			url:  "/select_no_transaction",
			body: "",
			handler: func(w http.ResponseWriter, r *http.Request) (string, error) {
				if err := checkRequestTransactionHeader(r, ""); err != nil {
					return "", err
				}

				return "", nil
			},
		},
	}

	db, err := sql.Open("presto", ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelReadUncommitted})
	if err != nil {
		t.Fatal(err.Error())
	}

	_, err = tx.Query("SELECT * FROM TransactionTable")
	if err != nil {
		t.Fatal(err.Error())
	}

	err = tx.Commit()
	if err != nil {
		t.Fatal(err.Error())
	}

	_, err = db.Query("SELECT * FROM NoTransactionTable")
	if err != nil {
		t.Fatal(err.Error())
	}

	err = server.verifyExpectedQueries()
	if err != nil {
		t.Fatal(err.Error())
	}
}

func TestTransactionRollback(t *testing.T) {
	server := &testServer{}
	ts := httptest.NewServer(server)
	defer ts.Close()

	transactionID := "123"
	server.expectedQueries = []*queryHandler{
		{
			url:  "/v1/statement",
			body: "START TRANSACTION READ ONLY, ISOLATION LEVEL Read Uncommitted",
			handler: func(w http.ResponseWriter, r *http.Request) (string, error) {
				if err := checkRequestTransactionHeader(r, "NONE"); err != nil {
					return "", err
				}

				return fmt.Sprintf("%s/%s", ts.URL, "start"), nil
			},
		},
		{
			url:  "/start",
			body: "",
			handler: func(w http.ResponseWriter, r *http.Request) (string, error) {
				if err := checkRequestTransactionHeader(r, "NONE"); err != nil {
					return "", err
				}

				w.Header().Set(prestoStartedTransactionHeader, transactionID)
				return "", nil
			},
		},
		{
			url:  "/v1/statement",
			body: "SELECT * FROM TransactionTable",
			handler: func(w http.ResponseWriter, r *http.Request) (string, error) {
				if err := checkRequestTransactionHeader(r, transactionID); err != nil {
					return "", err
				}

				return fmt.Sprintf("%s/%s", ts.URL, "select_transaction"), nil
			},
		},
		{
			url:  "/select_transaction",
			body: "",
			handler: func(w http.ResponseWriter, r *http.Request) (string, error) {
				if err := checkRequestTransactionHeader(r, transactionID); err != nil {
					return "", err
				}

				return "", nil
			},
		},
		{
			url:  "/v1/statement",
			body: "ROLLBACK",
			handler: func(w http.ResponseWriter, r *http.Request) (string, error) {
				if err := checkRequestTransactionHeader(r, transactionID); err != nil {
					return "", err
				}

				return fmt.Sprintf("%s/%s", ts.URL, "rollback"), nil
			},
		},
		{
			url:  "/rollback",
			body: "",
			handler: func(w http.ResponseWriter, r *http.Request) (string, error) {
				if err := checkRequestTransactionHeader(r, transactionID); err != nil {
					return "", err
				}

				w.Header().Set(prestoClearTransactionHeader, "true")
				return "", nil
			},
		},
		{
			url:  "/v1/statement",
			body: "SELECT * FROM NoTransactionTable",
			handler: func(w http.ResponseWriter, r *http.Request) (string, error) {
				if err := checkRequestTransactionHeader(r, ""); err != nil {
					return "", err
				}

				return fmt.Sprintf("%s/%s", ts.URL, "select_no_transaction"), nil
			},
		},
		{
			url:  "/select_no_transaction",
			body: "",
			handler: func(w http.ResponseWriter, r *http.Request) (string, error) {
				if err := checkRequestTransactionHeader(r, ""); err != nil {
					return "", err
				}

				return "", nil
			},
		},
	}

	db, err := sql.Open("presto", ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelReadUncommitted})
	if err != nil {
		t.Fatal(err.Error())
	}

	_, err = tx.Query("SELECT * FROM TransactionTable")
	if err != nil {
		t.Fatal(err.Error())
	}

	err = tx.Rollback()
	if err != nil {
		t.Fatal(err.Error())
	}

	_, err = db.Query("SELECT * FROM NoTransactionTable")
	if err != nil {
		t.Fatal(err.Error())
	}

	err = server.verifyExpectedQueries()
	if err != nil {
		t.Fatal(err.Error())
	}
}
