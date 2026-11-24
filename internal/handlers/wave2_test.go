package handlers

import (
	"context"
	"errors"
	"testing"
	"time"

	"monitoring-worker-go/internal/model"
)

func TestRunSQL_QueryMatchesExpected(t *testing.T) {
	prevMySQL := mysqlOpener
	prevPostgres := postgresOpener
	t.Cleanup(func() {
		mysqlOpener = prevMySQL
		postgresOpener = prevPostgres
	})

	var gotDSN string
	mysqlOpener = func(dsn string) (sqlClient, error) {
		gotDSN = dsn
		return fakeSQLClient{queryResult: "ok"}, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp := RunSQL(ctx, model.Job{
		Type:          JobTypeMySQL,
		Host:          "db.local",
		Port:          3306,
		BasicAuthUser: "alice",
		BasicAuthPass: "secret",
		DbName:        "app",
		DbQuery:       "select 'ok'",
		DbExpected:    "ok",
		Timeout:       1,
	})

	if resp["status"] != 1 || resp["status_code"] != 200 {
		t.Fatalf("resp=%v", resp)
	}
	if gotDSN == "" {
		t.Fatal("mysql opener not called")
	}
}

func TestRunSQL_QueryMismatchFails(t *testing.T) {
	prev := postgresOpener
	t.Cleanup(func() { postgresOpener = prev })
	postgresOpener = func(dsn string) (sqlClient, error) {
		return fakeSQLClient{queryResult: "actual"}, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp := RunSQL(ctx, model.Job{
		Type:          JobTypePostgres,
		Host:          "db.local",
		Port:          5432,
		BasicAuthUser: "bob",
		BasicAuthPass: "secret",
		DbName:        "app",
		DbQuery:       "select 'actual'",
		DbExpected:    "expected",
		Timeout:       1,
	})

	if resp["status"] != 0 {
		t.Fatalf("status=%v want 0", resp["status"])
	}
	if resp["status_code"] != 422 {
		t.Fatalf("status_code=%v want 422", resp["status_code"])
	}
}

func TestRunRedis_PingSuccess(t *testing.T) {
	prev := redisClientFactory
	t.Cleanup(func() { redisClientFactory = prev })
	redisClientFactory = func(ctx context.Context, job model.Job) (redisClient, error) {
		return fakeRedisClient{}, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp := RunRedis(ctx, model.Job{Host: "127.0.0.1", Port: 6379, Timeout: 1})

	if resp["status"] != 1 || resp["status_code"] != 200 {
		t.Fatalf("resp=%v", resp)
	}
}

func TestRunMongo_PingFailure(t *testing.T) {
	prev := mongoClientFactory
	t.Cleanup(func() { mongoClientFactory = prev })
	mongoClientFactory = func(ctx context.Context, job model.Job) (mongoClient, error) {
		return fakeMongoClient{pingErr: errors.New("mongo down")}, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp := RunMongo(ctx, model.Job{Host: "127.0.0.1", Port: 27017, Timeout: 1})

	if resp["status"] != 0 {
		t.Fatalf("status=%v want 0", resp["status"])
	}
	if resp["status_code"] != 500 {
		t.Fatalf("status_code=%v want 500", resp["status_code"])
	}
}

type fakeSQLClient struct {
	pingErr     error
	queryErr    error
	queryResult string
}

func (f fakeSQLClient) PingContext(context.Context) error { return f.pingErr }
func (f fakeSQLClient) QueryValue(context.Context, string) (string, error) {
	return f.queryResult, f.queryErr
}
func (f fakeSQLClient) Close() error { return nil }

type fakeRedisClient struct{ pingErr error }

func (f fakeRedisClient) Ping(context.Context) error { return f.pingErr }
func (f fakeRedisClient) Close() error               { return nil }

type fakeMongoClient struct{ pingErr error }

func (f fakeMongoClient) Ping(context.Context) error { return f.pingErr }
func (f fakeMongoClient) Disconnect(context.Context) error {
	return nil
}
