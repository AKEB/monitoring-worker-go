package handlers

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"

	"monitoring-worker-go/internal/model"
)

type sqlClient interface {
	PingContext(context.Context) error
	QueryValue(context.Context, string) (string, error)
	Close() error
}

type redisClient interface {
	Ping(context.Context) error
	Close() error
}

type mongoClient interface {
	Ping(context.Context) error
	Disconnect(context.Context) error
}

var (
	mysqlOpener        = func(dsn string) (sqlClient, error) { return openSQLClient("mysql", dsn) }
	postgresOpener     = func(dsn string) (sqlClient, error) { return openSQLClient("postgres", dsn) }
	redisClientFactory = func(ctx context.Context, job model.Job) (redisClient, error) {
		return newRedisTCPClient(ctx, job)
	}
	mongoClientFactory = func(ctx context.Context, job model.Job) (mongoClient, error) {
		return newMongoPingClient(ctx, job)
	}
)

func RunSQL(ctx context.Context, job model.Job) map[string]any {
	start := time.Now()
	resp := map[string]any{"status": 1, "status_code": 0}

	var (
		open func(string) (sqlClient, error)
		dsn  string
	)
	switch job.Type {
	case JobTypeMySQL:
		open = mysqlOpener
		dsn = buildMySQLDSN(job)
	case JobTypePostgres:
		open = postgresOpener
		dsn = buildPostgresDSN(job)
	default:
		resp["status"] = 0
		resp["status_code"] = 500
		resp["response_error_num"] = 500
		resp["response_error"] = "unsupported SQL job type"
		resp["total_time_us"] = time.Since(start).Microseconds()
		return resp
	}

	client, err := open(dsn)
	if err != nil {
		resp["status"] = 0
		resp["status_code"] = 500
		resp["response_error_num"] = 500
		resp["response_error"] = truncateResponseError(err.Error())
		resp["total_time_us"] = time.Since(start).Microseconds()
		return resp
	}
	defer client.Close()

	if err := client.PingContext(ctx); err != nil {
		resp["status"] = 0
		resp["status_code"] = 500
		resp["response_error_num"] = 500
		resp["response_error"] = truncateResponseError(err.Error())
		resp["total_time_us"] = time.Since(start).Microseconds()
		return resp
	}

	if strings.TrimSpace(job.DbQuery) != "" {
		got, err := client.QueryValue(ctx, job.DbQuery)
		if err != nil {
			resp["status"] = 0
			resp["status_code"] = 500
			resp["response_error_num"] = 500
			resp["response_error"] = truncateResponseError(err.Error())
			resp["total_time_us"] = time.Since(start).Microseconds()
			return resp
		}
		resp["db_result"] = got
		if strings.TrimSpace(job.DbExpected) != "" && got != job.DbExpected {
			resp["status"] = 0
			resp["status_code"] = 422
			resp["response_error_num"] = 422
			resp["response_error"] = fmt.Sprintf("DB result mismatch: expected %q, got %q", job.DbExpected, got)
			resp["total_time_us"] = time.Since(start).Microseconds()
			return resp
		}
	}

	resp["status_code"] = 200
	resp["response_error"] = ""
	resp["response_error_num"] = 0
	resp["total_time_us"] = time.Since(start).Microseconds()
	return resp
}

func RunRedis(ctx context.Context, job model.Job) map[string]any {
	start := time.Now()
	resp := map[string]any{"status": 1, "status_code": 0}

	client, err := redisClientFactory(ctx, job)
	if err != nil {
		resp["status"] = 0
		resp["status_code"] = 500
		resp["response_error_num"] = 500
		resp["response_error"] = truncateResponseError(err.Error())
		resp["total_time_us"] = time.Since(start).Microseconds()
		return resp
	}
	defer client.Close()

	if err := client.Ping(ctx); err != nil {
		resp["status"] = 0
		resp["status_code"] = 500
		resp["response_error_num"] = 500
		resp["response_error"] = truncateResponseError(err.Error())
		resp["total_time_us"] = time.Since(start).Microseconds()
		return resp
	}

	resp["status_code"] = 200
	resp["response_error"] = ""
	resp["response_error_num"] = 0
	resp["total_time_us"] = time.Since(start).Microseconds()
	return resp
}

func RunMongo(ctx context.Context, job model.Job) map[string]any {
	start := time.Now()
	resp := map[string]any{"status": 1, "status_code": 0}

	client, err := mongoClientFactory(ctx, job)
	if err != nil {
		resp["status"] = 0
		resp["status_code"] = 500
		resp["response_error_num"] = 500
		resp["response_error"] = truncateResponseError(err.Error())
		resp["total_time_us"] = time.Since(start).Microseconds()
		return resp
	}
	defer client.Disconnect(context.Background())

	if err := client.Ping(ctx); err != nil {
		resp["status"] = 0
		resp["status_code"] = 500
		resp["response_error_num"] = 500
		resp["response_error"] = truncateResponseError(err.Error())
		resp["total_time_us"] = time.Since(start).Microseconds()
		return resp
	}

	resp["status_code"] = 200
	resp["response_error"] = ""
	resp["response_error_num"] = 0
	resp["total_time_us"] = time.Since(start).Microseconds()
	return resp
}

type stdSQLClient struct{ db *sql.DB }

func openSQLClient(driver, dsn string) (sqlClient, error) {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxIdleConns(1)
	db.SetMaxOpenConns(1)
	db.SetConnMaxLifetime(time.Minute)
	return &stdSQLClient{db: db}, nil
}

func (c *stdSQLClient) PingContext(ctx context.Context) error { return c.db.PingContext(ctx) }

func (c *stdSQLClient) QueryValue(ctx context.Context, query string) (string, error) {
	var v any
	if err := c.db.QueryRowContext(ctx, query).Scan(&v); err != nil {
		return "", err
	}
	return fmt.Sprint(v), nil
}

func (c *stdSQLClient) Close() error { return c.db.Close() }

func buildMySQLDSN(job model.Job) string {
	dbName := strings.TrimSpace(job.DbName)
	if dbName == "" {
		dbName = "mysql"
	}
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?timeout=%ds&readTimeout=%ds&writeTimeout=%ds",
		url.QueryEscape(job.BasicAuthUser),
		url.QueryEscape(job.BasicAuthPass),
		job.Host,
		job.Port,
		dbName,
		capTimeout(job.Timeout),
		capTimeout(job.Timeout),
		capTimeout(job.Timeout),
	)
}

func buildPostgresDSN(job model.Job) string {
	dbName := strings.TrimSpace(job.DbName)
	if dbName == "" {
		dbName = "postgres"
	}
	sslmode := "disable"
	if truthy(job.SSLVerify, false) {
		sslmode = "require"
	}
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s connect_timeout=%d sslmode=%s",
		job.Host, job.Port, job.BasicAuthUser, job.BasicAuthPass, dbName, capTimeout(job.Timeout), sslmode)
}

type redisTCPClient struct {
	conn net.Conn
	r    *bufio.Reader
}

func newRedisTCPClient(ctx context.Context, job model.Job) (redisClient, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", fmt.Sprintf("%s:%d", job.Host, job.Port))
	if err != nil {
		return nil, err
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	c := &redisTCPClient{conn: conn, r: bufio.NewReader(conn)}
	if strings.TrimSpace(job.BasicAuthPass) != "" {
		if err := c.command("AUTH", job.BasicAuthPass); err != nil {
			conn.Close()
			return nil, err
		}
	}
	return c, nil
}

func (c *redisTCPClient) Ping(ctx context.Context) error {
	if deadline, ok := ctx.Deadline(); ok {
		_ = c.conn.SetDeadline(deadline)
	}
	return c.command("PING")
}

func (c *redisTCPClient) Close() error { return c.conn.Close() }

func (c *redisTCPClient) command(parts ...string) error {
	var sb strings.Builder
	fmt.Fprintf(&sb, "*%d\r\n", len(parts))
	for _, p := range parts {
		fmt.Fprintf(&sb, "$%d\r\n%s\r\n", len(p), p)
	}
	if _, err := io.WriteString(c.conn, sb.String()); err != nil {
		return err
	}
	line, err := c.r.ReadString('\n')
	if err != nil {
		return err
	}
	line = strings.TrimSpace(line)
	if strings.HasPrefix(line, "+") {
		return nil
	}
	if strings.HasPrefix(line, "-") {
		return fmt.Errorf("redis %s", strings.TrimPrefix(line, "-"))
	}
	return fmt.Errorf("redis unexpected reply: %s", line)
}

type mongoPingClient struct{ client *mongo.Client }

func newMongoPingClient(ctx context.Context, job model.Job) (mongoClient, error) {
	dbName := strings.TrimSpace(job.DbName)
	if dbName == "" {
		dbName = "admin"
	}
	uri := fmt.Sprintf("mongodb://%s:%d/%s", job.Host, job.Port, dbName)
	opts := options.Client().ApplyURI(uri).SetConnectTimeout(time.Duration(capTimeout(job.Timeout)) * time.Second)
	if strings.TrimSpace(job.BasicAuthUser) != "" {
		opts.SetAuth(options.Credential{
			Username:   job.BasicAuthUser,
			Password:   job.BasicAuthPass,
			AuthSource: dbName,
		})
	}
	client, err := mongo.Connect(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &mongoPingClient{client: client}, nil
}

func (c *mongoPingClient) Ping(ctx context.Context) error {
	return c.client.Ping(ctx, readpref.Primary())
}

func (c *mongoPingClient) Disconnect(ctx context.Context) error {
	return c.client.Disconnect(ctx)
}
