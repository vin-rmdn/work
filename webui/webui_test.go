package webui

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/vin-rmdn/work"
)

type TestWebUIServerSuite struct {
	suite.Suite

	ns       string
	pool     *redis.Pool
	server   *Server
	enqueuer *work.Enqueuer
}

func (s *TestWebUIServerSuite) SetupSuite() {
	s.pool = newTestPool(s.T())
	s.ns = "work"

	s.server = NewServer(s.ns, s.pool, ":6666")
	s.server.Start()

	s.enqueuer = work.NewEnqueuer(s.ns, s.pool)
}

func (s *TestWebUIServerSuite) TearDownSuite() {
	s.server.Stop()
}

func (s *TestWebUIServerSuite) SetupTest() {
	cleanKeyspace(s.ns, s.pool)
}

func TestNewServer(t *testing.T) {
	suite.Run(t, new(TestWebUIServerSuite))
}

type TestContext struct{}

func (s *TestWebUIServerSuite) TestPing() {
	req, err := http.NewRequest(http.MethodGet, "http://127.0.0.1:6666/ping", nil)
	s.NoError(err)
	resp, err := http.DefaultClient.Do(req)
	s.NoError(err)
	s.Equal(200, resp.StatusCode)

	var res map[string]string
	err = json.NewDecoder(resp.Body).Decode(&res)
	s.NoError(err)
	s.Equal("pong", res["ping"])
}

func (s *TestWebUIServerSuite) TestQueues() {
	enqueuer := s.enqueuer
	_, err := enqueuer.Enqueue("wat", nil)
	s.NoError(err)
	enqueuer.Enqueue("foo", nil)
	enqueuer.Enqueue("zaz", nil)

	// Start a pool to work on it. It's going to work on the queues
	// side effect of that is knowing which jobs are avail
	wp := work.NewWorkerPool(TestContext{}, 10, s.ns, s.pool)
	wp.Job("wat", func(job *work.Job) error {
		return nil
	})
	wp.Job("foo", func(job *work.Job) error {
		return nil
	})
	wp.Job("zaz", func(job *work.Job) error {
		return nil
	})
	wp.Start()
	time.Sleep(20 * time.Millisecond)
	wp.Stop()

	// Now that we have the jobs, populate some queues
	enqueuer.Enqueue("wat", nil)
	enqueuer.Enqueue("wat", nil)
	enqueuer.Enqueue("wat", nil)
	enqueuer.Enqueue("foo", nil)
	enqueuer.Enqueue("foo", nil)
	enqueuer.Enqueue("zaz", nil)

	req, err := http.NewRequest(http.MethodGet, "http://127.0.0.1:6666/queues", nil)
	s.NoError(err)
	resp, err := http.DefaultClient.Do(req)
	s.NoError(err)
	s.Equal(200, resp.StatusCode)

	var res []interface{}
	err = json.NewDecoder(resp.Body).Decode(&res)
	s.NoError(err)

	s.Equal(3, len(res))

	foomap, ok := res[0].(map[string]interface{})
	s.True(ok)
	s.Equal("foo", foomap["job_name"])
	s.EqualValues(2, foomap["count"])
	s.EqualValues(0, foomap["latency"])
}

func (s *TestWebUIServerSuite) TestWorkerPools() {

	wp := work.NewWorkerPool(TestContext{}, 10, s.ns, s.pool)
	wp.Job("wat", func(job *work.Job) error { return nil })
	wp.Job("bob", func(job *work.Job) error { return nil })
	wp.Start()
	defer wp.Stop()

	wp2 := work.NewWorkerPool(TestContext{}, 11, s.ns, s.pool)
	wp2.Job("foo", func(job *work.Job) error { return nil })
	wp2.Job("bar", func(job *work.Job) error { return nil })
	wp2.Start()
	defer wp2.Stop()

	time.Sleep(20 * time.Millisecond)

	req, err := http.NewRequest(http.MethodGet, "http://127.0.0.1:6666/worker_pools", nil)
	s.NoError(err)
	resp, err := http.DefaultClient.Do(req)
	s.NoError(err)
	s.Equal(200, resp.StatusCode)

	var res []interface{}
	err = json.NewDecoder(resp.Body).Decode(&res)
	s.NoError(err)

	s.Equal(2, len(res))

	w1stat, ok := res[0].(map[string]interface{})
	s.True(ok)
	s.True(w1stat["worker_pool_id"] != "")
	// NOTE: WorkerPoolStatus is tested elsewhere.
}

func (s *TestWebUIServerSuite) TestBusyWorkers() {

	// Keep a job in the in-progress state without using sleeps
	wgroup := sync.WaitGroup{}
	wgroup2 := sync.WaitGroup{}
	wgroup2.Add(1)

	wp := work.NewWorkerPool(TestContext{}, 10, s.ns, s.pool)
	wp.Job("wat", func(job *work.Job) error {
		wgroup2.Done()
		wgroup.Wait()
		return nil
	})
	wp.Start()
	defer wp.Stop()

	wp2 := work.NewWorkerPool(TestContext{}, 11, s.ns, s.pool)
	wp2.Start()
	defer wp2.Stop()

	time.Sleep(10 * time.Millisecond)

	req, err := http.NewRequest(http.MethodGet, "http://127.0.0.1:6666/busy_workers", nil)
	s.NoError(err)
	resp, err := http.DefaultClient.Do(req)
	s.NoError(err)
	s.Equal(200, resp.StatusCode)

	var res []interface{}
	err = json.NewDecoder(resp.Body).Decode(&res)
	s.NoError(err)
	s.Equal(0, len(res))

	wgroup.Add(1)

	// Ok, now let's make a busy worker
	enqueuer := s.enqueuer
	enqueuer.Enqueue("wat", nil)
	wgroup2.Wait()
	time.Sleep(5 * time.Millisecond) // need to let obsever process

	req, err = http.NewRequest(http.MethodGet, "http://127.0.0.1:6666/busy_workers", nil)
	s.NoError(err)
	resp, err = http.DefaultClient.Do(req)
	s.NoError(err)
	wgroup.Done()
	s.Equal(200, resp.StatusCode)
	err = json.NewDecoder(resp.Body).Decode(&res)
	s.NoError(err)
	s.Equal(1, len(res))

	if len(res) == 1 {
		hash, ok := res[0].(map[string]interface{})
		s.True(ok)
		s.Equal("wat", hash["job_name"])
		s.Equal(true, hash["is_busy"])
	}
}

func (s *TestWebUIServerSuite) TestRetryJobs() {

	enqueuer := s.enqueuer
	_, err := enqueuer.Enqueue("wat", nil)
	s.Nil(err)

	wp := work.NewWorkerPool(TestContext{}, 2, s.ns, s.pool)
	wp.Job("wat", func(job *work.Job) error {
		return fmt.Errorf("ohno")
	})
	wp.Start()
	wp.Drain()
	wp.Stop()

	req, err := http.NewRequest(http.MethodGet, "http://127.0.0.1:6666/retry_jobs", nil)
	s.NoError(err)
	resp, err := http.DefaultClient.Do(req)
	s.NoError(err)
	s.Equal(200, resp.StatusCode)
	var res struct {
		Count int64 `json:"count"`
		Jobs  []struct {
			RetryAt int64  `json:"retry_at"`
			Name    string `json:"name"`
			Fails   int64  `json:"fails"`
		} `json:"jobs"`
	}
	err = json.NewDecoder(resp.Body).Decode(&res)
	s.NoError(err)

	s.EqualValues(1, res.Count)
	s.Equal(1, len(res.Jobs))
	if len(res.Jobs) == 1 {
		s.True(res.Jobs[0].RetryAt > 0)
		s.Equal("wat", res.Jobs[0].Name)
		s.EqualValues(1, res.Jobs[0].Fails)
	}
}

func (s *TestWebUIServerSuite) TestScheduledJobs() {
	enqueuer := s.enqueuer
	_, err := enqueuer.EnqueueIn("watter", 1, nil)
	s.Nil(err)

	req, err := http.NewRequest(http.MethodGet, "http://127.0.0.1:6666/scheduled_jobs", nil)
	s.NoError(err)
	resp, err := http.DefaultClient.Do(req)
	s.NoError(err)
	s.Equal(200, resp.StatusCode)
	var res struct {
		Count int64 `json:"count"`
		Jobs  []struct {
			RunAt int64  `json:"run_at"`
			Name  string `json:"name"`
		} `json:"jobs"`
	}
	err = json.NewDecoder(resp.Body).Decode(&res)
	s.NoError(err)

	s.EqualValues(1, res.Count)
	s.Equal(1, len(res.Jobs))
	if len(res.Jobs) == 1 {
		s.True(res.Jobs[0].RunAt > 0)
		s.Equal("watter", res.Jobs[0].Name)
	}
}

func (s *TestWebUIServerSuite) TestDeadJobs() {

	enqueuer := s.enqueuer
	_, err := enqueuer.Enqueue("wat", nil)
	_, err = enqueuer.Enqueue("wat", nil)
	s.Nil(err)

	wp := work.NewWorkerPool(TestContext{}, 2, s.ns, s.pool)
	wp.JobWithOptions("wat", work.JobOptions{Priority: 1, MaxFails: 1}, func(job *work.Job) error {
		return fmt.Errorf("ohno")
	})
	wp.Start()
	wp.Drain()
	wp.Stop()

	req, err := http.NewRequest(http.MethodGet, "http://127.0.0.1:6666/dead_jobs", nil)
	s.NoError(err)
	resp, err := http.DefaultClient.Do(req)
	s.NoError(err)
	s.Equal(200, resp.StatusCode)
	var res struct {
		Count int64 `json:"count"`
		Jobs  []struct {
			DiedAt int64  `json:"died_at"`
			Name   string `json:"name"`
			ID     string `json:"id"`
			Fails  int64  `json:"fails"`
		} `json:"jobs"`
	}
	err = json.NewDecoder(resp.Body).Decode(&res)
	s.NoError(err)

	s.EqualValues(2, res.Count)
	s.Equal(2, len(res.Jobs))
	var diedAt0, diedAt1 int64
	var id0, id1 string
	if len(res.Jobs) == 2 {
		s.True(res.Jobs[0].DiedAt > 0)
		s.Equal("wat", res.Jobs[0].Name)
		s.EqualValues(1, res.Jobs[0].Fails)

		diedAt0, diedAt1 = res.Jobs[0].DiedAt, res.Jobs[1].DiedAt
		id0, id1 = res.Jobs[0].ID, res.Jobs[1].ID
	} else {
		return
	}

	// Ok, now let's retry one and delete one.
	req, err = http.NewRequest(http.MethodPost, fmt.Sprintf("http://127.0.0.1:6666/delete_dead_job/%d/%s", diedAt0, id0), nil)
	s.NoError(err)
	resp, err = http.DefaultClient.Do(req)
	s.NoError(err)
	s.Equal(200, resp.StatusCode)

	req, err = http.NewRequest(http.MethodPost, fmt.Sprintf("http://127.0.0.1:6666/retry_dead_job/%d/%s", diedAt1, id1), nil)
	s.NoError(err)
	resp, err = http.DefaultClient.Do(req)
	s.NoError(err)
	s.Equal(200, resp.StatusCode)

	// Make sure dead queue is empty
	req, err = http.NewRequest(http.MethodGet, "http://127.0.0.1:6666/dead_jobs", nil)
	s.NoError(err)
	resp, err = http.DefaultClient.Do(req)
	s.NoError(err)
	s.Equal(200, resp.StatusCode)
	err = json.NewDecoder(resp.Body).Decode(&res)
	s.NoError(err)
	s.EqualValues(0, res.Count)

	// Make sure the "wat" queue has 1 item in it
	req, err = http.NewRequest(http.MethodGet, "http://127.0.0.1:6666/queues", nil)
	s.NoError(err)
	resp, err = http.DefaultClient.Do(req)
	s.NoError(err)
	s.Equal(200, resp.StatusCode)
	var queueRes []struct {
		JobName string `json:"job_name"`
		Count   int64  `json:"count"`
	}
	err = json.NewDecoder(resp.Body).Decode(&queueRes)
	s.NoError(err)
	s.Equal(1, len(queueRes))
	if len(queueRes) == 1 {
		s.Equal("wat", queueRes[0].JobName)
	}
}

func (s *TestWebUIServerSuite) TestDeadJobsDeleteRetryAll() {

	enqueuer := s.enqueuer
	_, err := enqueuer.Enqueue("wat", nil)
	_, err = enqueuer.Enqueue("wat", nil)
	s.Nil(err)

	wp := work.NewWorkerPool(TestContext{}, 2, s.ns, s.pool)
	wp.JobWithOptions("wat", work.JobOptions{Priority: 1, MaxFails: 1}, func(job *work.Job) error {
		return fmt.Errorf("ohno")
	})
	wp.Start()
	wp.Drain()
	wp.Stop()

	req, err := http.NewRequest(http.MethodGet, "http://127.0.0.1:6666/dead_jobs", nil)
	s.NoError(err)
	resp, err := http.DefaultClient.Do(req)
	s.NoError(err)
	s.Equal(200, resp.StatusCode)
	var res struct {
		Count int64 `json:"count"`
		Jobs  []struct {
			DiedAt int64  `json:"died_at"`
			Name   string `json:"name"`
			ID     string `json:"id"`
			Fails  int64  `json:"fails"`
		} `json:"jobs"`
	}
	err = json.NewDecoder(resp.Body).Decode(&res)
	s.NoError(err)

	s.EqualValues(2, res.Count)
	s.Equal(2, len(res.Jobs))

	// Ok, now let's retry all
	req, err = http.NewRequest(http.MethodPost, "http://127.0.0.1:6666/retry_all_dead_jobs", nil)
	s.NoError(err)
	resp, err = http.DefaultClient.Do(req)
	s.NoError(err)
	s.Equal(200, resp.StatusCode)

	// Make sure dead queue is empty
	req, err = http.NewRequest(http.MethodGet, "http://127.0.0.1:6666/dead_jobs", nil)
	s.NoError(err)
	resp, err = http.DefaultClient.Do(req)
	s.NoError(err)
	s.Equal(200, resp.StatusCode)
	err = json.NewDecoder(resp.Body).Decode(&res)
	s.NoError(err)
	s.EqualValues(0, res.Count)

	// Make sure the "wat" queue has 2 items in it
	req, err = http.NewRequest(http.MethodGet, "http://127.0.0.1:6666/queues", nil)
	s.NoError(err)
	resp, err = http.DefaultClient.Do(req)
	s.NoError(err)
	s.Equal(200, resp.StatusCode)
	var queueRes []struct {
		JobName string `json:"job_name"`
		Count   int64  `json:"count"`
	}
	err = json.NewDecoder(resp.Body).Decode(&queueRes)
	s.NoError(err)
	s.Equal(1, len(queueRes))
	if len(queueRes) == 1 {
		s.Equal("wat", queueRes[0].JobName)
		s.EqualValues(2, queueRes[0].Count)
	}

	// Make them dead again:
	wp.Start()
	wp.Drain()
	wp.Stop()

	// Make sure we have 2 dead things again:
	req, err = http.NewRequest(http.MethodGet, "http://127.0.0.1:6666/dead_jobs", nil)
	s.NoError(err)
	resp, err = http.DefaultClient.Do(req)
	s.NoError(err)
	s.Equal(200, resp.StatusCode)
	err = json.NewDecoder(resp.Body).Decode(&res)
	s.NoError(err)
	s.EqualValues(2, res.Count)

	// Now delete them:
	req, err = http.NewRequest(http.MethodPost, "http://127.0.0.1:6666/delete_all_dead_jobs", nil)
	s.NoError(err)
	resp, err = http.DefaultClient.Do(req)
	s.NoError(err)
	s.Equal(200, resp.StatusCode)

	// Make sure dead queue is empty
	req, err = http.NewRequest(http.MethodGet, "http://127.0.0.1:6666/dead_jobs", nil)
	s.NoError(err)
	resp, err = http.DefaultClient.Do(req)
	s.NoError(err)
	s.Equal(200, resp.StatusCode)
	err = json.NewDecoder(resp.Body).Decode(&res)
	s.NoError(err)
	s.EqualValues(0, res.Count)
}

func (s *TestWebUIServerSuite) TestAssets() {
	req, err := http.NewRequest(http.MethodGet, "http://127.0.0.1:6666/", nil)
	s.NoError(err)
	resp, err := http.DefaultClient.Do(req)
	s.NoError(err)
	bytes, err := io.ReadAll(resp.Body)
	s.NoError(err)
	s.Regexp("html", string(bytes))

	req, err = http.NewRequest(http.MethodGet, "http://127.0.0.1:6666/work.js", nil)
	s.NoError(err)
	resp, err = http.DefaultClient.Do(req)
	s.NoError(err)
}

func newTestPool(t testing.TB) *redis.Pool {
	t.Helper()

	s, err := miniredis.Run()
	assert.NoError(t, err)
	t.Cleanup(s.Close)
	return &redis.Pool{
		MaxActive:   3,
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", s.Addr())
		},
		Wait: true,
	}
}

func cleanKeyspace(namespace string, pool *redis.Pool) {
	conn := pool.Get()
	defer conn.Close()

	keys, err := redis.Strings(conn.Do("KEYS", namespace+"*"))
	if err != nil {
		panic("could not get keys: " + err.Error())
	}
	for _, k := range keys {
		if _, err := conn.Do("DEL", k); err != nil {
			panic("could not del: " + err.Error())
		}
	}
}
