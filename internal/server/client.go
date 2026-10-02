package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/Mohith26/airlock/internal/agent"
	"github.com/Mohith26/airlock/internal/bundle"
	"github.com/Mohith26/airlock/internal/control"
)

// Client talks to a control-plane cluster. It remembers the last leader it
// found, follows 421 leader hints, and fails over to other replicas when one
// is unreachable. It implements agent.ControlPlane.
type Client struct {
	mu      sync.Mutex
	urls    []string
	leader  string
	http    *http.Client
	Retries int
	Backoff time.Duration
}

func NewClient(urls ...string) *Client {
	return &Client{urls: urls, http: &http.Client{Timeout: 3 * time.Second}, Retries: 30, Backoff: 50 * time.Millisecond}
}

var _ agent.ControlPlane = (*Client)(nil)

func (c *Client) candidates() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, 0, len(c.urls)+1)
	if c.leader != "" {
		out = append(out, c.leader)
	}
	for _, u := range c.urls {
		if u != c.leader {
			out = append(out, u)
		}
	}
	return out
}

func (c *Client) setLeader(u string) {
	c.mu.Lock()
	c.leader = u
	c.mu.Unlock()
}

// do sends a request to the leader, retrying through elections.
func (c *Client) do(method, path string, body any, out any) error {
	var payload []byte
	if body != nil {
		payload, _ = json.Marshal(body)
	}
	var last error
	for attempt := 0; attempt <= c.Retries; attempt++ {
		for _, base := range c.candidates() {
			req, _ := http.NewRequest(method, base+path, bytes.NewReader(payload))
			if body != nil {
				req.Header.Set("Content-Type", "application/json")
			}
			resp, err := c.http.Do(req)
			if err != nil {
				last = err
				continue
			}
			b, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
			resp.Body.Close()
			switch {
			case resp.StatusCode == http.StatusMisdirectedRequest:
				var hint struct {
					LeaderURL string `json:"leader_url"`
				}
				json.Unmarshal(b, &hint)
				if hint.LeaderURL != "" {
					c.setLeader(hint.LeaderURL)
				}
				last = errors.New("not the leader")
				continue
			case resp.StatusCode == http.StatusServiceUnavailable:
				last = fmt.Errorf("%s", bytes.TrimSpace(b))
				continue
			case resp.StatusCode >= 400:
				return fmt.Errorf("%s %s: %d %s", method, path, resp.StatusCode, bytes.TrimSpace(b))
			}
			c.setLeader(base)
			if out == nil {
				return nil
			}
			if raw, ok := out.(*[]byte); ok {
				*raw = b
				return nil
			}
			return json.Unmarshal(b, out)
		}
		time.Sleep(c.Backoff)
	}
	return fmt.Errorf("control plane unavailable: %w", last)
}

// CommitResult is the response to a replicated write.
type CommitResult struct {
	Op        string `json:"op"`
	JobID     string `json:"job_id"`
	Status    string `json:"status"`
	Duplicate bool   `json:"duplicate"`
}

func (c *Client) RegisterRelease(m bundle.Manifest, artifact []byte) (CommitResult, error) {
	var r CommitResult
	err := c.do("POST", "/v1/releases", releaseReq{Manifest: m, Artifact: artifact}, &r)
	return r, err
}

func (c *Client) Submit(jobID, region, service, version string) (CommitResult, error) {
	var r CommitResult
	err := c.do("POST", "/v1/jobs", control.Command{JobID: jobID, Region: region, Service: service, Version: version}, &r)
	return r, err
}

func (c *Client) Status() (Status, error) {
	var s Status
	err := c.do("GET", "/v1/status", nil, &s)
	return s, err
}

func (c *Client) Export(region string) (bundle.Package, error) {
	var p bundle.Package
	err := c.do("GET", "/v1/offline/"+region, nil, &p)
	return p, err
}

// agent.ControlPlane, with a short retry budget so agents notice partitions quickly.

func (c *Client) quick() *Client {
	return &Client{urls: c.urls, leader: c.leaderURL(), http: c.http, Retries: 2, Backoff: 20 * time.Millisecond}
}

func (c *Client) leaderURL() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.leader
}

func (c *Client) Pending(region string) ([]control.Job, error) {
	q := c.quick()
	var jobs []control.Job
	err := q.do("GET", "/v1/regions/"+region+"/pending", nil, &jobs)
	c.setLeader(q.leaderURL())
	if err != nil {
		return nil, agent.ErrUnreachable
	}
	return jobs, nil
}

func (c *Client) Release(service, version string) (bundle.Manifest, error) {
	var m bundle.Manifest
	if err := c.quick().do("GET", "/v1/releases/"+service+"/"+version, nil, &m); err != nil {
		return m, agent.ErrUnreachable
	}
	return m, nil
}

func (c *Client) Artifact(digest string) ([]byte, error) {
	var b []byte
	if err := c.quick().do("GET", "/v1/artifacts/"+digest, nil, &b); err != nil {
		return nil, agent.ErrUnreachable
	}
	return b, nil
}

func (c *Client) Ack(jobID, status, detail string) error {
	var r CommitResult
	if err := c.quick().do("POST", "/v1/acks", control.Command{JobID: jobID, Status: status, Detail: detail}, &r); err != nil {
		return agent.ErrUnreachable
	}
	return nil
}
