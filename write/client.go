package write

import (
	"net/url"
	"time"

	"github.com/valyala/fasthttp"
)

const (
	DefaultDatabase        = "stress"
	DefaultRetentionPolicy = "autogen"
)

type ClientConfig struct {
	BaseURL string

	Database        string
	RetentionPolicy string
	Precision       string
	Consistency     string
}

type Client interface {
	Send([]byte) (int64, int, error)
}

type client struct {
	url []byte
}

func NewClient(cfg ClientConfig) Client {
	return &client{url: []byte(writeURLFromConfig(cfg))}
}

func (c *client) Send(b []byte) (latNs int64, statusCode int, err error) {
	req := fasthttp.AcquireRequest()
	req.Header.SetContentTypeBytes([]byte("text/plain"))
	req.Header.SetMethodBytes([]byte("POST"))
	req.Header.SetRequestURIBytes(c.url)
	req.SetBody(b)

	resp := fasthttp.AcquireResponse()
	start := time.Now()

	err = fasthttp.Do(req, resp)
	latNs = time.Since(start).Nanoseconds()
	statusCode = resp.StatusCode()

	fasthttp.ReleaseResponse(resp)
	fasthttp.ReleaseRequest(req)

	return
}

func writeURLFromConfig(cfg ClientConfig) string {
	params := url.Values{}
	params.Set("db", cfg.Database)
	if cfg.RetentionPolicy != "" {
		params.Set("rp", cfg.RetentionPolicy)
	}
	if cfg.Precision != "n" && cfg.Precision != "" {
		params.Set("precision", cfg.Precision)
	}
	if cfg.Consistency != "one" && cfg.Consistency != "" {
		params.Set("consistency", cfg.Consistency)
	}

	return cfg.BaseURL + "/write?" + params.Encode()
}
