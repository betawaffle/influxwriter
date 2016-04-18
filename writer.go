package influxwriter

import (
	"bytes"
	"crypto/tls"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"
)

type Writer struct {
	http    *http.Client
	baseURL *url.URL
	dbName  string
	user    string
	pass    string

	mu  sync.Mutex
	buf []byte
}

func NewWriter(uri, db, user, pass string, insecure bool) (*Writer, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}
	t := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		Dial: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	c := &http.Client{
		Transport: t,
	}

	if u.Scheme == "https" && insecure {
		t.TLSClientConfig = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	w := &Writer{
		http:    c,
		baseURL: u,
		dbName:  db,
		user:    user,
		pass:    pass,
	}
	go w.flusher()
	return w, nil
}

func (w *Writer) Write(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	w.mu.Lock()
	w.buf = append(w.buf, p...)
	w.mu.Unlock()
	return len(p), nil
}

func (w *Writer) flush() error {
	buf := w.swap(nil)
	for len(buf) > 0 {
		if err := w.post(buf); err != nil {
			w.unswap(buf)
			return err
		}
		buf = w.swap(buf[:0])
	}
	w.unswap(buf[:0])
	return nil
}

func (w *Writer) flusher() {
	// FIXME: This would leak if this goroutine shuts down.
	for range time.Tick(time.Second) {
		if err := w.flush(); err != nil {
			log.Printf("[ERROR] influx flush failed: %s", err)
		}
	}
}

func (w *Writer) post(buf []byte) error {
	req, err := http.NewRequest("POST", "/write", bytes.NewReader(buf))
	if err != nil {
		return err
	}

	req.URL = w.baseURL.ResolveReference(req.URL)
	req.URL.RawQuery = url.Values{
		"db": []string{w.dbName},
	}.Encode()

	if w.user != "" {
		req.SetBasicAuth(w.user, w.pass)
	}

	res, err := w.http.Do(req)
	if err != nil {
		return err
	}

	b, err := readClose(res.Body)
	if err != nil {
		return err
	}

	if res.StatusCode != http.StatusNoContent {
		return errors.New(string(b))
	}
	return nil
}

func (w *Writer) swap(buf []byte) []byte {
	w.mu.Lock()
	if len(w.buf) > 0 {
		w.buf, buf = buf, w.buf
	}
	w.mu.Unlock()
	return buf
}

func (w *Writer) unswap(buf []byte) {
	w.mu.Lock()
	w.buf = append(buf, w.buf...)
	w.mu.Unlock()
}

func readClose(r io.ReadCloser) ([]byte, error) {
	b, err := ioutil.ReadAll(r)
	r.Close()
	return b, err
}
