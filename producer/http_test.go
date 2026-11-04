//: ----------------------------------------------------------------------------
//: Copyright (C) 2017 Verizon.  All Rights Reserved.
//: All Rights Reserved
//:
//: file:    http_test.go
//: details: tests for the http producer worker pool
//:
//: Licensed under the Apache License, Version 2.0 (the "License");
//: you may not use this file except in compliance with the License.
//: You may obtain a copy of the License at
//:
//:     http://www.apache.org/licenses/LICENSE-2.0
//:
//: Unless required by applicable law or agreed to in writing, software
//: distributed under the License is distributed on an "AS IS" BASIS,
//: WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//: See the License for the specific language governing permissions and
//: limitations under the License.
//: ----------------------------------------------------------------------------

package producer

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func newHttp(addr string, workers, maxRetry, batch int) *Http {
	h := &Http{
		config: HttpConfig{
			Address:       addr,
			Protocol:      "tcp",
			MaxRetry:      maxRetry,
			Workers:       workers,
			Batch:         batch,
			FlushInterval: 200,
		},
		logger: log.New(io.Discard, "", 0),
	}
	h.client = h.newClient()
	return h
}

func addrOf(ts *httptest.Server) string {
	return strings.TrimPrefix(ts.URL, "http://")
}

func countEventsServer(events, requests *int64) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(requests, 1)
		var arr []json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&arr); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		atomic.AddInt64(events, int64(len(arr)))
		w.WriteHeader(http.StatusOK)
	}))
}

func TestHttpInputMsgConcurrent(t *testing.T) {
	var events, requests int64
	ts := countEventsServer(&events, &requests)
	defer ts.Close()

	const total = 100
	h := newHttp(addrOf(ts), 8, 2, 1)

	ch := make(chan []byte, total)
	for i := 0; i < total; i++ {
		ch <- []byte("{}")
	}
	close(ch)

	var errCount uint64
	h.inputMsg("", ch, &errCount)

	if got := atomic.LoadInt64(&events); got != total {
		t.Fatalf("events = %d, want %d", got, total)
	}
	if errCount != 0 {
		t.Fatalf("errCount = %d, want 0", errCount)
	}
}

func TestHttpInputMsgBatch(t *testing.T) {
	var events, requests int64
	ts := countEventsServer(&events, &requests)
	defer ts.Close()

	const (
		total = 25
		batch = 10
	)
	h := newHttp(addrOf(ts), 1, 2, batch)

	ch := make(chan []byte, total)
	for i := 0; i < total; i++ {
		ch <- []byte("{}")
	}
	close(ch)

	var errCount uint64
	h.inputMsg("", ch, &errCount)

	if got := atomic.LoadInt64(&events); got != total {
		t.Fatalf("events = %d, want %d", got, total)
	}
	// 25 events, batch 10 -> 10 + 10 + 5 = 3 requests.
	if got := atomic.LoadInt64(&requests); got != 3 {
		t.Fatalf("requests = %d, want 3", got)
	}
	if errCount != 0 {
		t.Fatalf("errCount = %d, want 0", errCount)
	}
}

// TestHttpInputMsgFlushInterval verifies that a partial batch (fewer than
// Batch events) is flushed by the timer while the channel is still open.
func TestHttpInputMsgFlushInterval(t *testing.T) {
	var events, requests int64
	ts := countEventsServer(&events, &requests)
	defer ts.Close()

	// Batch far larger than what we send, so only the timer can flush it.
	h := newHttp(addrOf(ts), 1, 2, 1000)
	h.config.FlushInterval = 50

	ch := make(chan []byte)
	var errCount uint64
	done := make(chan struct{})
	go func() {
		h.inputMsg("", ch, &errCount)
		close(done)
	}()

	for i := 0; i < 3; i++ {
		ch <- []byte("{}")
	}

	// Wait past a couple of flush intervals; the partial batch must be sent
	// without the channel being closed.
	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadInt64(&events) < 3 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	if got := atomic.LoadInt64(&events); got != 3 {
		t.Fatalf("events = %d, want 3 (timer flush)", got)
	}

	close(ch)
	<-done

	if errCount != 0 {
		t.Fatalf("errCount = %d, want 0", errCount)
	}
}

func TestHttpInputMsgNon2xx(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	const maxRetry = 2
	h := newHttp(addrOf(ts), 1, maxRetry, 1)

	ch := make(chan []byte, 1)
	ch <- []byte("{}")
	close(ch)

	var errCount uint64
	h.inputMsg("", ch, &errCount)

	// Attempts i = 0..MaxRetry are made, each counted on failure.
	if want := uint64(maxRetry + 1); errCount != want {
		t.Fatalf("errCount = %d, want %d", errCount, want)
	}
}
