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
)

func newHttp(addr string, workers, maxRetry, batch int) *Http {
	h := &Http{
		config: HttpConfig{
			Address:  addr,
			Protocol: "tcp",
			MaxRetry: maxRetry,
			Workers:  workers,
			Batch:    batch,
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
