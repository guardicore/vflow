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
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func newHttp(addr string, workers, maxRetry int) *Http {
	h := &Http{
		config: HttpConfig{
			Address:  addr,
			Protocol: "tcp",
			MaxRetry: maxRetry,
			Workers:  workers,
		},
		logger: log.New(io.Discard, "", 0),
	}
	h.client = h.newClient()
	return h
}

func addrOf(ts *httptest.Server) string {
	return strings.TrimPrefix(ts.URL, "http://")
}

func TestHttpInputMsgConcurrent(t *testing.T) {
	var received int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&received, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	const total = 100
	h := newHttp(addrOf(ts), 8, 2)

	ch := make(chan []byte, total)
	for i := 0; i < total; i++ {
		ch <- []byte("{}")
	}
	close(ch)

	var errCount uint64
	h.inputMsg("", ch, &errCount)

	if got := atomic.LoadInt64(&received); got != total {
		t.Fatalf("received = %d, want %d", got, total)
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
	h := newHttp(addrOf(ts), 1, maxRetry)

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
