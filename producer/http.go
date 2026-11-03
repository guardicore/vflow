//: ----------------------------------------------------------------------------
//: Copyright (C) 2017 Verizon.  All Rights Reserved.
//: All Rights Reserved
//:
//: file:    http.go
//: details: vflow tcp/udp producer plugin
//: author:  Joe Percivall
//: date:    12/18/2017
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
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gopkg.in/yaml.v2"
	"net"
	"net/http"
)

const retryBackoff = 50 * time.Millisecond

// Http represents Http producer
type Http struct {
	config HttpConfig
	client *http.Client
	logger *log.Logger
}

// HttpConfig is the struct that holds all configuation for HttpConfig connections
type HttpConfig struct {
	Address  string `yaml:"address"`
	URL      string `yaml:"url"`
	Protocol string `yaml:"protocol"`
	MaxRetry int    `yaml:"retry-max"`
	Workers  int    `yaml:"workers"`
	Batch    int    `yaml:"batch"`
	FlushInterval int `yaml:"flush-interval-ms"`
}

func (h *Http) setup(configFile string, logger *log.Logger) error {
	h.config = HttpConfig{
		Address:       "localhost:9555",
		URL:           "",
		Protocol:      "tcp",
		MaxRetry:      2,
		Workers:       1,
		Batch:         50,
		FlushInterval: 200,
	}

	if err := h.load(configFile); err != nil {
		logger.Println(err)
		return err
	}

	if h.config.Workers < 1 {
		h.config.Workers = 1
	}

	if h.config.Batch < 1 {
		h.config.Batch = 1
	}

	if h.config.FlushInterval < 1 {
		h.config.FlushInterval = 200
	}

	h.client = h.newClient()
	h.logger = logger

	return nil
}

func (h *Http) newClient() *http.Client {
	dialer := &net.Dialer{
		Timeout:   5 * time.Second,
		KeepAlive: 30 * time.Second,
	}

	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return dialer.DialContext(ctx, h.config.Protocol, h.config.Address)
		},
		MaxIdleConns:        h.config.Workers * 2,
		MaxIdleConnsPerHost: h.config.Workers * 2,
		MaxConnsPerHost:     h.config.Workers * 2,
		IdleConnTimeout:     90 * time.Second,
	}

	return &http.Client{
		Timeout:   10 * time.Second,
		Transport: transport,
	}
}

func (h *Http) inputMsg(topic string, mCh chan []byte, errCount *uint64) {
	h.logger.Printf("start producer: Http, server: %+v, Protocol: %s, workers: %d, batch: %d\n",
		h.config.Address, h.config.Protocol, h.config.Workers, h.config.Batch)

	var wg sync.WaitGroup
	for w := 0; w < h.config.Workers; w++ {
		wg.Go(func() {
			h.worker(mCh, errCount)
		})
	}

	wg.Wait()
}

func (h *Http) worker(mCh chan []byte, errCount *uint64) {
	batch := make([][]byte, 0, h.config.Batch)

	interval := time.Duration(h.config.FlushInterval) * time.Millisecond
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		h.send(batch, errCount)
		batch = batch[:0]
	}

	for {
		select {
		case msg, ok := <-mCh:
			if !ok {
				flush()
				return
			}
			batch = append(batch, msg)
			if len(batch) >= h.config.Batch {
				flush()
				ticker.Reset(interval)
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (h *Http) send(batch [][]byte, errCount *uint64) {
	var buf bytes.Buffer
	buf.WriteByte('[')
	for i, msg := range batch {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.Write(msg)
	}
	buf.WriteByte(']')
	body := buf.String()

	for i := 0; ; i++ {
		err := h.post(body)
		if err == nil {
			break
		}

		h.logger.Println("Error sending events via POST:", err)

		atomic.AddUint64(errCount, 1)

		if i >= h.config.MaxRetry {
			h.logger.Println("batch failed after the configured retry limit:", err)
			break
		}

		h.logger.Println("retrying after error:", err)
		time.Sleep(retryBackoff)
	}
}

func (h *Http) post(event string) error {
	resp, err := h.client.Post("http://unix"+h.config.URL, "application/json", strings.NewReader(event))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("unexpected response status: %s", resp.Status)
	}

	return nil
}

func (h *Http) load(f string) error {
	b, err := ioutil.ReadFile(f)
	if err != nil {
		return err
	}

	err = yaml.Unmarshal(b, &h.config)
	if err != nil {
		return err
	}

	return nil
}
