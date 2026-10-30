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

// Http represents Http producer
type Http struct {
	config HttpConfig
	logger *log.Logger
}

// HttpConfig is the struct that holds all configuation for HttpConfig connections
type HttpConfig struct {
	Address  string `yaml:"address"`
	URL      string `yaml:"url"`
	Protocol string `yaml:"protocol"`
	MaxRetry int    `yaml:"retry-max"`
	Workers  int    `yaml:"workers"`
}

func (h *Http) setup(configFile string, logger *log.Logger) error {
	h.config = HttpConfig{
		Address:  "localhost:9555",
		URL:      "",
		Protocol: "tcp",
		MaxRetry: 2,
		Workers:  200,
	}

	if err := h.load(configFile); err != nil {
		logger.Println(err)
		return err
	}

	if h.config.Workers < 1 {
		h.config.Workers = 1
	}

	h.logger = logger

	return nil
}

func (h *Http) newClient(timeout time.Duration) http.Client {
	return http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
				return net.Dial(h.config.Protocol, h.config.Address)
			},
		},
	}
}

func (h *Http) inputMsg(topic string, mCh chan []byte, ec *uint64) {
	h.logger.Printf("start producer: Http, server: %+v, Protocol: %s, workers: %d\n",
		h.config.Address, h.config.Protocol, h.config.Workers)

	var wg sync.WaitGroup
	for w := 0; w < h.config.Workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h.worker(mCh, ec)
		}()
	}

	wg.Wait()
}

func (h *Http) worker(mCh chan []byte, ec *uint64) {
	client := h.newClient(time.Second * 10)

	for {
		msg, ok := <-mCh
		if !ok {
			return
		}

		event := fmt.Sprintf("[%s]", string(msg))
		for i := 0; ; i++ {
			resp, err := client.Post("http://unix"+h.config.URL, "application/json", strings.NewReader(event))
			if err == nil {
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				break
			}

			h.logger.Println("Error sending event via POST:", err)

			atomic.AddUint64(ec, 1)

			if strings.HasSuffix(err.Error(), "broken pipe") {
				client = h.newClient(time.Second * 10)
				h.logger.Println("Reconnected after broken pipe")
			}

			if i >= h.config.MaxRetry {
				h.logger.Println("message failed after the configured retry limit:", err)
				break
			}

			h.logger.Println("retrying after error:", err)
		}
	}
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
