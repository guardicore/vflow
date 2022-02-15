package producer

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"gopkg.in/yaml.v2"
)

// New returns a new logger that logs to the provided testing.T.
func testLogger(t testing.TB) *log.Logger {
	t.Helper()
	return log.New(testWriter{TB: t}, t.Name()+" ", log.LstdFlags|log.Lshortfile|log.LUTC)
}

type testWriter struct {
	testing.TB
}

func (tw testWriter) Write(p []byte) (int, error) {
	tw.Helper()
	tw.Logf("%s", p)
	return len(p), nil
}

type testHttpServer struct {
	s       *http.Server
	success bool
}

func testServer(config *HttpConfig) (*testHttpServer, error) {
	var ts testHttpServer

	handlerFunc := func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" && r.URL.Path == config.URL {
			w.WriteHeader(http.StatusOK)
			ts.success = true
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}

	ts.s = &http.Server{Handler: http.HandlerFunc(handlerFunc)}
	if config.Protocol == "unix" {
		os.Remove(config.Address)
	}

	//	unixListener, err := net.Listen(config.Protocol, config.Address)
	unixListener, err := net.ListenUnix(config.Protocol, &net.UnixAddr{Name: config.Address, Net: config.Protocol})
	if err == nil {
		go ts.s.Serve(unixListener)
	}

	return &ts, err
}

func (s *testHttpServer) shutdown() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = s.s.Shutdown(ctx)
}

func createConfigFile(config *HttpConfig) (string, error) {
	tmpFile, err := ioutil.TempFile("", "testConfig*.yaml")
	if err != nil {
		return "", fmt.Errorf("could not create temp config file: %s: %v\n", tmpFile.Name(), err)
	}

	confYaml, err := yaml.Marshal(config)
	if err != nil {
		return "", fmt.Errorf("could not Marshal yaml: %s: %v\n", confYaml, err)
	}
	tmpFile.Write(confYaml)
	filename := tmpFile.Name()
	tmpFile.Close()

	return filename, nil
}

func TestFlowSanity(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "testSocket*.sock")
	if err != nil {
		t.Fatalf(err.Error())
	}
	tmpFile.Close()

	config := &HttpConfig{
		Address:  tmpFile.Name(),
		URL:      "/network-events/add-event",
		Protocol: "unix",
		MaxRetry: 2,
	}

	ts, err := testServer(config)
	defer ts.shutdown()

	if err != nil {
		t.Fatalf(err.Error())
	}

	configFile, err := createConfigFile(config)
	defer os.Remove(configFile)
	if err != nil {
		t.Fatalf(err.Error())
	}

	var producer Http
	producer.setup(configFile, testLogger(t))

	var ec uint64
	ch := make(chan []byte)
	endCh := make(chan struct{})
	go func() {
		producer.inputMsg("dummy_topic", ch, &ec)
		close(endCh)
	}()

	ch <- []byte("{}")
	close(ch)

	<-endCh
	if !ts.success {
		t.Error("did not get the expected server request")
	}
}
