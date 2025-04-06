package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	temporalflow "temporal-flow"
	"time"

	"github.com/wcharczuk/go-incr"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/worker"
)

func main() {
	c, err := client.Dial(client.Options{
		HostPort:  "127.0.0.1:7233",
		Namespace: "default",
		Logger:    slogShim{},
	})
	if err != nil {
		slog.Error("Unable to create client", slog.Any("err", err))
		os.Exit(1)
	}
	defer c.Close()

	w := worker.New(c, "default", worker.Options{})

	wf := temporalflow.Workflow{}
	w.RegisterWorkflow(wf.HostGraph)

	// register all the activities
	w.RegisterActivity(greeter)
	w.RegisterActivity(delay)
	w.RegisterActivity(fetchURL)

	err = w.Start()
	if err != nil {
		slog.Error("Unable to start worker", slog.Any("err", err))
		os.Exit(1)
	}
	defer w.Stop()

	g := makeGraph()

	_, err = c.SignalWithStartWorkflow(context.Background(), fmt.Sprintf("graph_%s", g.ID.Short()), temporalflow.SignalStabilize, struct{}{}, client.StartWorkflowOptions{
		TaskQueue: "default",
	}, wf.HostGraph, g)
	if err != nil {
		slog.Error("Unable to start workflow", slog.Any("err", err))
		os.Exit(1)
	}

	fmt.Println()
	fmt.Println("Workflow Started! You can interact with the graph with the tools in the UI found here: http://locahost:8233")
	select {}
}

func makeGraph() (g temporalflow.SerializedGraph) {
	g.ID = incr.NewIdentifier()
	urlVar := temporalflow.Node{
		Kind:  string(temporalflow.NodeKindVariable),
		Label: "url",
		Var: temporalflow.Var{
			Value: "http://example.com",
		},
	}
	nameVar := temporalflow.Node{
		Kind:  string(temporalflow.NodeKindVariable),
		Label: "name",
		Var: temporalflow.Var{
			Value: "Bufo",
		},
	}
	fetchURL := temporalflow.Node{
		Kind:  string(temporalflow.NodeKindActivity),
		Label: "fetchURL",
		Activity: temporalflow.Activity{
			ActivityType:        "fetchURL",
			StartToCloseTimeout: 10 * time.Second,
			RetryInterval:       5 * time.Second,
			MaxAttempts:         5,
		},
	}
	greet := temporalflow.Node{
		Kind:  string(temporalflow.NodeKindActivity),
		Label: "greet",
		Activity: temporalflow.Activity{
			ActivityType:        "greeter",
			StartToCloseTimeout: time.Second,
			RetryInterval:       5 * time.Second,
			MaxAttempts:         1,
		},
	}
	greetAgain := temporalflow.Node{
		Kind:  string(temporalflow.NodeKindActivity),
		Label: "greet_again",
		Activity: temporalflow.Activity{
			ActivityType:        "greeter",
			StartToCloseTimeout: time.Second,
			RetryInterval:       5 * time.Second,
			MaxAttempts:         1,
		},
	}
	obs00 := temporalflow.Node{
		Kind:  string(temporalflow.NodeKindObserver),
		Label: "obs00",
	}
	obs01 := temporalflow.Node{
		Kind:  string(temporalflow.NodeKindObserver),
		Label: "obs01",
	}
	g.Nodes = []temporalflow.Node{
		urlVar,
		nameVar,
		fetchURL,
		greet,
		greetAgain,
		obs00,
		obs01,
	}
	g.Edges = []temporalflow.Edge{
		{FromLabel: urlVar.Label, ToLabel: fetchURL.Label},
		{FromLabel: nameVar.Label, ToLabel: greet.Label},
		{FromLabel: greet.Label, ToLabel: greetAgain.Label},
		{FromLabel: fetchURL.Label, ToLabel: obs00.Label},
		{FromLabel: greetAgain.Label, ToLabel: obs01.Label},
	}
	return
}

func delay(ctx context.Context, value any) (any, error) {
	t := time.NewTimer(5 * time.Second)
	defer t.Stop()

	select {
	case <-ctx.Done():
		return value, nil
	case <-t.C:
		return value, nil
	}
}

func greeter(ctx context.Context, name string) (string, error) {
	return fmt.Sprintf("Hello %s!", name), nil
}

func fetchURL(ctx context.Context, url string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("fetchURL; failed to make request: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return "", fmt.Errorf("fetchURL; non-200 returned from server: %d", res.StatusCode)
	}
	contents, err := io.ReadAll(res.Body)
	if err != nil {
		return "", fmt.Errorf("fetchURL; failed to read response: %w", err)
	}
	return string(contents), nil
}

var _ log.Logger = (*slogShim)(nil)

type slogShim struct{}

func (slogShim) Debug(msg string, keyvals ...interface{}) {
	slog.Debug(msg, keyvals...)
}
func (slogShim) Info(msg string, keyvals ...interface{}) {
	slog.Info(msg, keyvals...)
}
func (slogShim) Warn(msg string, keyvals ...interface{}) {
	slog.Warn(msg, keyvals...)
}
func (slogShim) Error(msg string, keyvals ...interface{}) {
	slog.Error(msg, keyvals...)
}
