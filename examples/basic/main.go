package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/wcharczuk/go-incr"
	"github.com/wcharczuk/temporalflow"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
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

	wf := temporalflow.Orchestrator{}
	w.RegisterWorkflow(wf.Orchestrate)

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

	_, err = c.SignalWithStartWorkflow(context.Background(), fmt.Sprintf("graph_%s", g.ID.Short()), temporalflow.SignalStabilize, temporalflow.SignalStabilizeArgs{}, client.StartWorkflowOptions{
		TaskQueue: "default",
	}, wf.Orchestrate, g)
	if err != nil {
		slog.Error("Unable to start workflow", slog.Any("err", err))
		os.Exit(1)
	}

	fmt.Println()
	fmt.Println("Workflow Started! You can interact with the graph with the tools in the UI found here: http://localhost:8233")
	select {}
}

const splay = 1024

func makeGraph() (g temporalflow.Graph) {
	g.ID = incr.NewIdentifier()
	nameVar := temporalflow.Node{
		Kind:  temporalflow.NodeKindVariable,
		Label: "name",
		Value: "Bufo",
	}
	obs := temporalflow.Node{
		Kind:  temporalflow.NodeKindObserver,
		Label: "obs",
	}
	var greetNodes []temporalflow.Node
	for index := range splay {
		greetNodes = append(greetNodes, temporalflow.Node{
			Kind:         temporalflow.NodeKindActivity,
			Label:        fmt.Sprintf("greet_%02d", index),
			ActivityType: "greeter",
			ActivityOptions: &workflow.ActivityOptions{
				StartToCloseTimeout: 10 * time.Second,
				RetryPolicy: &temporal.RetryPolicy{
					InitialInterval:    5 * time.Second,
					BackoffCoefficient: 1.0,
					MaximumAttempts:    5,
				},
			},
		})
	}
	g.Nodes = append([]temporalflow.Node{
		nameVar,
		obs,
	}, greetNodes...)
	for index := range splay {
		g.Edges = append(g.Edges, temporalflow.Edge{
			From: temporalflow.NodeSelector{Label: nameVar.Label},
			To:   temporalflow.NodeSelector{Label: fmt.Sprintf("greet_%02d", index)},
		})
		g.Edges = append(g.Edges, temporalflow.Edge{
			From: temporalflow.NodeSelector{Label: fmt.Sprintf("greet_%02d", index)},
			To:   temporalflow.NodeSelector{Label: obs.Label},
		})
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

func (slogShim) Debug(msg string, keyvals ...any) {
	slog.Debug(msg, keyvals...)
}
func (slogShim) Info(msg string, keyvals ...any) {
	slog.Info(msg, keyvals...)
}
func (slogShim) Warn(msg string, keyvals ...any) {
	slog.Warn(msg, keyvals...)
}
func (slogShim) Error(msg string, keyvals ...any) {
	slog.Error(msg, keyvals...)
}
