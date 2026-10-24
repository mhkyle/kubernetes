/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"k8s.io/apimachinery/pkg/util/validation"
)

type options struct {
	listenAddress   string
	labelKey        string
	maxRequestBytes int64
}

func main() {
	opts := options{}
	flag.StringVar(&opts.listenAddress, "listen-address", "127.0.0.1:8081", "HTTP listening address.")
	flag.StringVar(&opts.labelKey, "label-key", "node.osimage", "Node label containing version-<nonnegative integer>.")
	flag.Int64Var(&opts.maxRequestBytes, "max-request-bytes", 32<<20, "Maximum size of a prioritize request body in bytes.")
	flag.Parse()
	if flag.NArg() != 0 {
		log.Print("scheduler-extender accepts flags only; use --help for usage")
		os.Exit(1)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, opts); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func run(ctx context.Context, opts options) error {
	if errs := validation.IsQualifiedName(opts.labelKey); len(errs) != 0 {
		return fmt.Errorf("invalid --label-key %q: %s", opts.labelKey, strings.Join(errs, "; "))
	}
	if opts.maxRequestBytes <= 0 {
		return fmt.Errorf("--max-request-bytes must be positive, got %d", opts.maxRequestBytes)
	}
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", opts.listenAddress)
	if err != nil {
		return fmt.Errorf("listen on %q: %w", opts.listenAddress, err)
	}
	log.Printf("scheduler-extender listening on %s; OS version label: %s", listener.Addr(), opts.labelKey)
	return serveHTTP(ctx, listener, newHandler(opts.labelKey, opts.maxRequestBytes))
}

func serveHTTP(ctx context.Context, listener net.Listener, handler http.Handler) error {
	server := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	defer server.Close()
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.Serve(listener)
	}()
	select {
	case err := <-serveErr:
		return fmt.Errorf("serve HTTP: %w", err)
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutdown HTTP server: %w", err)
		}
		if err := <-serveErr; !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve HTTP during shutdown: %w", err)
		}
		return nil
	}
}
