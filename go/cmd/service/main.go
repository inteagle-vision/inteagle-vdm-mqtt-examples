package main

import (
	"context"
	"fmt"
	"github.com/inteagle-vision/inteagle-vdm-mqtt-examples/go/internal/service"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
func run() error {
	config, e := service.LoadConfig(env("VDM_SERVICE_CONFIG", "../contracts/config.example.json"))
	if e != nil {
		return e
	}
	host, token := env("VDM_HTTP_HOST", "127.0.0.1"), os.Getenv("VDM_API_TOKEN")
	if e = service.ValidateBind(host, token); e != nil {
		return e
	}
	port, e := strconv.Atoi(env("VDM_HTTP_PORT", "8080"))
	if e != nil || port < 1 || port > 65535 {
		return fmt.Errorf("invalid VDM_HTTP_PORT")
	}
	s, e := service.New(config, env("VDM_DATA_DIR", "./data"), token, os.Getenv("VDM_WEBHOOK_URL"), os.Getenv("VDM_WEBHOOK_TOKEN"))
	if e != nil {
		return e
	}
	defer s.Close()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if e = s.Start(ctx); e != nil {
		return e
	}
	server := &http.Server{Addr: net.JoinHostPort(host, strconv.Itoa(port)), Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 65 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 32 << 10}
	go func() {
		<-ctx.Done()
		shutdown, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		server.Shutdown(shutdown)
	}()
	log.Printf("VDM Go service listening on %s", server.Addr)
	e = server.ListenAndServe()
	if e == http.ErrServerClosed {
		return nil
	}
	return e
}
func main() {
	if e := run(); e != nil {
		log.Print(e)
		os.Exit(1)
	}
}
