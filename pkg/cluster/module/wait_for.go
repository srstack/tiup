// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package module

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiup/pkg/cluster/ctxt"
	"github.com/pingcap/tiup/pkg/utils"
	"go.uber.org/zap"
)

// WaitForConfig is the configurations of WaitFor module.
type WaitForConfig struct {
	Port  int           // Port number to poll.
	Sleep time.Duration // Duration to sleep between checks, default 1 second.
	// Choices:
	// started
	// stopped
	// When checking a port started will ensure the port is open, stopped will check that it is closed
	State   string
	Timeout time.Duration // Maximum duration to wait for.
	OS      string
}

// WaitFor is the module used to wait for some condition.
type WaitFor struct {
	c WaitForConfig
}

// NewWaitFor create a WaitFor instance.
func NewWaitFor(c WaitForConfig) *WaitFor {
	if c.Sleep == 0 {
		c.Sleep = time.Second
	}
	if c.Timeout == 0 {
		c.Timeout = time.Second * 60
	}
	if c.State == "" {
		c.State = "started"
	}

	w := &WaitFor{
		c: c,
	}

	return w
}

// Execute the module return nil if successfully wait for the event.
func (w *WaitFor) Execute(ctx context.Context, e ctxt.Executor) (err error) {
	retryOpt := utils.RetryOption{
		Delay:   w.c.Sleep,
		Timeout: w.c.Timeout,
	}

	switch w.c.OS {
	case MacOS:
		err = w.waitForMacOS(ctx, e, retryOpt)
	case Linux:
		err = w.waitForLinux(ctx, e, retryOpt)
	}

	if err != nil {
		zap.L().Debug("retry error", zap.Error(err))
		return errors.Errorf("timed out waiting for port %d to be %s after %s", w.c.Port, w.c.State, w.c.Timeout)
	}
	return nil
}

// waitForLinux
func (w *WaitFor) waitForLinux(ctx context.Context, e ctxt.Executor, retryOpt utils.RetryOption) error {
	pattern := []byte(fmt.Sprintf(":%d ", w.c.Port))
	// only listing TCP ports
	cmd := "ss -ltn"
	return utils.Retry(func() error {
		stdout, _, err := e.Execute(ctx, cmd, false)

		if err == nil {
			switch w.c.State {
			case "started":
				if bytes.Contains(stdout, pattern) {
					return nil
				}
				fallthrough
			case "stopped":
				if !bytes.Contains(stdout, pattern) {
					return nil
				}
				return errors.New("still waiting for port state to be satisfied")
			}
		}

		return err
	}, retryOpt)
}

// waitForMacOS
func (w *WaitFor) waitForMacOS(ctx context.Context, e ctxt.Executor, retryOpt utils.RetryOption) error {
	// start: listing TCP ports
	cmd := fmt.Sprintf("lsof -i:%d", w.c.Port)
	pattern := []byte("LISTEN")

	if w.c.State == "stopped" {
		// stop: get process status
		cmd = fmt.Sprintf("launchctl list | grep pingcap | grep %d", w.c.Port)
		pattern = []byte("-")
	}
	return utils.Retry(func() error {
		stdout, _, err := e.Execute(ctx, cmd, false)

		if err == nil {
			switch w.c.State {
			case "started":
				if bytes.Contains(stdout, pattern) {
					return nil
				}
				fallthrough
			case "stopped":
				if bytes.Contains(stdout, pattern) {
					return nil
				}
				return errors.New("still waiting for port state to be satisfied")
			}
		}

		return err
	}, retryOpt)
}
