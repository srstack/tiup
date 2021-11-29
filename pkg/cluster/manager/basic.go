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

package manager

import (
	"context"
	"errors"
	"fmt"
	"path"
	"path/filepath"
	"strings"

	"github.com/fatih/color"
	"github.com/joomcode/errorx"
	perrs "github.com/pingcap/errors"
	"github.com/pingcap/tiup/pkg/cluster/ctxt"
	operator "github.com/pingcap/tiup/pkg/cluster/operation"
	"github.com/pingcap/tiup/pkg/cluster/spec"
	"github.com/pingcap/tiup/pkg/cluster/task"
	"github.com/pingcap/tiup/pkg/logger/log"
	"github.com/pingcap/tiup/pkg/meta"
	"github.com/pingcap/tiup/pkg/set"
	"github.com/pingcap/tiup/pkg/tui"
)

// EnableCluster enable/disable the service in a cluster
func (m *Manager) EnableCluster(name string, gOpt operator.Options, isEnable bool) error {
	if isEnable {
		log.Infof("Enabling cluster %s...", name)
	} else {
		log.Infof("Disabling cluster %s...", name)
	}

	metadata, err := m.meta(name)
	if err != nil && !errors.Is(perrs.Cause(err), meta.ErrValidate) {
		return err
	}

	topo := metadata.GetTopology()
	base := metadata.GetBaseMeta()

	b, err := m.sshTaskBuilder(name, topo, base.User, gOpt)
	if err != nil {
		return err
	}

	if isEnable {
		b = b.Func("EnableCluster", func(ctx context.Context) error {
			return operator.Enable(ctx, topo, gOpt, isEnable)
		})
	} else {
		b = b.Func("DisableCluster", func(ctx context.Context) error {
			return operator.Enable(ctx, topo, gOpt, isEnable)
		})
	}

	t := b.Build()

	if err := t.Execute(ctxt.New(context.Background(), gOpt.Concurrency)); err != nil {
		if errorx.Cast(err) != nil {
			// FIXME: Map possible task errors and give suggestions.
			return err
		}
		return perrs.Trace(err)
	}

	if isEnable {
		log.Infof("Enabled cluster `%s` successfully", name)
	} else {
		log.Infof("Disabled cluster `%s` successfully", name)
	}

	return nil
}

// StartCluster start the cluster with specified name.
func (m *Manager) StartCluster(name string, gOpt operator.Options, fn ...func(b *task.Builder, metadata spec.Metadata)) error {
	log.Infof("Starting cluster %s...", name)

	// check locked
	if err := m.specManager.ScaleOutLockedErr(name); err != nil {
		return err
	}

	metadata, err := m.meta(name)
	if err != nil && !errors.Is(perrs.Cause(err), meta.ErrValidate) {
		return err
	}

	topo := metadata.GetTopology()
	base := metadata.GetBaseMeta()

	tlsCfg, err := topo.TLSConfig(m.specManager.Path(name, spec.TLSCertKeyDir))
	if err != nil {
		return err
	}

	b, err := m.sshTaskBuilder(name, topo, base.User, gOpt)
	if err != nil {
		return err
	}

	b.Func("StartCluster", func(ctx context.Context) error {
		return operator.Start(ctx, topo, gOpt, tlsCfg)
	})

	for _, f := range fn {
		f(b, metadata)
	}

	t := b.Build()

	if err := t.Execute(ctxt.New(context.Background(), gOpt.Concurrency)); err != nil {
		if errorx.Cast(err) != nil {
			// FIXME: Map possible task errors and give suggestions.
			return err
		}
		return perrs.Trace(err)
	}

	log.Infof("Started cluster `%s` successfully", name)
	return nil
}

// StopCluster stop the cluster.
func (m *Manager) StopCluster(name string, gOpt operator.Options, skipConfirm bool) error {
	// check locked
	if err := m.specManager.ScaleOutLockedErr(name); err != nil {
		return err
	}

	metadata, err := m.meta(name)
	if err != nil && !errors.Is(perrs.Cause(err), meta.ErrValidate) {
		return err
	}

	topo := metadata.GetTopology()
	base := metadata.GetBaseMeta()

	tlsCfg, err := topo.TLSConfig(m.specManager.Path(name, spec.TLSCertKeyDir))
	if err != nil {
		return err
	}

	if !skipConfirm {
		if err := tui.PromptForConfirmOrAbortError(
			fmt.Sprintf("Will stop the cluster %s with nodes: %s, roles: %s.\nDo you want to continue? [y/N]:",
				color.HiYellowString(name),
				color.HiRedString(strings.Join(gOpt.Nodes, ",")),
				color.HiRedString(strings.Join(gOpt.Roles, ",")),
			),
		); err != nil {
			return err
		}
	}

	b, err := m.sshTaskBuilder(name, topo, base.User, gOpt)
	if err != nil {
		return err
	}

	t := b.
		Func("StopCluster", func(ctx context.Context) error {
			return operator.Stop(ctx, topo, gOpt, tlsCfg)
		}).
		Build()

	if err := t.Execute(ctxt.New(context.Background(), gOpt.Concurrency)); err != nil {
		if errorx.Cast(err) != nil {
			// FIXME: Map possible task errors and give suggestions.
			return err
		}
		return perrs.Trace(err)
	}

	log.Infof("Stopped cluster `%s` successfully", name)
	return nil
}

// RestartCluster restart the cluster.
func (m *Manager) RestartCluster(name string, gOpt operator.Options, skipConfirm bool) error {
	// check locked
	if err := m.specManager.ScaleOutLockedErr(name); err != nil {
		return err
	}

	metadata, err := m.meta(name)
	if err != nil && !errors.Is(perrs.Cause(err), meta.ErrValidate) {
		return err
	}

	topo := metadata.GetTopology()
	base := metadata.GetBaseMeta()

	tlsCfg, err := topo.TLSConfig(m.specManager.Path(name, spec.TLSCertKeyDir))
	if err != nil {
		return err
	}

	if !skipConfirm {
		if err := tui.PromptForConfirmOrAbortError(
			fmt.Sprintf("Will restart the cluster %s with nodes: %s roles: %s.\nCluster will be unavailable\nDo you want to continue? [y/N]:",
				color.HiYellowString(name),
				color.HiYellowString(strings.Join(gOpt.Nodes, ",")),
				color.HiYellowString(strings.Join(gOpt.Roles, ",")),
			),
		); err != nil {
			return err
		}
	}

	b, err := m.sshTaskBuilder(name, topo, base.User, gOpt)
	if err != nil {
		return err
	}
	t := b.
		Func("RestartCluster", func(ctx context.Context) error {
			return operator.Restart(ctx, topo, gOpt, tlsCfg)
		}).
		Build()

	if err := t.Execute(ctxt.New(context.Background(), gOpt.Concurrency)); err != nil {
		if errorx.Cast(err) != nil {
			// FIXME: Map possible task errors and give suggestions.
			return err
		}
		return perrs.Trace(err)
	}

	log.Infof("Restarted cluster `%s` successfully", name)
	return nil
}

// getMonitorHosts  get the instance to ignore list if it marks itself as ignore_exporter
func getMonitorHosts(topo spec.Topology) (map[string]hostInfo, set.StringSet) {
	// monitor
	uniqueHosts := make(map[string]hostInfo) // host -> ssh-port, os, arch
	noAgentHosts := set.NewStringSet()
	topo.IterInstance(func(inst spec.Instance) {
		// add the instance to ignore list if it marks itself as ignore_exporter
		if inst.IgnoreMonitorAgent() {
			noAgentHosts.Insert(inst.GetHost())
		}

		if _, found := uniqueHosts[inst.GetHost()]; !found {
			uniqueHosts[inst.GetHost()] = hostInfo{
				ssh:  inst.GetSSHPort(),
				os:   inst.OS(),
				arch: inst.Arch(),
			}
		}
	})

	return uniqueHosts, noAgentHosts
}

// cleanUpFiles record the file that needs to be cleaned up
type cleanUpFiles struct {
	cleanupData     bool                     // whether to clean up the data
	cleanupLog      bool                     // whether to clean up the log
	cleanupTLS      bool                     // whether to clean up the tls files
	retainDataRoles []string                 // roles that don't clean up
	retainDataNodes []string                 // roles that don't clean up
	delFileMap      map[string]set.StringSet //
}

// getCleanupFiles  get the files that need to be deleted
func getCleanupFiles(topo spec.Topology, cleanupData, cleanupLog, cleanupTLS bool, retainDataRoles, retainDataNodes []string) map[string]set.StringSet {
	c := &cleanUpFiles{
		cleanupData:     cleanupData,
		cleanupLog:      cleanupLog,
		cleanupTLS:      cleanupTLS,
		retainDataRoles: retainDataRoles,
		retainDataNodes: retainDataNodes,
		delFileMap:      make(map[string]set.StringSet),
	}

	// calculate file paths to be deleted before the prompt
	c.instanceCleanupFiles(topo)
	c.monitorCleanupFiles(topo)

	return c.delFileMap
}

// instanceCleanupFiles get the files that need to be deleted in the component
func (c *cleanUpFiles) instanceCleanupFiles(topo spec.Topology) {
	for _, com := range topo.ComponentsByStopOrder() {
		instances := com.Instances()
		retainDataRoles := set.NewStringSet(c.retainDataRoles...)
		retainDataNodes := set.NewStringSet(c.retainDataNodes...)

		for _, ins := range instances {
			// not cleaning files of monitor agents if the instance does not have one
			// may not work
			switch ins.ComponentName() {
			case spec.ComponentNodeExporter,
				spec.ComponentBlackboxExporter:
				if ins.IgnoreMonitorAgent() {
					continue
				}
			}

			// Some data of instances will be retained
			dataRetained := retainDataRoles.Exist(ins.ComponentName()) ||
				retainDataNodes.Exist(ins.ID()) || retainDataNodes.Exist(ins.GetHost())

			if dataRetained {
				continue
			}

			// prevent duplicate directories
			dataPaths := set.NewStringSet()
			logPaths := set.NewStringSet()
			tlsPath := set.NewStringSet()

			if c.cleanupData && len(ins.DataDir()) > 0 {
				for _, dataDir := range strings.Split(ins.DataDir(), ",") {
					dataPaths.Insert(path.Join(dataDir, "*"))
				}
			}

			if c.cleanupLog && len(ins.LogDir()) > 0 {
				for _, logDir := range strings.Split(ins.LogDir(), ",") {
					logPaths.Insert(path.Join(logDir, "*.log"))
				}
			}

			// clean tls data
			if c.cleanupTLS && !topo.BaseTopo().GlobalOptions.TLSEnabled {
				deployDir := spec.Abs(topo.BaseTopo().GlobalOptions.User, ins.DeployDir())
				tlsDir := filepath.Join(deployDir, spec.TLSCertKeyDir)
				tlsPath.Insert(tlsDir)
			}

			if c.delFileMap[ins.GetHost()] == nil {
				c.delFileMap[ins.GetHost()] = set.NewStringSet()
			}
			c.delFileMap[ins.GetHost()].Join(logPaths).Join(dataPaths).Join(tlsPath)
		}
	}
}

// monitorCleanupFoles get the files that need to be deleted in the mointor
func (c *cleanUpFiles) monitorCleanupFiles(topo spec.Topology) {
	monitoredOptions := topo.BaseTopo().MonitoredOptions
	if monitoredOptions == nil {
		return
	}
	user := topo.BaseTopo().GlobalOptions.User

	// get the host with monitor installed
	uniqueHosts, noAgentHosts := getMonitorHosts(topo)
	retainDataNodes := set.NewStringSet(c.retainDataNodes...)

	// monitoring agents
	for host := range uniqueHosts {
		// determine if host don't need to delete
		dataRetained := noAgentHosts.Exist(host) || retainDataNodes.Exist(host)
		if dataRetained {
			continue
		}

		deployDir := spec.Abs(user, monitoredOptions.DeployDir)

		// prevent duplicate directories
		dataPaths := set.NewStringSet()
		logPaths := set.NewStringSet()
		tlsPath := set.NewStringSet()

		// data dir would be empty for components which don't need it
		dataDir := monitoredOptions.DataDir
		if c.cleanupData && len(dataDir) > 0 {
			// the default data_dir is relative to deploy_dir
			if !strings.HasPrefix(dataDir, "/") {
				dataDir = filepath.Join(deployDir, dataDir)
			}
			dataPaths.Insert(path.Join(dataDir, "*"))
		}

		// log dir will always be with values, but might not used by the component
		logDir := spec.Abs(user, monitoredOptions.LogDir)
		if c.cleanupLog && len(logDir) > 0 {
			logPaths.Insert(path.Join(logDir, "*.log"))
		}

		// clean tls data
		if c.cleanupTLS && !topo.BaseTopo().GlobalOptions.TLSEnabled {
			tlsDir := filepath.Join(deployDir, spec.TLSCertKeyDir)
			tlsPath.Insert(tlsDir)
		}

		if c.delFileMap[host] == nil {
			c.delFileMap[host] = set.NewStringSet()
		}
		c.delFileMap[host].Join(logPaths).Join(dataPaths).Join(tlsPath)
	}
}
