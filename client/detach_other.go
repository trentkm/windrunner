//go:build !unix

package client

import "syscall"

func daemonSysProcAttr() *syscall.SysProcAttr { return nil }
