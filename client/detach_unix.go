//go:build unix

package client

import "syscall"

// daemonSysProcAttr detaches the daemon into its own session, away from
// the launching terminal's controlling tty and its SIGHUP.
func daemonSysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setsid: true}
}
