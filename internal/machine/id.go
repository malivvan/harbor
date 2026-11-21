// Package machineid provides support for reading the unique machine id of most OSs (without admin privileges).
//
// https://github.com/denisbrodbeck/machineid
//
// https://godoc.org/github.com/denisbrodbeck/machineid/cmd/machineid
//
// This package is Cross-Platform (tested on Win7+, Debian 8+, Ubuntu 14.04+, OS X 10.6+, FreeBSD 11+)
// and does not use any internal hardware IDs (no MAC, BIOS, or CPU).
//
// Returned machine IDs are generally stable for the OS installation
// and usually stay the same after updates or hardware changes.
//
// This package allows sharing of machine IDs in a secure way by
// calculating HMAC-SHA256 over a user provided app ID, which is keyed by the machine id.
//
// Caveat: Image-based environments have usually the same machine-id (perfect clone).
// Linux users can generate a new id with `dbus-uuidgen` and put the id into
// `/var/lib/dbus/machine-id` and `/etc/machine-id`.
// Windows users can use the `sysprep` toolchain to create images, which produce valid images ready for distribution.
package machine // import "github.com/denisbrodbeck/machineid"

import (
	"crypto/hmac"
	"crypto/sha256"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"

	"github.com/malivvan/storm/internal"
)

// ID returns a hashed version of the machine ID in a cryptographically secure way,
// using a fixed, application-specific key.
// Internally, this function calculates HMAC-SHA256 of the application ID, keyed by the machine ID.
func ID() ([]byte, error) {
	id, err := machineID()
	if err != nil {
		return nil, fmt.Errorf("machineid: %v", err)
	}
	return protect(internal.AppID, id), nil
}

// run wraps `exec.Command` with easy access to stdout and stderr.
func run(stdout, stderr io.Writer, cmd string, args ...string) error {
	c := exec.Command(cmd, args...)
	c.Stdin = os.Stdin
	c.Stdout = stdout
	c.Stderr = stderr
	return c.Run()
}

// protect calculates HMAC-SHA256 of the application ID, keyed by the machine ID and returns a hex-encoded string.
func protect(appID, id string) []byte {
	mac := hmac.New(sha256.New, []byte(id))
	mac.Write([]byte(appID))
	return mac.Sum(nil)
}

func readFile(filename string) ([]byte, error) {
	return ioutil.ReadFile(filename)
}

func trim(s string) string {
	return strings.TrimSpace(strings.Trim(s, "\n"))
}
