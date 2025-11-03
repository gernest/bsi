// Copyright 2017 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build !windows && !plan9 && !js

package syswrap

import (
	"golang.org/x/sys/unix"
)

func mmap(f int, length int, write bool) ([]byte, error) {
	mtype := unix.PROT_READ
	if write {
		mtype |= unix.PROT_WRITE
	}
	return unix.Mmap(f, 0, length, mtype, unix.MAP_SHARED)
}

func munmap(b []byte) (err error) {
	return unix.Munmap(b)
}

// msync writes any modified data to persistent storage.
func msync(b []byte) error {
	return unix.Msync(b, unix.MS_SYNC)
}
