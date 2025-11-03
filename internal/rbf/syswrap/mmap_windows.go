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

package syswrap

import (
	"os"
	"syscall"
	"unsafe"
)

func mmap(f int, size int, write bool) ([]byte, error) {
	low, high := uint32(size), uint32(size>>32)
	protect := syscall.PAGE_READONLY
	access := syscall.FILE_MAP_READ

	if write {
		protect = syscall.PAGE_READWRITE
		access = syscall.FILE_MAP_WRITE
	}
	h, errno := syscall.CreateFileMapping(syscall.Handle(f), nil, uint32(protect), high, low, nil)
	if h == 0 {
		return nil, os.NewSyscallError("CreateFileMapping", errno)
	}

	addr, errno := syscall.MapViewOfFile(h, uint32(access), 0, 0, uintptr(size))
	if addr == 0 {
		return nil, os.NewSyscallError("MapViewOfFile", errno)
	}

	if err := syscall.CloseHandle(syscall.Handle(h)); err != nil {
		return nil, os.NewSyscallError("CloseHandle", err)
	}

	return (*[maxMapSize]byte)(unsafe.Pointer(addr))[:size], nil
}

func munmap(b []byte) error {
	if err := syscall.UnmapViewOfFile((uintptr)(unsafe.Pointer(&b[0]))); err != nil {
		return os.NewSyscallError("UnmapViewOfFile", err)
	}
	return nil
}

func msync(b []byte) error {
	// TODO: Figure out how to do msync on Windows.
	return nil
}
