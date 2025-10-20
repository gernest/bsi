package magic

import "unsafe"

func Slice(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

func String(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}

func ReinterpretSlice[Out, T any](b []T) []Out {
	if cap(b) == 0 {
		return nil
	}
	out := (*Out)(unsafe.Pointer(&b[:1][0]))

	lenBytes := len(b) * int(unsafe.Sizeof(b[0]))
	capBytes := cap(b) * int(unsafe.Sizeof(b[0]))

	lenOut := lenBytes / int(unsafe.Sizeof(*out))
	capOut := capBytes / int(unsafe.Sizeof(*out))

	return unsafe.Slice(out, capOut)[:lenOut]
}
