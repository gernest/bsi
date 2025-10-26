package prefix

import "encoding/binary"

func Encode(w []byte, value []byte) []byte {
	w = binary.AppendUvarint(w, uint64(len(value)))
	return append(w, value...)
}

func Decode(data []byte) (value, left []byte) {
	size, n := binary.Uvarint(data)
	value = data[n : size+uint64(n)]
	left = data[size+uint64(n):]
	return
}
