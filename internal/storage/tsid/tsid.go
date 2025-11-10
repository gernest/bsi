package tsid

type B struct {
	B []ID
}

type ID = []Column

type Column struct {
	ID    uint64
	Value uint64
}
