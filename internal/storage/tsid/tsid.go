package tsid

type ID = []Column

type Column struct {
	ID    uint64
	Value uint64
}
