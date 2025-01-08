package record

import "simple-db-go/types"

type RecordID struct {
	blockNumber types.BlockNumber
	slotNumber  SlotNumber
}

func NewRecordID(blockNumber types.BlockNumber, slotNumber SlotNumber) RecordID {
	return RecordID{blockNumber: blockNumber, slotNumber: slotNumber}
}
