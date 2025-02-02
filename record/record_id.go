package record

import "simple-db-go/types"

type RecordID struct {
	blockNumber types.BlockNumber
	slotNumber  types.SlotNumber
}

func NewRecordID(blockNumber types.BlockNumber, slotNumber types.SlotNumber) RecordID {
	return RecordID{blockNumber: blockNumber, slotNumber: slotNumber}
}

func (ri RecordID) GetBlockNumber() types.BlockNumber {
	return ri.blockNumber
}

func (ri RecordID) GetSlotNumber() types.SlotNumber {
	return ri.slotNumber
}
