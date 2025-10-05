package protocol

import (
	"fmt"
)

// TxSelectMethod represents the tx.select method
type TxSelectMethod struct {
	// tx.select has no parameters
}

// Serialize serializes the tx.select method
func (m *TxSelectMethod) Serialize() []byte {
	// tx.select has no method-specific content
	return []byte{}
}

// DeserializeTxSelectMethod deserializes a tx.select method
func DeserializeTxSelectMethod(data []byte) (*TxSelectMethod, error) {
	if len(data) > 0 {
		return nil, fmt.Errorf("tx.select method should have no parameters, got %d bytes", len(data))
	}
	return &TxSelectMethod{}, nil
}

// TxSelectOKMethod represents the tx.select-ok method
type TxSelectOKMethod struct {
	// tx.select-ok has no parameters
}

// Serialize serializes the tx.select-ok method
func (m *TxSelectOKMethod) Serialize() []byte {
	// tx.select-ok has no method-specific content
	return []byte{}
}

// DeserializeTxSelectOKMethod deserializes a tx.select-ok method
func DeserializeTxSelectOKMethod(data []byte) (*TxSelectOKMethod, error) {
	if len(data) > 0 {
		return nil, fmt.Errorf("tx.select-ok method should have no parameters, got %d bytes", len(data))
	}
	return &TxSelectOKMethod{}, nil
}

// TxCommitMethod represents the tx.commit method
type TxCommitMethod struct {
	// tx.commit has no parameters
}

// Serialize serializes the tx.commit method
func (m *TxCommitMethod) Serialize() []byte {
	// tx.commit has no method-specific content
	return []byte{}
}

// DeserializeTxCommitMethod deserializes a tx.commit method
func DeserializeTxCommitMethod(data []byte) (*TxCommitMethod, error) {
	if len(data) > 0 {
		return nil, fmt.Errorf("tx.commit method should have no parameters, got %d bytes", len(data))
	}
	return &TxCommitMethod{}, nil
}

// TxCommitOKMethod represents the tx.commit-ok method
type TxCommitOKMethod struct {
	// tx.commit-ok has no parameters
}

// Serialize serializes the tx.commit-ok method
func (m *TxCommitOKMethod) Serialize() []byte {
	// tx.commit-ok has no method-specific content
	return []byte{}
}

// DeserializeTxCommitOKMethod deserializes a tx.commit-ok method
func DeserializeTxCommitOKMethod(data []byte) (*TxCommitOKMethod, error) {
	if len(data) > 0 {
		return nil, fmt.Errorf("tx.commit-ok method should have no parameters, got %d bytes", len(data))
	}
	return &TxCommitOKMethod{}, nil
}

// TxRollbackMethod represents the tx.rollback method
type TxRollbackMethod struct {
	// tx.rollback has no parameters
}

// Serialize serializes the tx.rollback method
func (m *TxRollbackMethod) Serialize() []byte {
	// tx.rollback has no method-specific content
	return []byte{}
}

// DeserializeTxRollbackMethod deserializes a tx.rollback method
func DeserializeTxRollbackMethod(data []byte) (*TxRollbackMethod, error) {
	if len(data) > 0 {
		return nil, fmt.Errorf("tx.rollback method should have no parameters, got %d bytes", len(data))
	}
	return &TxRollbackMethod{}, nil
}

// TxRollbackOKMethod represents the tx.rollback-ok method
type TxRollbackOKMethod struct {
	// tx.rollback-ok has no parameters
}

// Serialize serializes the tx.rollback-ok method
func (m *TxRollbackOKMethod) Serialize() []byte {
	// tx.rollback-ok has no method-specific content
	return []byte{}
}

// DeserializeTxRollbackOKMethod deserializes a tx.rollback-ok method
func DeserializeTxRollbackOKMethod(data []byte) (*TxRollbackOKMethod, error) {
	if len(data) > 0 {
		return nil, fmt.Errorf("tx.rollback-ok method should have no parameters, got %d bytes", len(data))
	}
	return &TxRollbackOKMethod{}, nil
}

// Transaction method frame constructors

// NewTxSelectFrame creates a tx.select method frame
func NewTxSelectFrame(channelID uint16) *Frame {
	method := &TxSelectMethod{}
	methodData := method.Serialize()
	return EncodeMethodFrameForChannel(channelID, 90, TxSelect, methodData)
}

// NewTxSelectOKFrame creates a tx.select-ok method frame
func NewTxSelectOKFrame(channelID uint16) *Frame {
	method := &TxSelectOKMethod{}
	methodData := method.Serialize()
	return EncodeMethodFrameForChannel(channelID, 90, TxSelectOK, methodData)
}

// NewTxCommitFrame creates a tx.commit method frame
func NewTxCommitFrame(channelID uint16) *Frame {
	method := &TxCommitMethod{}
	methodData := method.Serialize()
	return EncodeMethodFrameForChannel(channelID, 90, TxCommit, methodData)
}

// NewTxCommitOKFrame creates a tx.commit-ok method frame
func NewTxCommitOKFrame(channelID uint16) *Frame {
	method := &TxCommitOKMethod{}
	methodData := method.Serialize()
	return EncodeMethodFrameForChannel(channelID, 90, TxCommitOK, methodData)
}

// NewTxRollbackFrame creates a tx.rollback method frame
func NewTxRollbackFrame(channelID uint16) *Frame {
	method := &TxRollbackMethod{}
	methodData := method.Serialize()
	return EncodeMethodFrameForChannel(channelID, 90, TxRollback, methodData)
}

// NewTxRollbackOKFrame creates a tx.rollback-ok method frame
func NewTxRollbackOKFrame(channelID uint16) *Frame {
	method := &TxRollbackOKMethod{}
	methodData := method.Serialize()
	return EncodeMethodFrameForChannel(channelID, 90, TxRollbackOK, methodData)
}

// Transaction method parsing helper

// ParseTransactionMethod parses transaction class methods based on method ID
func ParseTransactionMethod(methodID uint16, data []byte) (interface{}, error) {
	switch methodID {
	case TxSelect:
		return DeserializeTxSelectMethod(data)
	case TxSelectOK:
		return DeserializeTxSelectOKMethod(data)
	case TxCommit:
		return DeserializeTxCommitMethod(data)
	case TxCommitOK:
		return DeserializeTxCommitOKMethod(data)
	case TxRollback:
		return DeserializeTxRollbackMethod(data)
	case TxRollbackOK:
		return DeserializeTxRollbackOKMethod(data)
	default:
		return nil, fmt.Errorf("unknown transaction method ID: %d", methodID)
	}
}