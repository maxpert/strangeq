package server

import (
	"fmt"

	"github.com/maxpert/amqp-go/protocol"
	"go.uber.org/zap"
)

// processTransactionMethod processes transaction class methods
func (s *Server) processTransactionMethod(conn *protocol.Connection, channelID uint16, methodID uint16, payload []byte) error {
	switch methodID {
	case protocol.TxSelect: // Method ID 10 for tx class
		return s.handleTxSelect(conn, channelID, payload)
	case protocol.TxCommit: // Method ID 20 for tx class
		return s.handleTxCommit(conn, channelID, payload)
	case protocol.TxRollback: // Method ID 30 for tx class
		return s.handleTxRollback(conn, channelID, payload)
	default:
		s.Log.Warn("Unknown transaction method ID",
			zap.Uint16("method_id", methodID),
			zap.Uint16("channel_id", channelID),
			zap.String("connection_id", conn.ID))
		return fmt.Errorf("unknown transaction method ID: %d", methodID)
	}
}

// handleTxSelect handles the tx.select method
func (s *Server) handleTxSelect(conn *protocol.Connection, channelID uint16, payload []byte) error {
	// Deserialize the tx.select method
	_, err := protocol.DeserializeTxSelectMethod(payload)
	if err != nil {
		s.Log.Error("Failed to deserialize tx.select",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return err
	}

	s.Log.Info("Transaction select requested",
		zap.String("connection_id", conn.ID),
		zap.Uint16("channel_id", channelID))

	// Put channel into transactional mode
	if s.TransactionManager != nil {
		err = s.TransactionManager.Select(channelID)
		if err != nil {
			s.Log.Error("Failed to select transaction mode",
				zap.Error(err),
				zap.Uint16("channel_id", channelID))
			return err
		}
	} else {
		s.Log.Warn("No transaction manager available")
		return fmt.Errorf("transactions not supported")
	}

	// Send tx.select-ok response
	response := protocol.NewTxSelectOKFrame(channelID)
	return protocol.WriteFrameToConnection(conn, response)
}

// handleTxCommit handles the tx.commit method
func (s *Server) handleTxCommit(conn *protocol.Connection, channelID uint16, payload []byte) error {
	// Deserialize the tx.commit method
	_, err := protocol.DeserializeTxCommitMethod(payload)
	if err != nil {
		s.Log.Error("Failed to deserialize tx.commit",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return err
	}

	s.Log.Info("Transaction commit requested",
		zap.String("connection_id", conn.ID),
		zap.Uint16("channel_id", channelID))

	// Commit pending operations
	if s.TransactionManager != nil {
		err = s.TransactionManager.Commit(channelID)
		if err != nil {
			s.Log.Error("Failed to commit transaction",
				zap.Error(err),
				zap.Uint16("channel_id", channelID))
			return err
		}
	} else {
		return fmt.Errorf("transactions not supported")
	}

	// Send tx.commit-ok response
	response := protocol.NewTxCommitOKFrame(channelID)
	return protocol.WriteFrameToConnection(conn, response)
}

// handleTxRollback handles the tx.rollback method
func (s *Server) handleTxRollback(conn *protocol.Connection, channelID uint16, payload []byte) error {
	// Deserialize the tx.rollback method
	_, err := protocol.DeserializeTxRollbackMethod(payload)
	if err != nil {
		s.Log.Error("Failed to deserialize tx.rollback",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return err
	}

	s.Log.Info("Transaction rollback requested",
		zap.String("connection_id", conn.ID),
		zap.Uint16("channel_id", channelID))

	// Rollback pending operations
	if s.TransactionManager != nil {
		err = s.TransactionManager.Rollback(channelID)
		if err != nil {
			s.Log.Error("Failed to rollback transaction",
				zap.Error(err),
				zap.Uint16("channel_id", channelID))
			return err
		}
	} else {
		return fmt.Errorf("transactions not supported")
	}

	// Send tx.rollback-ok response
	response := protocol.NewTxRollbackOKFrame(channelID)
	return protocol.WriteFrameToConnection(conn, response)
}
