package clusterqueue

import "errors"

func validAckAndClaimCommand(c QueueCommand) bool {
	if len(c.Attempts) == 0 || len(c.Attempts) > MaxSettlementBatch {
		return false
	}
	for _, attempt := range c.Attempts {
		if attempt == 0 {
			return false
		}
	}
	// The refill has exactly the same limits and session requirements as an
	// explicit ClaimBatch. ACK is implicit; no other settlement is accepted.
	c.Attempts = nil
	return validClaimBatchCommand(c)
}

func (m *Machine) ackAndClaim(c QueueCommand, q *queueState, sess *sessionState) outcome {
	for i, attempt := range c.Attempts {
		if !m.settle(q, sess, AckMessage, attempt) {
			return outcome{Settled: uint64(i), Code: errorsToCode[ErrUnknownAttempt], Ready: uint64(len(q.Ready))}
		}
	}
	last := q.claimBatch(c, sess)
	last.Settled = uint64(len(c.Attempts))
	last.ClaimCode = last.Code
	last.Code = ""
	return last
}

func (sess *sessionState) ackAndClaimOutcome(last outcome) QueueResponse {
	response := QueueResponse{Queue: QueueInfo{Ready: last.Ready}}
	if last.BatchCount != 0 {
		response = sess.claimBatchOutcome(last)
	}
	response.AckClaim = true
	response.Settled = last.Settled
	response.Code = last.Code
	response.ClaimCode = last.ClaimCode
	return response
}

func validEmptyClaimCode(code string) bool {
	return code == errorsToCode[ErrEmpty] || code == errorsToCode[ErrNoCredit] || code == errorsToCode[ErrIDsExhausted]
}

func decodeAckAndClaimResponse(response QueueResponse, body []byte) (QueueResponse, error) {
	legacy := response.Result
	if response.Settled > MaxSettlementBatch || response.Count > MaxClaimBatchMessages || len(response.Queues) != 0 ||
		response.Queue.Ready > maxMessages || legacy.MessageID != 0 || legacy.Delivery.Attempt != 0 ||
		legacy.Delivery.MessageID != 0 || legacy.Delivery.Redelivered || legacy.Delivery.Packed {
		return QueueResponse{}, ErrInvalidCommand
	}
	if response.Code != "" {
		if response.Code != errorsToCode[ErrUnknownAttempt] || response.Settled >= MaxSettlementBatch ||
			response.Count != 0 || response.DeliveryBatch || response.ClaimCode != "" || len(body) != 0 {
			return QueueResponse{}, ErrInvalidCommand
		}
		return response, nil
	}
	if response.Settled == 0 {
		return QueueResponse{}, ErrInvalidCommand
	}
	if response.Count == 0 {
		if response.DeliveryBatch || !validEmptyClaimCode(response.ClaimCode) || len(body) != 0 {
			return QueueResponse{}, ErrInvalidCommand
		}
		return response, nil
	}
	if !response.DeliveryBatch || response.ClaimCode != "" {
		return QueueResponse{}, ErrInvalidCommand
	}
	// The explicit discriminator keeps combined metadata out of the legacy
	// response path, while payload records retain the ClaimBatch wire layout.
	return decodeClaimBatchResponse(response, body)
}

func validateAckAndClaimOutcome(q *queueState, sess *sessionState) error {
	last := sess.Last
	invalid := errors.New("invalid latest ACK and claim result in snapshot")
	if sess.LastKey.Attempt != 0 || last.Attempt != 0 || last.Message != 0 || last.Settled > MaxSettlementBatch {
		return invalid
	}
	if last.Code != "" {
		if last.Code == errorsToCode[ErrUnknownAttempt] && last.Settled < MaxSettlementBatch &&
			last.BatchFirst == 0 && last.BatchCount == 0 && last.ClaimCode == "" {
			return nil
		}
		return invalid
	}
	if last.Settled == 0 {
		return invalid
	}
	// Validate the saved refill with the same bounds and outstanding-flight
	// requirements as ClaimBatch, without changing the retained ACK result.
	claim := *sess
	claim.LastKey.Kind = ClaimBatch
	claim.Last.Settled = 0
	claim.Last.Code = last.ClaimCode
	claim.Last.ClaimCode = ""
	return validateClaimBatchOutcome(q, &claim)
}
