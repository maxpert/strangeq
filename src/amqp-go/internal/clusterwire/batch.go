package clusterwire

import (
	"encoding/binary"
	"errors"
	"math"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const maxBatch = 1024

var errBatch = errors.New("invalid peer Raft batch")

// A batch consists of its group, message count, and length-delimited protobufs.
// Frame size is validated before allocation and every byte belongs to one frame.
func encodeBatch(group uint64, messages []*raftpb.Message, limit int64) ([]byte, error) {
	if group == 0 || len(messages) == 0 || len(messages) > maxBatch {
		return nil, errBatch
	}
	size := int64(10)
	for _, msg := range messages {
		if msg == nil {
			return nil, errBatch
		}
		messageSize := int64(proto.Size(msg))
		if messageSize == 0 {
			return nil, errBatch
		}
		size += messageSize + 4
		if size > limit || messageSize > math.MaxUint32 {
			return nil, ErrTooLarge
		}
	}
	output := make([]byte, 10, size)
	binary.BigEndian.PutUint64(output[:8], group)
	binary.BigEndian.PutUint16(output[8:], uint16(len(messages)))
	for _, msg := range messages {
		output = binary.BigEndian.AppendUint32(output, uint32(proto.Size(msg)))
		var err error
		output, err = proto.MarshalOptions{}.MarshalAppend(output, msg)
		if err != nil {
			return nil, err
		}
	}
	return output, nil
}

func decodeBatch(body []byte) (uint64, []*raftpb.Message, error) {
	if len(body) < 10 {
		return 0, nil, errBatch
	}
	group := binary.BigEndian.Uint64(body[:8])
	count := int(binary.BigEndian.Uint16(body[8:10]))
	body = body[10:]
	if group == 0 || count < 1 || count > maxBatch || count > len(body)/5 {
		return 0, nil, errBatch
	}
	messages := make([]*raftpb.Message, 0, count)
	for range count {
		if len(body) < 4 {
			return 0, nil, errBatch
		}
		length := uint64(binary.BigEndian.Uint32(body[:4]))
		body = body[4:]
		if length == 0 || length > uint64(len(body)) {
			return 0, nil, errBatch
		}
		var msg raftpb.Message
		if err := proto.Unmarshal(body[:length], &msg); err != nil {
			return 0, nil, errBatch
		}
		body = body[length:]
		messages = append(messages, &msg)
	}
	if len(body) != 0 {
		return 0, nil, errBatch
	}
	return group, messages, nil
}
