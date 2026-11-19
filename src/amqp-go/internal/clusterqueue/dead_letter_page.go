package clusterqueue

import "slices"

const (
	maxDeadLetterPageMessages = 32
	DeadLetterPageBytes       = 2 << 20
)

type DeadLetterPage struct {
	Through uint64
	Entries []DeadLetterPageEntry
}

type DeadLetterPageEntry struct {
	DeadLetterTransfer
	Packed bool `json:",omitempty"`
	Size   int
	Body   []byte `json:"-"`
}

// CaptureDeadLetterPage fixes a high-water mark for each sweep so continuous
// arrivals cannot starve old retries. The encoder requires a journal capture pin.
func (m *Machine) CaptureDeadLetterPage(after, through uint64) (func() ([]byte, error), int, int) {
	s := &m.state
	page := DeadLetterPage{Through: through}
	var bodies []Delivery
	wireBytes, logicalBytes := 0, 0
	capture := func(ids []uint64) {
		for _, id := range ids {
			if id > page.Through {
				break
			}
			entry, ok := s.DeadLetters[id]
			if !ok {
				continue
			}
			size := entry.Message.logicalBodySize()
			if len(bodies) >= maxDeadLetterPageMessages || len(bodies) != 0 && size > DeadLetterPageBytes-logicalBytes {
				break
			}
			delivery := entry.Message.delivery(0)
			bodies = append(bodies, delivery)
			page.Entries = append(page.Entries, DeadLetterPageEntry{
				DeadLetterTransfer: entry.DeadLetterTransfer, Packed: delivery.Packed, Size: delivery.bodySize(),
			})
			wireBytes += delivery.bodySize()
			logicalBytes += size
		}
	}
	start, exact := slices.BinarySearch(s.deadLetterIDs, after)
	if exact {
		start++
	}
	capture(s.deadLetterIDs[start:])
	if len(bodies) == 0 {
		page.Through = s.LastDeadLetter
		capture(s.deadLetterIDs)
	}
	return func() ([]byte, error) {
		frame, err := encodeFrameHeader(page, wireBytes)
		if err != nil {
			return nil, err
		}
		for _, body := range bodies {
			frame, err = body.appendBody(frame)
			if err != nil {
				return nil, err
			}
		}
		return frame, nil
	}, wireBytes, logicalBytes
}

func DecodeDeadLetterPage(frame []byte) (DeadLetterPage, error) {
	var page DeadLetterPage
	body, err := decodeFrame(frame, &page)
	if err != nil || len(page.Entries) > maxDeadLetterPageMessages {
		return DeadLetterPage{}, ErrInvalidCommand
	}
	var last uint64
	for i := range page.Entries {
		entry := &page.Entries[i]
		if entry.ID <= last || entry.ID > page.Through || entry.Size < 0 || entry.Size > min(len(body), maxMessageBytes) {
			return DeadLetterPage{}, ErrInvalidCommand
		}
		entry.Body, body = body[:entry.Size:entry.Size], body[entry.Size:]
		last = entry.ID
	}
	if len(body) != 0 {
		return DeadLetterPage{}, ErrInvalidCommand
	}
	return page, nil
}
