package protocol

// SetBodyRelease transfers a body lease to a fresh decoded egress Message.
// Call exactly once before publication. Such a message must be passed by
// pointer through delivery and never shallow-copied, stored or republished.
// Ordinary messages leave this callback nil.
func (m *Message) SetBodyRelease(release func()) { m.releaseBody = release }

func (m *Message) ReleaseBody() {
	if m == nil || m.releaseBody == nil {
		return
	}
	release := m.releaseBody
	m.releaseBody = nil
	m.Body = nil
	release()
}

func (d *Delivery) ReleaseBody() {
	if d != nil {
		d.Message.ReleaseBody()
	}
}

func ReleaseDeliveryBodies(deliveries []*Delivery) {
	for _, delivery := range deliveries {
		delivery.ReleaseBody()
	}
}
