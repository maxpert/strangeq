package broker

// This file holds the aggregate binding listing (BOB-4). It is deliberately
// lock-free on the request path: ListBindings is one
// interfaces.MetadataStore.SnapshotBindings call that reads the authoritative
// queue-binding sync.Map (no per-binding disk read) plus a single directory
// walk for the rare exchange-to-exchange bindings. It takes no Channel or
// Connection lock (spec mgmt-04 §1.1/§4).

// BindingView is the management view of one binding (DATA-011/DATA-012).
//
// Arguments is the raw AMQP field table; the management layer maps it to JSON
// (FR-CONV-047) and computes properties_key (FR-API-090, spec §1.3). Vhost is
// always "/" (D-1/DATA-012) and is therefore not carried here.
type BindingView struct {
	Source          string // exchange name ("" is the default exchange; see §8)
	Destination     string // queue name or exchange name
	DestinationType string // "queue" | "exchange"
	RoutingKey      string
	Arguments       map[string]interface{}
}

// ListBindings returns every binding in the vhost, in storage order. It takes
// no broker lock and no channel/connection lock; it is one
// interfaces.MetadataStore.SnapshotBindings call. The order is not specified;
// the management layer sorts.
func (b *StorageBroker) ListBindings() ([]BindingView, error) {
	queueBindings, exchangeBindings, err := b.storage.SnapshotBindings()
	if err != nil {
		return nil, err
	}

	out := make([]BindingView, 0, len(queueBindings)+len(exchangeBindings))
	for _, qb := range queueBindings {
		out = append(out, BindingView{
			Source:          qb.ExchangeName,
			Destination:     qb.QueueName,
			DestinationType: "queue",
			RoutingKey:      qb.RoutingKey,
			Arguments:       qb.Arguments,
		})
	}
	for _, eb := range exchangeBindings {
		out = append(out, BindingView{
			Source:          eb.Source,
			Destination:     eb.Destination,
			DestinationType: "exchange",
			RoutingKey:      eb.RoutingKey,
			Arguments:       eb.Arguments,
		})
	}
	return out, nil
}
