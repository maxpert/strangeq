package server

import (
	"context"

	"github.com/maxpert/amqp-go/internal/clusterproposal"
	"github.com/maxpert/amqp-go/internal/clusterqueue"
)

type clusterOwnedPublisher interface {
	SupportsOwnedPublishes() bool
	SubmitOwned(context.Context, string, *clusterproposal.Owned) ([]byte, error)
}

func (b *ClusterBroker) ownedPublisher() clusterOwnedPublisher {
	owned, ok := b.cluster.(clusterOwnedPublisher)
	if ok && owned.SupportsOwnedPublishes() {
		return owned
	}
	return nil
}

func (b *ClusterBroker) submitPublishRecord(ctx context.Context, target clusterTarget, record clusterqueue.PublishRecord) (clusterqueue.QueueResponse, error) {
	if owned := b.ownedPublisher(); owned != nil {
		command, err := clusterqueue.EncodeOwnedPublishCommand(target.Queue, target.Incarnation, record)
		if err != nil {
			return clusterqueue.QueueResponse{}, err
		}
		wire, err := owned.SubmitOwned(ctx, target.Queue, command)
		if err != nil {
			return clusterqueue.QueueResponse{}, err
		}
		return clusterqueue.DecodeResponse(wire)
	}
	command, err := clusterqueue.EncodePublishCommand(target.Queue, target.Incarnation, record)
	if err != nil {
		return clusterqueue.QueueResponse{}, err
	}
	return b.executeEncoded(ctx, target.Queue, command, false)
}

func (b *ClusterBroker) submitPublishBatch(ctx context.Context, target clusterTarget, records []clusterqueue.PublishRecord) ([]byte, error) {
	if owned := b.ownedPublisher(); owned != nil {
		command, err := clusterqueue.EncodeOwnedPublishBatchRecords(target.Queue, target.Incarnation, records)
		if err != nil {
			return nil, err
		}
		return owned.SubmitOwned(ctx, target.Queue, command)
	}
	command, err := clusterqueue.EncodePublishBatchRecords(target.Queue, target.Incarnation, records)
	if err != nil {
		return nil, err
	}
	return b.cluster.Submit(ctx, target.Queue, command)
}
