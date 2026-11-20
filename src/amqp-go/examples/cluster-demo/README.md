# Three-node cluster demo

From the repository root, run the launcher with the Go version listed in the
[module file](../../go.mod). It builds StrangeQ and starts three brokers on loopback.

```sh
./src/amqp-go/examples/cluster-demo/run.sh
```

Open one of the printed management URLs and sign in with the generated
credentials. Each broker serves StrangeQ's embedded management UI, including
Overview, Queues, Exchanges, Metrics, and Nodes. The launcher also prints an AMQP
URL for each broker.

1. In **Nodes**, wait for three running, ready brokers and three observed active
   voters for every Raft group.
2. Find the current catalog leader in the group table. Click **Stop** for that
   node and confirm. If that broker serves the current tab, open another
   broker's management URL first. The same panel and credentials are available
   there.
3. Click **Start** for the stopped node. Watch it return with the same identity
   and its existing journal. Wait for all groups to show three observed active
   voters before stopping another broker.

The demo allows one broker to be stopped at a time. **Stop** sends a graceful
shutdown signal to a broker owned by the launcher. **Start** reopens its data
with the same voter membership. The launcher handles process supervision over
a private Unix socket. Every browser request goes directly to a broker.

Press Ctrl-C in the launcher terminal to stop its brokers. The
launcher removes its temporary binaries, logs, and data after the processes exit.
The [management guide](../../../../docs/MANAGEMENT.md#cluster-administration)
describes the cluster inspection available on a broker.
