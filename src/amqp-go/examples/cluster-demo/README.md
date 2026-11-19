# Three-node cluster demo

From the repository root, run the launcher with the Go version listed in the
[module file](../../go.mod). It builds the brokers and panel for your machine.

```sh
./src/amqp-go/examples/cluster-demo/run.sh
```

Open the printed panel URL and sign in with the generated credentials. The
launcher uses available loopback ports and prints an AMQP URL for each broker.

1. In **Nodes**, wait for three running, ready brokers and three observed active
   voters for every Raft group.
2. Find the current catalog leader in the group table. Click **Stop** for that
   node and confirm. The panel stays open while the other brokers elect a leader.
3. Click **Start** for the stopped node. Watch it return with the same identity
   and its existing journal. Wait for all groups to show three observed active
   voters before stopping another broker.

The demo allows one broker to be stopped at a time. **Stop** sends a graceful
shutdown signal to the broker process owned by the launcher. Its voter
membership is retained. **Start** reopens that broker's data. The panel reports
process state and the latest observations available from the brokers. The
observed active voter counts can briefly lag a stop or restart.

Press Ctrl-C in the launcher terminal to stop the panel and its brokers. The
launcher removes its temporary binaries, logs, and data after the processes exit.
The [management guide](../../../../docs/MANAGEMENT.md#cluster-administration)
describes the cluster inspection available on a broker.
