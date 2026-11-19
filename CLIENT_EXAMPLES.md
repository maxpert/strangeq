# Client Examples

Examples for Python, Node.js, and Go using the default local broker at
`localhost:5672`, with authentication disabled. The `guest` credentials used by
client defaults are not a configured production account. For authenticated
deployments, supply your own credentials and use [TLS](docs/TLS.md).

Run a consumer before publishing to a fanout or topic exchange: messages sent
before its temporary queue is bound have no destination. The basic examples
share a durable `task_queue`. All three languages can use the same queue.
Fanout and topic examples use non-durable exchanges and temporary queues.

Basic publishers wait for broker confirms. A confirm is a broker acknowledgement,
not proof that a consumer processed the message. Persistent messages, durable
queues, and the configured [storage durability](src/amqp-go/config.sample.yaml)
determine restart behavior. The routing and RPC examples illustrate the API.
production applications also need reconnect handling, bounded work, and explicit
error handling. RPC examples allow one outstanding call per client.

## Table of Contents

- [Python (pika)](#python-pika)
- [Node.js (amqplib)](#nodejs-amqplib)
- [Go (amqp091-go)](#go-amqp091-go)

---

## Python (pika)

### Installation

```bash
pip install pika
```

### Basic Publisher

```python
import pika

def publish_message():
    # Connect to StrangeQ
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost')
    )
    channel = connection.channel()

    # Declare queue
    channel.queue_declare(queue='task_queue', durable=True)

    # Ask the broker to confirm accepted publishes.
    channel.confirm_delivery()

    # Publish message
    message = "Hello from Python!"
    channel.basic_publish(
        exchange='',
        routing_key='task_queue',
        body=message,
        properties=pika.BasicProperties(
            delivery_mode=2,  # make message persistent
        )
    )

    print(f" [x] Sent '{message}'")
    connection.close()

if __name__ == '__main__':
    publish_message()
```

### Basic Consumer

```python
import pika

def callback(ch, method, properties, body):
    print(f" [x] Received {body.decode()}")
    # Acknowledge the message
    ch.basic_ack(delivery_tag=method.delivery_tag)

def consume_messages():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost')
    )
    channel = connection.channel()

    # Declare queue
    channel.queue_declare(queue='task_queue', durable=True)

    # Fair dispatch - don't give more than one message at a time
    channel.basic_qos(prefetch_count=1)

    # Start consuming
    channel.basic_consume(
        queue='task_queue',
        on_message_callback=callback
    )

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    try:
        consume_messages()
    except KeyboardInterrupt:
        print('Interrupted')
```

### Publish/Subscribe with Fanout Exchange

These functions are separate publisher and subscriber entry points. Save the
block in a file and call `receive_logs()` for a subscriber or `publish_logs()`
for a publisher. Use separate processes.

```python
import pika

# Publisher
def publish_logs():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost')
    )
    channel = connection.channel()

    # Declare fanout exchange
    channel.exchange_declare(exchange='logs', exchange_type='fanout')

    message = "Log message"
    channel.basic_publish(exchange='logs', routing_key='', body=message)

    print(f" [x] Sent '{message}'")
    connection.close()

# Subscriber
def receive_logs():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost')
    )
    channel = connection.channel()

    channel.exchange_declare(exchange='logs', exchange_type='fanout')

    # Create exclusive queue for this consumer
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue

    # Bind queue to exchange
    channel.queue_bind(exchange='logs', queue=queue_name)

    def callback(ch, method, properties, body):
        print(f" [x] {body.decode()}")

    channel.basic_consume(
        queue=queue_name,
        on_message_callback=callback,
        auto_ack=True
    )

    print(' [*] Waiting for logs. To exit press CTRL+C')
    channel.start_consuming()
```

### Topic Exchange (Routing)

```python
import pika

# Publisher
def publish_to_topic(routing_key, message):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost')
    )
    channel = connection.channel()

    channel.exchange_declare(exchange='topic_logs', exchange_type='topic')

    channel.basic_publish(
        exchange='topic_logs',
        routing_key=routing_key,
        body=message
    )

    print(f" [x] Sent '{routing_key}':'{message}'")
    connection.close()

# Example: publish_to_topic('kern.critical', 'A critical kernel error')

# Consumer
def consume_from_topic(binding_keys):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost')
    )
    channel = connection.channel()

    channel.exchange_declare(exchange='topic_logs', exchange_type='topic')

    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue

    # Bind queue with multiple routing patterns
    for binding_key in binding_keys:
        channel.queue_bind(
            exchange='topic_logs',
            queue=queue_name,
            routing_key=binding_key
        )

    def callback(ch, method, properties, body):
        print(f" [x] {method.routing_key}:{body.decode()}")

    channel.basic_consume(
        queue=queue_name,
        on_message_callback=callback,
        auto_ack=True
    )

    print(' [*] Waiting for logs. To exit press CTRL+C')
    channel.start_consuming()

# Example: consume_from_topic(['kern.*', '*.critical'])
```

### RPC Pattern

Start `start_rpc_server()` in one process. In another, create a
`FibonacciRpcClient`, call `client.call(10)`, and call `client.close()` in a
`finally` block. Use small nonnegative inputs. Recursive Fibonacci is a teaching
example with exponential work. A timeout does not cancel work on the server.

```python
import pika
import time
import uuid

# RPC Client
class FibonacciRpcClient:
    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters('localhost')
        )
        self.channel = self.connection.channel()

        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True
        )

        self.response = None
        self.corr_id = None

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, n, timeout=10):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='',
            routing_key='rpc_queue',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=str(n)
        )

        deadline = time.monotonic() + timeout
        while self.response is None:
            if time.monotonic() >= deadline:
                raise TimeoutError("RPC response timed out")
            self.connection.process_data_events(time_limit=0.1)

        return int(self.response)

    def close(self):
        self.connection.close()

# RPC Server
def fib(n):
    if n == 0:
        return 0
    elif n == 1:
        return 1
    else:
        return fib(n-1) + fib(n-2)

def on_request(ch, method, props, body):
    n = int(body)
    print(f" [.] fib({n})")
    response = fib(n)

    ch.basic_publish(
        exchange='',
        routing_key=props.reply_to,
        properties=pika.BasicProperties(correlation_id=props.correlation_id),
        body=str(response)
    )
    ch.basic_ack(delivery_tag=method.delivery_tag)

def start_rpc_server():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost')
    )
    channel = connection.channel()
    channel.queue_declare(queue='rpc_queue')
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='rpc_queue', on_message_callback=on_request)

    print(" [x] Awaiting RPC requests")
    channel.start_consuming()
```

---

## Node.js (amqplib)

The basic examples invoke their entry point. Fanout, topic, and RPC blocks define
functions. Add a call to the desired function and handle its promise rejection.
Run publishers and consumers in separate processes.

### Installation

```bash
npm install amqplib
```

### Basic Publisher

```javascript
const amqp = require('amqplib');

async function publishMessage() {
    try {
        const connection = await amqp.connect('amqp://localhost');
        const channel = await connection.createConfirmChannel();

        const queue = 'task_queue';
        const message = 'Hello from Node.js!';

        await channel.assertQueue(queue, {
            durable: true
        });

        channel.sendToQueue(queue, Buffer.from(message), {
            persistent: true
        });

        await channel.waitForConfirms();
        console.log(` [x] Sent '${message}'`);
        await connection.close();
    } catch (error) {
        console.error(error);
    }
}

publishMessage();
```

### Basic Consumer

```javascript
const amqp = require('amqplib');

async function consumeMessages() {
    try {
        const connection = await amqp.connect('amqp://localhost');
        const channel = await connection.createChannel();

        const queue = 'task_queue';

        await channel.assertQueue(queue, {
            durable: true
        });

        // Fair dispatch
        await channel.prefetch(1);

        console.log(' [*] Waiting for messages. To exit press CTRL+C');

        await channel.consume(queue, (msg) => {
            if (msg === null) return; // consumer cancelled
            const content = msg.content.toString();
            console.log(` [x] Received '${content}'`);

            // Simulate work
            setTimeout(() => {
                console.log(' [x] Done');
                channel.ack(msg);
            }, 1000);
        });
    } catch (error) {
        console.error(error);
    }
}

consumeMessages();
```

### Publish/Subscribe with Fanout Exchange

```javascript
const amqp = require('amqplib');

// Publisher
async function publishLogs() {
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createConfirmChannel();

    const exchange = 'logs';
    const message = 'Log message from Node.js';

    await channel.assertExchange(exchange, 'fanout', {
        durable: false
    });

    channel.publish(exchange, '', Buffer.from(message));
    await channel.waitForConfirms();
    console.log(` [x] Sent '${message}'`);
    await connection.close();
}

// Subscriber
async function receiveLogs() {
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();

    const exchange = 'logs';

    await channel.assertExchange(exchange, 'fanout', {
        durable: false
    });

    const q = await channel.assertQueue('', {
        exclusive: true
    });

    console.log(' [*] Waiting for logs. To exit press CTRL+C');

    await channel.bindQueue(q.queue, exchange, '');

    await channel.consume(q.queue, (msg) => {
        if (msg === null) return;
        console.log(` [x] ${msg.content.toString()}`);
    }, {
        noAck: true
    });
}
```

### Topic Exchange (Routing)

```javascript
const amqp = require('amqplib');

// Publisher
async function publishToTopic(routingKey, message) {
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createConfirmChannel();

    const exchange = 'topic_logs';

    await channel.assertExchange(exchange, 'topic', {
        durable: false
    });

    channel.publish(exchange, routingKey, Buffer.from(message));
    await channel.waitForConfirms();
    console.log(` [x] Sent '${routingKey}':'${message}'`);
    await connection.close();
}

// Example: publishToTopic('kern.critical', 'A critical kernel error');

// Consumer
async function consumeFromTopic(bindingKeys) {
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();

    const exchange = 'topic_logs';

    await channel.assertExchange(exchange, 'topic', {
        durable: false
    });

    const q = await channel.assertQueue('', {
        exclusive: true
    });

    console.log(' [*] Waiting for logs. To exit press CTRL+C');

    for (const key of bindingKeys) {
        await channel.bindQueue(q.queue, exchange, key);
    }

    await channel.consume(q.queue, (msg) => {
        if (msg === null) return;
        console.log(` [x] ${msg.fields.routingKey}:${msg.content.toString()}`);
    }, {
        noAck: true
    });
}

// Example: consumeFromTopic(['kern.*', '*.critical']);
```

### RPC Pattern

Use a Node.js version supporting `node:crypto.randomUUID`. Start
`startRpcServer().catch(console.error)` in one process. In another, create a
`FibonacciRpcClient`, await `client.connect()`, await `client.call(10)`, then
await `client.close()` in `finally`. Keep inputs small and nonnegative. A timeout
does not cancel server work.

```javascript
const amqp = require('amqplib');
const { randomUUID } = require('node:crypto');

// RPC Client
class FibonacciRpcClient {
    async connect() {
        this.connection = await amqp.connect('amqp://localhost');
        this.channel = await this.connection.createChannel();

        const { queue } = await this.channel.assertQueue('', {
            exclusive: true
        });

        this.callbackQueue = queue;

        await this.channel.consume(this.callbackQueue, (msg) => {
            if (msg === null) return;
            if (msg.properties.correlationId === this.correlationId) {
                this.response = msg.content.toString();
            }
        }, { noAck: true });
    }

    async call(n, timeoutMs = 10000) {
        this.correlationId = randomUUID();
        this.response = null;

        this.channel.sendToQueue('rpc_queue', Buffer.from(n.toString()), {
            correlationId: this.correlationId,
            replyTo: this.callbackQueue
        });

        // Wait for response
        return new Promise((resolve, reject) => {
            const timeout = setTimeout(() => {
                clearInterval(interval);
                reject(new Error('RPC response timed out'));
            }, timeoutMs);
            const interval = setInterval(() => {
                if (this.response !== null) {
                    clearInterval(interval);
                    clearTimeout(timeout);
                    resolve(Number.parseInt(this.response, 10));
                }
            }, 100);
        });
    }

    async close() {
        await this.connection.close();
    }
}

// RPC Server
function fib(n) {
    if (n === 0) return 0;
    if (n === 1) return 1;
    return fib(n - 1) + fib(n - 2);
}

async function startRpcServer() {
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();

    await channel.assertQueue('rpc_queue', {
        durable: false
    });

    await channel.prefetch(1);

    console.log(' [x] Awaiting RPC requests');

    await channel.consume('rpc_queue', (msg) => {
        if (msg === null) return;
        const n = parseInt(msg.content.toString());

        console.log(` [.] fib(${n})`);
        const result = fib(n);

        channel.sendToQueue(msg.properties.replyTo, Buffer.from(result.toString()), {
            correlationId: msg.properties.correlationId
        });

        channel.ack(msg);
    });
}
```

---

## Go (amqp091-go)

### Installation

Start a new Go module, then save each example as `main.go` and run `go run .`.
The fanout and topic blocks define functions. Add a `main` function that calls
the publisher or consumer you want to run, in separate processes.

```bash
go mod init example.com/strangeq-client
go get github.com/rabbitmq/amqp091-go
```

### Basic Publisher

```go
package main

import (
    "log"
    "time"
    amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
    conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()

    ch, err := conn.Channel()
    if err != nil {
        log.Fatal(err)
    }
    defer ch.Close()

    q, err := ch.QueueDeclare(
        "task_queue", // name
        true,         // durable
        false,        // delete when unused
        false,        // exclusive
        false,        // no-wait
        nil,          // arguments
    )
    if err != nil {
        log.Fatal(err)
    }

    if err := ch.Confirm(false); err != nil {
        log.Fatal(err)
    }
    confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

    body := "Hello from Go!"
    err = ch.Publish(
        "",     // exchange
        q.Name, // routing key
        false,  // mandatory
        false,  // immediate
        amqp.Publishing{
            DeliveryMode: amqp.Persistent,
            ContentType:  "text/plain",
            Body:         []byte(body),
        },
    )
    if err != nil {
        log.Fatal(err)
    }

    select {
    case confirmation, ok := <-confirms:
        if !ok || !confirmation.Ack {
            log.Fatal("publish was not confirmed")
        }
    case <-time.After(10 * time.Second):
        log.Fatal("publish confirm timed out")
    }
    log.Printf(" [x] Sent '%s'", body)
}
```

### Basic Consumer

```go
package main

import (
    "log"
    amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
    conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()

    ch, err := conn.Channel()
    if err != nil {
        log.Fatal(err)
    }
    defer ch.Close()

    q, err := ch.QueueDeclare(
        "task_queue",
        true,
        false,
        false,
        false,
        nil,
    )
    if err != nil {
        log.Fatal(err)
    }

    err = ch.Qos(
        1,     // prefetch count
        0,     // prefetch size
        false, // global
    )
    if err != nil {
        log.Fatal(err)
    }

    msgs, err := ch.Consume(
        q.Name,
        "",
        false, // auto-ack
        false,
        false,
        false,
        nil,
    )
    if err != nil {
        log.Fatal(err)
    }

    log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
    for d := range msgs {
        log.Printf(" [x] Received %s", d.Body)
        if err := d.Ack(false); err != nil {
            log.Fatal(err)
        }
    }
}
```

### Publish/Subscribe with Fanout Exchange

```go
package main

import (
    "log"
    amqp "github.com/rabbitmq/amqp091-go"
)

// Publisher
func publishLogs() {
    conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()

    ch, err := conn.Channel()
    if err != nil {
        log.Fatal(err)
    }
    defer ch.Close()

    err = ch.ExchangeDeclare(
        "logs",   // name
        "fanout", // type
        false,    // durable
        false,    // auto-deleted
        false,    // internal
        false,    // no-wait
        nil,      // arguments
    )
    if err != nil {
        log.Fatal(err)
    }

    body := "Log message from Go"
    err = ch.Publish(
        "logs", // exchange
        "",     // routing key
        false,
        false,
        amqp.Publishing{
            ContentType: "text/plain",
            Body:        []byte(body),
        },
    )
    if err != nil {
        log.Fatal(err)
    }

    log.Printf(" [x] Sent '%s'", body)
}

// Subscriber
func receiveLogs() {
    conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()

    ch, err := conn.Channel()
    if err != nil {
        log.Fatal(err)
    }
    defer ch.Close()

    err = ch.ExchangeDeclare(
        "logs",
        "fanout",
        false,
        false,
        false,
        false,
        nil,
    )
    if err != nil {
        log.Fatal(err)
    }

    q, err := ch.QueueDeclare(
        "",    // name
        false, // durable
        false, // delete when unused
        true,  // exclusive
        false, // no-wait
        nil,   // arguments
    )
    if err != nil {
        log.Fatal(err)
    }

    err = ch.QueueBind(
        q.Name, // queue name
        "",     // routing key
        "logs", // exchange
        false,
        nil,
    )
    if err != nil {
        log.Fatal(err)
    }

    msgs, err := ch.Consume(
        q.Name,
        "",
        true,
        false,
        false,
        false,
        nil,
    )
    if err != nil {
        log.Fatal(err)
    }

    log.Printf(" [*] Waiting for logs. To exit press CTRL+C")
    for d := range msgs {
        log.Printf(" [x] %s", d.Body)
    }
}
```

### Topic Exchange (Routing)

```go
package main

import (
    "log"
    amqp "github.com/rabbitmq/amqp091-go"
)

func publishToTopic(routingKey string, message string) {
    conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()

    ch, err := conn.Channel()
    if err != nil {
        log.Fatal(err)
    }
    defer ch.Close()

    err = ch.ExchangeDeclare(
        "topic_logs",
        "topic",
        false,
        false,
        false,
        false,
        nil,
    )
    if err != nil {
        log.Fatal(err)
    }

    err = ch.Publish(
        "topic_logs",
        routingKey,
        false,
        false,
        amqp.Publishing{
            ContentType: "text/plain",
            Body:        []byte(message),
        },
    )
    if err != nil {
        log.Fatal(err)
    }

    log.Printf(" [x] Sent '%s':'%s'", routingKey, message)
}

func consumeFromTopic(bindingKeys []string) {
    conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()

    ch, err := conn.Channel()
    if err != nil {
        log.Fatal(err)
    }
    defer ch.Close()

    err = ch.ExchangeDeclare(
        "topic_logs",
        "topic",
        false,
        false,
        false,
        false,
        nil,
    )
    if err != nil {
        log.Fatal(err)
    }

    q, err := ch.QueueDeclare(
        "",
        false,
        false,
        true,
        false,
        nil,
    )
    if err != nil {
        log.Fatal(err)
    }

    for _, key := range bindingKeys {
        err = ch.QueueBind(
            q.Name,
            key,
            "topic_logs",
            false,
            nil,
        )
        if err != nil {
            log.Fatal(err)
        }
    }

    msgs, err := ch.Consume(
        q.Name,
        "",
        true,
        false,
        false,
        false,
        nil,
    )
    if err != nil {
        log.Fatal(err)
    }

    log.Printf(" [*] Waiting for logs. To exit press CTRL+C")
    for d := range msgs {
        log.Printf(" [x] %s:%s", d.RoutingKey, d.Body)
    }
}
```

## Protocol limits

The standalone broker supports a subset of RabbitMQ's AMQP extensions.

- Priority queues, consumer priorities, lazy queues, SASL EXTERNAL, and multi-step
  SASL challenge-response are unsupported.
- Global prefetch is accepted but applied per consumer. Nonzero prefetch size is
  rejected with a connection-level `540 NOT_IMPLEMENTED` error.
- A full `reject-publish` queue produces a negative publisher confirm when
  confirms are enabled. Without confirms, a mandatory publish produces
  `basic.return` and a non-mandatory publish is silently dropped. Transactions
  enforce the queue limit by dropping refused copies while committing the
  remaining operations.
- Zero-TTL messages expire without a publish-time handoff to a waiting consumer.
  Malformed per-message expiration values are treated as no per-message TTL,
  although a queue TTL still applies.

See [cluster behavior and limits](docs/CLUSTERING.md#behavior-and-limits) for
replicated queues and the subset available in cluster mode.

## Next Steps

- See [QUICKSTART.md](QUICKSTART.md) for server installation and configuration
- Check [CONTRIBUTING.md](CONTRIBUTING.md) for development guidelines
