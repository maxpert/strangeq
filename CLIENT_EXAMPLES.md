# Client Examples

Complete examples for connecting to StrangeQ from various programming languages.

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
import sys

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

```python
import pika
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

    def call(self, n):
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

        while self.response is None:
            self.connection.process_data_events()

        return int(self.response)

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
        const channel = await connection.createChannel();

        const queue = 'task_queue';
        const message = 'Hello from Node.js!';

        await channel.assertQueue(queue, {
            durable: true
        });

        channel.sendToQueue(queue, Buffer.from(message), {
            persistent: true
        });

        console.log(` [x] Sent '${message}'`);

        setTimeout(() => {
            connection.close();
        }, 500);
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
        channel.prefetch(1);

        console.log(' [*] Waiting for messages. To exit press CTRL+C');

        channel.consume(queue, (msg) => {
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
    const channel = await connection.createChannel();

    const exchange = 'logs';
    const message = 'Log message from Node.js';

    await channel.assertExchange(exchange, 'fanout', {
        durable: false
    });

    channel.publish(exchange, '', Buffer.from(message));
    console.log(` [x] Sent '${message}'`);

    setTimeout(() => {
        connection.close();
    }, 500);
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

    channel.bindQueue(q.queue, exchange, '');

    channel.consume(q.queue, (msg) => {
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
    const channel = await connection.createChannel();

    const exchange = 'topic_logs';

    await channel.assertExchange(exchange, 'topic', {
        durable: false
    });

    channel.publish(exchange, routingKey, Buffer.from(message));
    console.log(` [x] Sent '${routingKey}':'${message}'`);

    setTimeout(() => {
        connection.close();
    }, 500);
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

    bindingKeys.forEach((key) => {
        channel.bindQueue(q.queue, exchange, key);
    });

    channel.consume(q.queue, (msg) => {
        console.log(` [x] ${msg.fields.routingKey}:${msg.content.toString()}`);
    }, {
        noAck: true
    });
}

// Example: consumeFromTopic(['kern.*', '*.critical']);
```

### RPC Pattern

```javascript
const amqp = require('amqplib');
const { v4: uuidv4 } = require('uuid');

// RPC Client
class FibonacciRpcClient {
    async connect() {
        this.connection = await amqp.connect('amqp://localhost');
        this.channel = await this.connection.createChannel();

        const { queue } = await this.channel.assertQueue('', {
            exclusive: true
        });

        this.callbackQueue = queue;

        this.channel.consume(this.callbackQueue, (msg) => {
            if (msg.properties.correlationId === this.correlationId) {
                this.response = msg.content.toString();
            }
        }, { noAck: true });
    }

    async call(n) {
        this.correlationId = uuidv4();
        this.response = null;

        this.channel.sendToQueue('rpc_queue', Buffer.from(n.toString()), {
            correlationId: this.correlationId,
            replyTo: this.callbackQueue
        });

        // Wait for response
        return new Promise((resolve) => {
            const interval = setInterval(() => {
                if (this.response !== null) {
                    clearInterval(interval);
                    resolve(parseInt(this.response));
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

    channel.prefetch(1);

    console.log(' [x] Awaiting RPC requests');

    channel.consume('rpc_queue', (msg) => {
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

```bash
go get github.com/rabbitmq/amqp091-go
```

### Basic Publisher

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

    forever := make(chan bool)

    go func() {
        for d := range msgs {
            log.Printf(" [x] Received %s", d.Body)
            d.Ack(false)
        }
    }()

    log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
    <-forever
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
        true,     // durable
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
        true,
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

    forever := make(chan bool)

    go func() {
        for d := range msgs {
            log.Printf(" [x] %s", d.Body)
        }
    }()

    log.Printf(" [*] Waiting for logs. To exit press CTRL+C")
    <-forever
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
        true,
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
        true,
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

    forever := make(chan bool)

    go func() {
        for d := range msgs {
            log.Printf(" [x] %s:%s", d.RoutingKey, d.Body)
        }
    }()

    log.Printf(" [*] Waiting for logs. To exit press CTRL+C")
    <-forever
}
```

## Next Steps

- See [QUICKSTART.md](QUICKSTART.md) for server installation and configuration
- Check [CONTRIBUTING.md](CONTRIBUTING.md) for development guidelines
- Review [docs/plan.md](docs/plan.md) for implementation details
