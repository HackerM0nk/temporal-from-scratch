// ─────────────────────────────────────────────────────────────────────────────
// RabbitMQ Task Queue
//
// RabbitMQ replaces the Redis LIST that we previously used for the ready queue.
// The critical difference is MESSAGE ACKNOWLEDGEMENT.
//
//   Redis BLPOP:   message removed from queue the moment it's dequeued.
//                  Worker crash before finishing = message is gone.
//                  Recovery relies entirely on the reconciler detecting a
//                  stuck workflow_lock after 60 seconds.
//
//   RabbitMQ:      message stays "unacknowledged" in the broker until the
//                  consumer calls channel.ack(msg). If the consumer's TCP
//                  connection drops (process crash, OOM kill, network blip),
//                  the broker redelivers the message to the next available
//                  consumer automatically — no reconciler involvement needed
//                  for this failure mode.
//
// The delayed/retry queue (previously Redis ZSET diy:tasks:delayed) is handled
// entirely in PostgreSQL via the scheduled_tasks table. The scheduler polls it
// every 2 seconds and publishes due tasks here.
//
// COMPARISON to Temporal:
//   RabbitMQ queue    ←→  Temporal's task queue (worker polls)
//   channel.ack()     ←→  RespondWorkflowTaskCompleted / RespondActivityTaskCompleted
//   unacked messages  ←→  Temporal's in-progress task tracking
//   consumer timeout  ←→  Temporal's scheduleToStartTimeout
// ─────────────────────────────────────────────────────────────────────────────

import amqp, { Channel, ChannelModel, ConsumeMessage } from 'amqplib';
import type { QueueTask } from '../types';

export const TASK_QUEUE = 'diy.tasks';

const RABBITMQ_URL = process.env.RABBITMQ_URL ?? 'amqp://guest:guest@localhost:5672';

let _connection: ChannelModel | null = null;
let _channel: Channel | null = null;

// ── Connection ────────────────────────────────────────────────────────────────

export async function getChannel(): Promise<Channel> {
  if (_channel) return _channel;

  if (!_connection) {
    _connection = await amqp.connect(RABBITMQ_URL);

    _connection.on('error', (err) => {
      console.error('[rabbitmq] Connection error:', err.message);
      _connection = null;
      _channel = null;
    });
    _connection.on('close', () => {
      console.log('[rabbitmq] Connection closed');
      _connection = null;
      _channel = null;
    });
  }

  _channel = await _connection.createChannel();

  // Declare the queue. durable=true means it survives a RabbitMQ restart.
  await _channel.assertQueue(TASK_QUEUE, {
    durable: true,
    arguments: {
      // Messages that exceed deliveryLimit (set via consumer) go to a
      // dead-letter queue. We manage dead-lettering in application code
      // via dead_letter_tasks, but this is where you'd configure broker-level DLQ.
      'x-queue-type': 'classic',
    },
  });

  return _channel;
}

export async function waitForRabbitMQ(maxAttempts = 30): Promise<void> {
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      await getChannel();
      console.log('[rabbitmq] Connected');
      return;
    } catch (err: any) {
      console.log(`[rabbitmq] Waiting for RabbitMQ... (attempt ${attempt}/${maxAttempts})`);
      _connection = null;
      _channel = null;
      await new Promise((r) => setTimeout(r, 2000));
    }
  }
  throw new Error(`Could not connect to RabbitMQ at ${RABBITMQ_URL}`);
}

// ── Publishing ────────────────────────────────────────────────────────────────

export async function publishTask(task: QueueTask): Promise<void> {
  const channel = await getChannel();
  const payload = Buffer.from(JSON.stringify(task));

  // persistent=true: message survives a RabbitMQ broker restart
  channel.sendToQueue(TASK_QUEUE, payload, { persistent: true });

  console.log(`[rabbitmq] → Published | type=${task.taskType} | workflowId=${task.workflowId} | attempt=${task.attempt}`);
}

// ── Consuming ─────────────────────────────────────────────────────────────────

export type TaskHandler = (
  task: QueueTask,
  ack: () => void,
  nack: (requeue: boolean) => void,
) => Promise<void>;

export async function startConsumer(handler: TaskHandler): Promise<void> {
  const channel = await getChannel();

  // prefetch(1): RabbitMQ will not deliver a second message to this consumer
  // until the first is acked. This gives us serial, one-at-a-time processing
  // per worker process — equivalent to our old single-item BLPOP loop.
  await channel.prefetch(1);

  await channel.consume(
    TASK_QUEUE,
    async (msg: ConsumeMessage | null) => {
      if (!msg) return; // consumer cancelled by broker

      let task: QueueTask;
      try {
        task = JSON.parse(msg.content.toString()) as QueueTask;
      } catch {
        console.error('[rabbitmq] Malformed message — discarding');
        channel.ack(msg); // discard, don't requeue garbage
        return;
      }

      const ack = () => channel.ack(msg);
      const nack = (requeue: boolean) => channel.nack(msg, false, requeue);

      await handler(task, ack, nack);
    },
    { noAck: false }, // manual acknowledgement
  );

  console.log(`[rabbitmq] Consumer started on queue "${TASK_QUEUE}"`);
}

// ── Queue stats ───────────────────────────────────────────────────────────────

export async function queueDepth(): Promise<{ ready: number; unacked: number }> {
  const channel = await getChannel();
  const info = await channel.checkQueue(TASK_QUEUE);
  return { ready: info.messageCount, unacked: info.consumerCount };
}

export async function closeRabbitMQ(): Promise<void> {
  try {
    await _channel?.close();
    await _connection?.close();
  } catch {
    // ignore on shutdown
  }
  _channel = null;
  _connection = null;
}
