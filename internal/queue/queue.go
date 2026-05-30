package queue

import (
	"context"
	"encoding/json"
	"time"

	"notification_without_cron/internal/job"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Client struct {
	conn      *amqp.Connection
	ch        *amqp.Channel
	queueName string
	dlqName   string
}

func Connect(url string, queueName string, dlqName string) (*Client, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	if _, err := ch.QueueDeclare(dlqName, true, false, false, false, nil); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, err
	}
	if _, err := ch.QueueDeclare(queueName, true, false, false, false, nil); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, err
	}
	return &Client{conn: conn, ch: ch, queueName: queueName, dlqName: dlqName}, nil
}

func (c *Client) Close() {
	_ = c.ch.Close()
	_ = c.conn.Close()
}

func (c *Client) PublishJob(ctx context.Context, saved job.Job) error {
	msg := job.QueueMessage{
		JobID:          saved.ID,
		IdempotencyKey: saved.IdempotencyKey,
		CallbackURL:    saved.CallbackURL,
		Payload:        saved.Payload,
	}
	return c.publish(ctx, c.queueName, msg)
}

func (c *Client) PublishDLQ(ctx context.Context, msg job.QueueMessage) error {
	return c.publish(ctx, c.dlqName, msg)
}

func (c *Client) Consume() (<-chan amqp.Delivery, error) {
	if err := c.ch.Qos(10, 0, false); err != nil {
		return nil, err
	}
	return c.ch.Consume(c.queueName, "", false, false, false, false, nil)
}

func DecodeMessage(delivery amqp.Delivery) (job.QueueMessage, error) {
	var msg job.QueueMessage
	err := json.Unmarshal(delivery.Body, &msg)
	return msg, err
}

func (c *Client) publish(ctx context.Context, queueName string, value any) error {
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return c.ch.PublishWithContext(ctx, "", queueName, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
		Timestamp:    time.Now(),
	})
}
