#!/usr/bin/env python3

'''Implementation of rabbitmq client.'''

import logging
from typing import Callable, cast
import pika

import rabbitmq_connection


MAX_RETRIES = 5
RETRY_COUNT_HEADER = 'x-retry-count'


ClientCallback = Callable[
    [
        pika.adapters.blocking_connection.BlockingChannel,
        pika.spec.Basic.Deliver,
        pika.spec.BasicProperties,
        bytes,
    ], None]


class UnrecoverableMessageError(Exception):
    '''Raised when a message can never be processed; must not be requeued.'''


class TransientMessageError(Exception):
    '''Raised when a message fails temporarily and may be retried.'''


def _get_retry_count(properties: pika.spec.BasicProperties) -> int:
    '''Returns the current retry count stored in the message headers.'''
    headers = properties.headers or {}
    return int(headers.get(RETRY_COUNT_HEADER, 0))


def _send_to_retry_queue(
        channel: pika.adapters.blocking_connection.BlockingChannel,
        body: bytes,
        retry_count: int) -> None:
    '''Publishes the message to the retry queue with an incremented count.'''
    channel.basic_publish(
        exchange=rabbitmq_connection.CERTIFICATES_EXCHANGE,
        routing_key=rabbitmq_connection.RETRY_ROUTING_KEY,
        body=body,
        properties=pika.BasicProperties(
            delivery_mode=2,
            headers={RETRY_COUNT_HEADER: retry_count},
        ))


def _process_and_ack(
        *,
        channel: pika.adapters.blocking_connection.BlockingChannel,
        method: pika.spec.Basic.Deliver,
        properties: pika.spec.BasicProperties,
        body: bytes,
        callback: ClientCallback) -> None:
    delivery_tag = cast(int, method.delivery_tag)
    try:
        callback(channel, method, properties, body)
    except UnrecoverableMessageError:
        logging.exception(
            'Message is unrecoverable, routing to the dead-letter queue'
        )
        if channel.is_open:
            channel.basic_nack(delivery_tag=delivery_tag, requeue=False)
        return
    except:  # noqa: bare-except pylint: disable=bare-except
        retry_count = _get_retry_count(properties)
        if not channel.is_open:
            return
        if retry_count >= MAX_RETRIES:
            logging.exception(
                'Max retries reached, routing to the dead-letter queue'
            )
            channel.basic_nack(delivery_tag=delivery_tag, requeue=False)
            return
        logging.exception(
            'Transient failure, scheduling retry %d of %d',
            retry_count + 1, MAX_RETRIES
        )
        _send_to_retry_queue(channel, body, retry_count + 1)
        channel.basic_ack(delivery_tag=delivery_tag)
        return
    if channel.is_open:
        channel.basic_ack(delivery_tag=delivery_tag)


def receive_messages(
        *, queue: str, exchange: str, routing_key: str,
        channel: pika.adapters.blocking_connection.BlockingChannel,
        callback: ClientCallback,
        stop_after_message: bool = False,
        dead_letter_exchange: str = '') -> None:
    '''Receive messages from a queue, acking after a successful callback.'''

    channel.exchange_declare(exchange=exchange,
                             durable=True,
                             exchange_type='direct')
    arguments = {}
    if dead_letter_exchange:
        arguments['x-dead-letter-exchange'] = dead_letter_exchange
        arguments['x-dead-letter-routing-key'] = (
            rabbitmq_connection.DEAD_LETTER_ROUTING_KEY
        )
    channel.queue_declare(
        queue=queue,
        durable=True,
        exclusive=False,
        arguments=arguments or None)  # type: ignore[arg-type]
    channel.queue_bind(exchange=exchange,
                       queue=queue,
                       routing_key=routing_key)
    if stop_after_message:
        method, properties, body = channel.basic_get(
            queue=queue,
            auto_ack=False)

        if method is not None and properties is not None and body is not None:
            _process_and_ack(
                channel=channel,
                method=cast(pika.spec.Basic.Deliver, method),
                properties=properties,
                body=cast(bytes, body),
                callback=callback)
        return

    def _on_message_callback(
            channel: pika.adapters.blocking_connection.BlockingChannel,
            method: pika.spec.Basic.Deliver,
            properties: pika.spec.BasicProperties,
            body: bytes) -> None:
        _process_and_ack(
            channel=channel,
            method=method,
            properties=properties,
            body=body,
            callback=callback)

    channel.basic_consume(queue=queue,
                          on_message_callback=_on_message_callback,
                          auto_ack=False)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
