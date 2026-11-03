#!/usr/bin/env python3

'''Send messages to queues in rabbitmq'''

import contextlib

from typing import Iterator

import argparse
import logging
import pika


CERTIFICATES_EXCHANGE = 'certificates'
CONTEST_QUEUE = 'contest'
CONTEST_ROUTING_KEY = 'ContestQueue'

DEAD_LETTER_EXCHANGE = 'certificates-dlx'
DEAD_LETTER_QUEUE = 'contest-dlq'
DEAD_LETTER_ROUTING_KEY = 'ContestDeadLetter'

RETRY_QUEUE = 'contest-retry'
RETRY_ROUTING_KEY = 'ContestRetry'
RETRY_TTL_MS = 30000


@contextlib.contextmanager
def connect(
        *, username: str, password: str, host: str, for_testing: bool = False
) -> Iterator[pika.adapters.blocking_connection.BlockingChannel]:
    '''Connects to rabbitmq with the arguments provided.'''
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=host,
        port=5672,
        virtual_host='/',
        credentials=pika.PlainCredentials(username, password),
        heartbeat=600,
        # mypy does not support structural typing yet
        # https://github.com/python/mypy/issues/3186
        blocked_connection_timeout=300.0,  # type: ignore
    ))
    channel = connection.channel()

    channel.exchange_declare(exchange='certificates',
                             exchange_type='direct',
                             durable=True)
    try:
        yield channel
    finally:
        if not for_testing:
            connection.close()
        else:
            logging.info('Avoiding close the connection for testing purposes')


def configure_parser(parser: argparse.ArgumentParser) -> None:
    '''Add rabbitmq-related arguments to `parser`'''
    parser.add_argument('--rabbitmq-username', type=str,
                        help='rabbitmq username',
                        default='omegaup')
    parser.add_argument('--rabbitmq-password', type=str,
                        help='rabbitmq password',
                        default='omegaup')
    parser.add_argument('--rabbitmq-host', type=str,
                        help='rabbitmq host',
                        default='rabbitmq')


def initialize_rabbitmq(
        *,
        queue: str,
        exchange: str,
        routing_key: str,
        channel: pika.adapters.blocking_connection.BlockingChannel
) -> None:
    '''initializes the queue and exchange'''
    channel.queue_declare(
        queue=queue, passive=False,
        durable=True, exclusive=False,
        auto_delete=False)
    channel.exchange_declare(
        exchange=exchange,
        auto_delete=False,
        durable=True,
        exchange_type='direct')
    channel.queue_bind(exchange=exchange,
                       queue=queue,
                       routing_key=routing_key)


def initialize_dlq_topology(
        channel: pika.adapters.blocking_connection.BlockingChannel,
        retry_ttl_ms: int = RETRY_TTL_MS,
) -> None:
    '''Declares the dead-letter and retry queues for the contest pipeline.'''
    channel.exchange_declare(
        exchange=DEAD_LETTER_EXCHANGE,
        exchange_type='direct',
        durable=True,
        auto_delete=False)
    channel.queue_declare(
        queue=DEAD_LETTER_QUEUE,
        durable=True,
        exclusive=False,
        auto_delete=False)
    channel.queue_bind(
        exchange=DEAD_LETTER_EXCHANGE,
        queue=DEAD_LETTER_QUEUE,
        routing_key=DEAD_LETTER_ROUTING_KEY)

    channel.queue_declare(
        queue=RETRY_QUEUE,
        durable=True,
        exclusive=False,
        auto_delete=False,
        arguments={  # type: ignore[arg-type]
            'x-message-ttl': retry_ttl_ms,
            'x-dead-letter-exchange': CERTIFICATES_EXCHANGE,
            'x-dead-letter-routing-key': CONTEST_ROUTING_KEY,
        })
    channel.queue_bind(
        exchange=CERTIFICATES_EXCHANGE,
        queue=RETRY_QUEUE,
        routing_key=RETRY_ROUTING_KEY)
