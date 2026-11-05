#!/usr/bin/python3

'''
Manually reprocess the contest certificate dead-letter queue.

Consumes the batch of messages currently sitting in `contest-dlq`, republishes
each one to the main contest queue with the retry count reset, and acknowledges
it in the dead-letter queue. Only the messages present when the script starts
are processed; messages that arrive later are left untouched.
'''

import argparse
import logging
import os
import sys

from typing import cast

import pika

import rabbitmq_client
import rabbitmq_connection

sys.path.insert(
    0,
    os.path.join(
        os.path.dirname(os.path.dirname(os.path.realpath(__file__))), '.'))
import lib.logs  # pylint: disable=wrong-import-position


def reprocess_dead_letters(
        channel: pika.adapters.blocking_connection.BlockingChannel,
) -> int:
    '''Reinjects the current batch of dead-lettered messages.

    Returns the number of messages republished to the main contest queue.
    '''
    rabbitmq_connection.initialize_dlq_topology(channel)

    queue_state = channel.queue_declare(
        queue=rabbitmq_connection.DEAD_LETTER_QUEUE,
        durable=True,
        exclusive=False,
        auto_delete=False,
        passive=True)
    pending = queue_state.method.message_count or 0
    logging.info('Found %d message(s) in %s',
                 pending, rabbitmq_connection.DEAD_LETTER_QUEUE)

    reinjected = 0
    for _ in range(pending):
        method, _properties, body = channel.basic_get(
            queue=rabbitmq_connection.DEAD_LETTER_QUEUE,
            auto_ack=False)
        if method is None or body is None:
            break
        channel.basic_publish(
            exchange=rabbitmq_connection.CERTIFICATES_EXCHANGE,
            routing_key=rabbitmq_connection.CONTEST_ROUTING_KEY,
            body=cast(bytes, body),
            properties=pika.BasicProperties(
                delivery_mode=2,
                headers={rabbitmq_client.RETRY_COUNT_HEADER: 0},
            ))
        channel.basic_ack(delivery_tag=cast(int, method.delivery_tag))
        reinjected += 1

    logging.info('Reinjected %d message(s) into %s',
                 reinjected, rabbitmq_connection.CONTEST_QUEUE)
    return reinjected


def main() -> None:
    '''Main entrypoint for the dead-letter queue reprocessor.'''
    parser = argparse.ArgumentParser(description=__doc__)
    lib.logs.configure_parser(parser)
    rabbitmq_connection.configure_parser(parser)
    args = parser.parse_args()
    lib.logs.init(parser.prog, args)

    logging.info('Started')
    with rabbitmq_connection.connect(
            username=args.rabbitmq_username,
            password=args.rabbitmq_password,
            host=args.rabbitmq_host,
    ) as channel:
        reprocess_dead_letters(channel)
    logging.info('Done')


if __name__ == '__main__':
    main()

# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
