#!/usr/bin/env python3

'''test rabbitmq_client module: retry and dead-letter handling.'''

from typing import Any

import pika
import pytest_mock

import rabbitmq_client
import rabbitmq_connection


def _method(mocker: pytest_mock.MockerFixture) -> Any:
    method = mocker.MagicMock()
    method.delivery_tag = 1
    return method


def _properties(retry_count: int = 0) -> pika.spec.BasicProperties:
    return pika.BasicProperties(
        headers={rabbitmq_client.RETRY_COUNT_HEADER: retry_count}
    )


def test_success_acks(mocker: pytest_mock.MockerFixture) -> None:
    '''A successful callback acknowledges the message.'''
    channel = mocker.MagicMock()
    channel.is_open = True
    rabbitmq_client._process_and_ack(  # pylint: disable=protected-access
        channel=channel,
        method=_method(mocker),
        properties=_properties(),
        body=b'{}',
        callback=mocker.MagicMock())
    channel.basic_ack.assert_called_once()
    channel.basic_nack.assert_not_called()


def test_unrecoverable_goes_to_dlq(
        mocker: pytest_mock.MockerFixture
) -> None:
    '''An unrecoverable message is nacked without requeue (to the DLQ).'''
    channel = mocker.MagicMock()
    channel.is_open = True

    def callback(*_args: object) -> None:
        raise rabbitmq_client.UnrecoverableMessageError('bad')

    rabbitmq_client._process_and_ack(  # pylint: disable=protected-access
        channel=channel,
        method=_method(mocker),
        properties=_properties(),
        body=b'{}',
        callback=callback)
    channel.basic_nack.assert_called_once_with(delivery_tag=1, requeue=False)


def test_transient_under_limit_retries(
        mocker: pytest_mock.MockerFixture
) -> None:
    '''A transient failure under the limit is published to the retry queue.'''
    channel = mocker.MagicMock()
    channel.is_open = True

    def callback(*_args: object) -> None:
        raise rabbitmq_client.TransientMessageError('temporary')

    rabbitmq_client._process_and_ack(  # pylint: disable=protected-access
        channel=channel,
        method=_method(mocker),
        properties=_properties(retry_count=0),
        body=b'{}',
        callback=callback)
    channel.basic_publish.assert_called_once()
    published = channel.basic_publish.call_args.kwargs
    assert published['routing_key'] == (
        rabbitmq_connection.RETRY_ROUTING_KEY
    )
    assert published['properties'].headers[
        rabbitmq_client.RETRY_COUNT_HEADER] == 1
    channel.basic_ack.assert_called_once()


def test_transient_at_limit_goes_to_dlq(
        mocker: pytest_mock.MockerFixture
) -> None:
    '''A transient failure at the retry limit is routed to the DLQ.'''
    channel = mocker.MagicMock()
    channel.is_open = True

    def callback(*_args: object) -> None:
        raise rabbitmq_client.TransientMessageError('temporary')

    rabbitmq_client._process_and_ack(  # pylint: disable=protected-access
        channel=channel,
        method=_method(mocker),
        properties=_properties(retry_count=rabbitmq_client.MAX_RETRIES),
        body=b'{}',
        callback=callback)
    channel.basic_nack.assert_called_once_with(delivery_tag=1, requeue=False)
    channel.basic_publish.assert_not_called()
