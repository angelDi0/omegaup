#!/usr/bin/env python3

'''test reprocess_dlq module: dead-letter queue reinjection.'''

from typing import Any, List

import pytest_mock

import rabbitmq_client
import rabbitmq_connection
import reprocess_dlq


def _queue_state(mocker: pytest_mock.MockerFixture, message_count: int) -> Any:
    state = mocker.MagicMock()
    state.method.message_count = message_count
    return state


def test_reprocess_empty_queue(mocker: pytest_mock.MockerFixture) -> None:
    '''With an empty dead-letter queue nothing is republished.'''
    channel = mocker.MagicMock()
    channel.queue_declare.return_value = _queue_state(mocker, 0)

    reinjected = reprocess_dlq.reprocess_dead_letters(channel)

    assert reinjected == 0
    channel.basic_get.assert_not_called()
    channel.basic_publish.assert_not_called()


def test_reprocess_reinjects_batch(mocker: pytest_mock.MockerFixture) -> None:
    '''Each dead-lettered message is republished and acked.'''
    channel = mocker.MagicMock()
    channel.queue_declare.return_value = _queue_state(mocker, 2)

    def make_get(tag: int) -> Any:
        method = mocker.MagicMock()
        method.delivery_tag = tag
        return (method, mocker.MagicMock(), b'{"contest_id": 1}')

    channel.basic_get.side_effect = [make_get(10), make_get(11)]

    reinjected = reprocess_dlq.reprocess_dead_letters(channel)

    assert reinjected == 2
    assert channel.basic_publish.call_count == 2
    assert channel.basic_ack.call_count == 2


def test_reprocess_resets_retry_count(
        mocker: pytest_mock.MockerFixture
) -> None:
    '''Republished messages go to the main queue with the retry count reset.'''
    channel = mocker.MagicMock()
    channel.queue_declare.return_value = _queue_state(mocker, 1)
    method = mocker.MagicMock()
    method.delivery_tag = 5
    channel.basic_get.side_effect = [
        (method, mocker.MagicMock(), b'{"contest_id": 1}')
    ]

    reprocess_dlq.reprocess_dead_letters(channel)

    published = channel.basic_publish.call_args.kwargs
    assert published['exchange'] == rabbitmq_connection.CERTIFICATES_EXCHANGE
    assert published['routing_key'] == (
        rabbitmq_connection.CONTEST_ROUTING_KEY
    )
    assert published['properties'].headers[
        rabbitmq_client.RETRY_COUNT_HEADER] == 0


def test_reprocess_stops_on_empty_get(
        mocker: pytest_mock.MockerFixture
) -> None:
    '''If a get returns no message the loop stops without failing.'''
    channel = mocker.MagicMock()
    channel.queue_declare.return_value = _queue_state(mocker, 3)
    method = mocker.MagicMock()
    method.delivery_tag = 1
    gets: List[Any] = [
        (method, mocker.MagicMock(), b'{}'),
        (None, None, None),
    ]
    channel.basic_get.side_effect = gets

    reinjected = reprocess_dlq.reprocess_dead_letters(channel)

    assert reinjected == 1
