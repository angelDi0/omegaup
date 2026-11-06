<?php

/**
 * Tests the publisher-confirmation helper of RabbitMQConnection.
 */
class RabbitMQConnectionTest extends \OmegaUp\Test\ControllerTestCase {
    public function testPublishWithConfirmationSucceeds() {
        $channel = $this->createMock(\PhpAmqpLib\Channel\AMQPChannel::class);
        $channel->expects($this->once())->method('basic_publish');
        $channel->expects($this->once())->method(
            'wait_for_pending_acks_returns'
        );

        \OmegaUp\RabbitMQConnection::publishWithConfirmation(
            $channel,
            'certificates',
            'ContestQueue',
            '{"contest_id": 1}'
        );
    }

    public function testPublishWithConfirmationNackThrows() {
        $channel = $this->createMock(\PhpAmqpLib\Channel\AMQPChannel::class);
        $channel->method('set_nack_handler')->willReturnCallback(
            function (callable $handler): void {
                $handler(
                    new \PhpAmqpLib\Message\AMQPMessage('{"contest_id": 1}')
                );
            }
        );

        try {
            \OmegaUp\RabbitMQConnection::publishWithConfirmation(
                $channel,
                'certificates',
                'ContestQueue',
                '{"contest_id": 1}'
            );
            $this->fail('Should have thrown a ServiceUnavailableException');
        } catch (\OmegaUp\Exceptions\ServiceUnavailableException $e) {
            $this->assertSame('serviceUnavailableError', $e->getMessage());
        }
    }

    public function testPublishWithConfirmationTimeoutThrows() {
        $channel = $this->createMock(\PhpAmqpLib\Channel\AMQPChannel::class);
        $channel->method('wait_for_pending_acks_returns')->willThrowException(
            new \PhpAmqpLib\Exception\AMQPTimeoutException('timed out')
        );

        try {
            \OmegaUp\RabbitMQConnection::publishWithConfirmation(
                $channel,
                'certificates',
                'ContestQueue',
                '{"contest_id": 1}'
            );
            $this->fail('Should have thrown a ServiceUnavailableException');
        } catch (\OmegaUp\Exceptions\ServiceUnavailableException $e) {
            $this->assertSame('serviceUnavailableError', $e->getMessage());
        }
    }
}
