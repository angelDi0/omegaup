<?php

namespace OmegaUp;

class RabbitMQConnection {
    /**
     * The singleton instance of this class.
     * @var null|RabbitMQConnection
     */
    private static $_instance = null;

    /**
     * @psalm-readonly
     */
    public \PhpAmqpLib\Connection\AMQPStreamConnection $connection;

    /**
     * Returns the singleton instance of this class. It also registers a
     * shutdown function to close the connection upon script
     * termination.
     */
    public static function getInstance(): RabbitMQConnection {
        if (is_null(self::$_instance)) {
            self::$_instance = new RabbitMQConnection();
            register_shutdown_function(function () {
                if (is_null(self::$_instance)) {
                    return;
                }
                self::$_instance->connection->close();
                self::$_instance = null;
            });
        }
        return self::$_instance;
    }

    private function __construct() {
        $this->connection = new \PhpAmqpLib\Connection\AMQPStreamConnection(
            OMEGAUP_RABBITMQ_HOST,
            OMEGAUP_RABBITMQ_PORT,
            OMEGAUP_RABBITMQ_USERNAME,
            OMEGAUP_RABBITMQ_PASSWORD,
        );
    }

    public function channel(): \PhpAmqpLib\Channel\AMQPChannel {
        return $this->connection->channel();
    }

    /**
     * Publishes a persistent message and blocks until the broker confirms it.
     */
    public static function publishWithConfirmation(
        \PhpAmqpLib\Channel\AMQPChannel $channel,
        string $exchange,
        string $routingKey,
        string $messageBody,
        float $timeout = 10.0
    ): void {
        $confirmed = true;

        $channel->confirm_select();
        $channel->set_nack_handler(
            function (\PhpAmqpLib\Message\AMQPMessage $_message) use (
                &$confirmed
            ): void {
                $confirmed = false;
            }
        );
        $channel->set_return_listener(
            function (
                int $_replyCode,
                string $_replyText,
                string $_exchange,
                string $_routingKey,
                \PhpAmqpLib\Message\AMQPMessage $_message
            ) use (&$confirmed): void {
                $confirmed = false;
            }
        );

        $message = new \PhpAmqpLib\Message\AMQPMessage(
            $messageBody,
            ['delivery_mode' => \PhpAmqpLib\Message\AMQPMessage::DELIVERY_MODE_PERSISTENT]
        );

        try {
            $channel->basic_publish(
                $message,
                $exchange,
                $routingKey,
                mandatory: true
            );
            $channel->wait_for_pending_acks_returns($timeout);
        } catch (\PhpAmqpLib\Exception\AMQPExceptionInterface $e) {
            throw new \OmegaUp\Exceptions\ServiceUnavailableException(
                'serviceUnavailableError',
                $e
            );
        }

        if (!$confirmed) {
            throw new \OmegaUp\Exceptions\ServiceUnavailableException(
                'serviceUnavailableError'
            );
        }
    }
}
