<?php
/**
 * @link https://github.com/webtoucher/yii2-amqp
 * @copyright Copyright (c) 2014 webtoucher
 * @license https://github.com/webtoucher/yii2-amqp/blob/master/LICENSE.md
 */

namespace rooooodik\amqp\components;

use PhpAmqpLib\Exception\AMQPTimeoutException;
use yii\base\Component;
use yii\base\Exception;
use yii\helpers\Inflector;
use yii\helpers\Json;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPConnection;
use PhpAmqpLib\Message\AMQPMessage;


/**
 * AMQP wrapper.
 *
 * @property AMQPConnection $connection AMQP connection.
 * @property AMQPChannel $channel AMQP channel.
 * @param bool $noAck
 * @author Alexey Kuznetsov <mirakuru@webtoucher.ru>
 * @since 2.0
 */
class Amqp extends Component
{
    const TYPE_TOPIC = 'topic';
    const TYPE_DIRECT = 'direct';
    const TYPE_HEADERS = 'headers';
    const TYPE_FANOUT = 'fanout';

    /**
     * @var AMQPConnection
     */
    protected static $ampqConnection;

    /**
     * @var AMQPChannel[]
     */
    protected $channels = [];

    /**
     * @var string
     */
    public $host = '127.0.0.1';

    /**
     * @var integer
     */
    public $port = 5672;

    /**
     * @var string
     */
    public $user;

    /**
     * @var string
     */
    public $password;

    /**
     * @var string
     */
    public $vhost = '/';

    /**
     * @inheritdoc
     */
    public function init()
    {
        parent::init();
        if (empty($this->user)) {
            throw new Exception("Parameter 'user' was not set for AMQP connection.");
        }
        if (empty(self::$ampqConnection)) {
            self::$ampqConnection = new AMQPConnection(
                $this->host,
                $this->port,
                $this->user,
                $this->password,
                $this->vhost
            );
        }
    }

    /**
     * Returns AMQP connection.
     *
     * @return AMQPConnection
     */
    public function getConnection()
    {
        return self::$ampqConnection;
    }

    /**
     * Returns AMQP connection.
     *
     * @param string $channel_id
     * @return AMQPChannel
     */
    public function getChannel($channel_id = null)
    {
        $index = $channel_id ?: 'default';
        if (!array_key_exists($index, $this->channels)) {
            $this->channels[$index] = $this->connection->channel($channel_id);
        }
        return $this->channels[$index];
    }

    /**
     * Sends message to the exchange.
     *
     * @param string $exchange
     * @param string $routing_key
     * @param string|array $message
     * @param string $type Use self::TYPE_DIRECT if it is an answer
     * @return void
     */
    public function send($exchange, $routing_key, $message, $type = self::TYPE_TOPIC, $properties = null)
    {
        $message = $this->prepareMessage($message, $properties);
        if ($type == self::TYPE_TOPIC) {
            $this->channel->exchange_declare($exchange, $type, false, true, false);
        }
        try {
            $this->channel->basic_publish($message, $exchange, $routing_key);
        } catch (AMQPTimeoutException $e) {
            $this->connection->reconnect();
            $this->channel->basic_publish($message, $exchange, $routing_key);
        }
    }

    /**
     * Sends message to the exchange and waits for answer.
     *
     * @param string $exchange
     * @param string $routing_key
     * @param string|array $message
     * @param integer $timeout Timeout in seconds.
     * @return string
     */
    public function ask($exchange, $routing_key, $message, $timeout)
    {
        list ($queueName) = $this->channel->queue_declare($routing_key, false, true, false, false);
        $message = $this->prepareMessage($message, [
            'reply_to' => $queueName,
        ]);
        // queue name must be used for answer's routing key
        $this->channel->queue_bind($queueName, $exchange, $queueName);

        $response = null;
        $callback = function(AMQPMessage $answer) use ($message, &$response) {
            $response = $answer->body;
        };

        $this->channel->basic_consume($queueName, '', false, false, false, false, $callback);
        $this->channel->basic_publish($message, $exchange, $routing_key);
        while (!$response) {
            // exception will be thrown on timeout
            $this->channel->wait(null, false, $timeout);
        }
        return $response;
    }

    /**
     * @param $exchange
     * @param $routing_key
     * @param $callback
     * @param string $type
     * @param bool $noAck
     * @param int $timeout
     */
    public function listen($exchange, $routing_key, $callback, $type = self::TYPE_TOPIC, $noAck = false, $timeout = 0)
    {
        list ($queueName) = $this->channel->queue_declare($routing_key, false, true, false, false);
        if ($type == Amqp::TYPE_DIRECT) {
            $this->channel->exchange_declare($exchange, $type, false, true, false);
        }
        $this->channel->queue_bind($queueName, $exchange, $routing_key);
        $this->channel->basic_qos(null, 1, null);
        $this->channel->basic_consume($queueName, '', false, $noAck, false, false, $callback);

        while (count($this->channel->callbacks)) {
            $this->channel->wait(null, false, $timeout);
        }

        $this->channel->close();
        $this->connection->close();
    }

    /**
     * Returns prepaired AMQP message.
     *
     * @param string|array|object $message
     * @param array $properties
     * @return AMQPMessage
     * @throws Exception If message is empty.
     */
    public function prepareMessage($message, $properties = null)
    {
        if (empty($message)) {
            throw new Exception('AMQP message can not be empty');
        }
        if (is_array($message) || is_object($message)) {
            $message = Json::encode($message);
        }
        return new AMQPMessage(
            $message,
            !is_null($properties) ? $properties : [
                'delivery_mode' => 2,
            ]
        );
    }

}
