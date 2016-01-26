#!/bin/python
import json
import unittest
import datetime
import os
import socket
from queue_manager import manager
from multiprocessing import Process

"""
Test manager on an 'ad hoc' basis or automatically (pytest).
"""

# Set up root logger
logger = manager.logger

# Basic test data.
def make_message():
    dt = str(datetime.datetime.now())
    return json.dumps(dt.split())

class Application(object):
    """ Mimic Process calls, for logging purposes. """

    def __init__(self, target=manager.run):
        self.target = target
        self.process = None
        logger.debug("Target:'{}'.".format(self.target))

    def start(self):
        self.process = Process(target=self.target)

        logger.debug("Starting '{}' as a separate process.".format(self.target))
        self.process.start()

    def join(self, timeout=None):
        self.process.join(timeout=timeout)
        logger.debug("'{}' completed.".format(self.target))

    def terminate(self):
        self.process.terminate()
        logger.debug("'{}' terminated.".format(self.target))

# N.B.: these tests may reverse the default 'producer' and 'consumer' targets.
class TestRegisterPublisher(unittest.TestCase):
    #: This can be the callback applied when a message is received - i.e. "consume()" case.
    def handle_message(self, body, message):
        # Note: 'body' may have been pickled, so refer to 'payload' instead.
        logger.info('Received message: {}'.format(message.payload))
        logger.info(' properties: {}'.format(message.properties))
        logger.info(' delivery_info: {}'.format(message.delivery_info))
        message.ack()

        self.payload = message.payload

    def consume(self, cfg=manager.incoming_cfg):
        """ Get message via callback mechanism """

        with manager.setup_consumer(cfg=cfg, callback=self.handle_message) as consumer:

            logger.debug("cfg: {}".format(cfg))

            # 'consume' may be a misnomer here - it just initiates the consumption process, I believe.
            consumer.consume()

            # Execute 'drain_events()' loop in a time-out thread, in case it gets stuck.
            logger.info("'drain_events()' with timeout")
            try:
                consumer.connection.drain_events(timeout=5)
            except Exception as e:
                logger.error(e)
                raise
            finally:
                consumer.close()

    def reset(self):

            # """ Clear the decks. """

        logger.debug("reset")

        try:
            with manager.setup_connection(manager.outgoing_cfg.hostname) as outgoing_connection:

                # Need a connection to delete the queues.
                self.assertEqual(outgoing_connection.connected, True)

                outgoing_channel = outgoing_connection.channel()
                queue = manager.setup_queue(outgoing_channel, cfg=manager.outgoing_cfg)
                queue.purge()
                queue.delete()

            with manager.setup_connection(manager.incoming_cfg.hostname) as incoming_connection:

                # Need a connection to delete the queues.
                self.assertEqual(incoming_connection.connected, True)

                incoming_channel = incoming_connection.channel()
                queue = manager.setup_queue(incoming_channel, cfg=manager.incoming_cfg)
                queue.purge()
                queue.delete()

        except Exception as e:
            logger.error(e)
            raise

    def setUp(self):
        """ Establish connection and other resources; prepare """

        logger.debug("setUp")

        self.app = Application()

        # Ensure that message broker is alive, etc.
        self.reset()

        self.message = None             # Message to be sent.
        self.payload = None             # Corresponding 'payload' of message received.

        # Execute 'manager.run()' as a separate process.
        self.app.start()

        test_title = self.id().split(sep='.')[-1]
        logger.info(test_title)

    def tearDown(self):

        logger.debug("setUp")

        # N.B.: app needs to be terminated before queues can be deleted!
        self.app.join(timeout=5)
        self.app.terminate()

        if os.getenv('LOG_THRESHOLD_LEVEL') != 'DEBUG':
            self.reset()

    def test_incoming_queue(self):
        """ Basic check of 'incoming' message via default direct exchange """

        # We don't need the app to be running for this test.
        self.app.terminate()

        self.message = make_message()

        producer = manager.setup_producer(cfg=manager.incoming_cfg)
        producer.publish(body=self.message, routing_key=manager.incoming_cfg.queue, headers={'title_number': 'DN1'})
        logger.info("Put message, exchange: {}, {}".format(self.message, producer.exchange))

        producer.close()

        self.consume()

        self.assertEqual(self.message, self.payload)

    def test_broken_connection(self):
        """ Attempt 'publish' via closed connection, which is subsequently restored. """

        self.message = make_message()

        # Send a message to 'incoming' exchange - i.e. as if from source .
        with manager.setup_producer(cfg=manager.incoming_cfg) as producer:

            producer.publish(body=self.message, routing_key=manager.incoming_cfg.queue, headers={'title_number': 'DN1'})

            # Kill connection to broker.
            producer.connection.close()

        # Block (wait) until app times out or terminates.
        self.app.join(timeout=5)

        # Consume message from outgoing exchange; this will establish another connection.
        self.consume(cfg=manager.outgoing_cfg)

        self.assertEqual(self.message, self.payload)

    def test_stored_incoming_message(self):
        """ Store message in INCOMING queue, then consume later and check it. """

        self.app.terminate()

        self.message = make_message()

        # Send a message to 'incoming' exchange - i.e. as if from source .
        with manager.setup_producer(cfg=manager.incoming_cfg) as producer:
            producer.publish(body=self.message, headers={'title_number': 'DN1'})
            logger.debug(self.message)

        self.app.start()

        # Consume message from outgoing exchange.
        self.consume(cfg=manager.outgoing_cfg)

        self.assertEqual(self.message, self.payload)

    def test_stored_outgoing_message(self):
        """ Store message in OUTGOING queue, then consume later and check it. """

        self.message = make_message()

        # Send a message to 'incoming' exchange - i.e. as if from source .
        with manager.setup_producer(cfg=manager.incoming_cfg) as producer:
            producer.publish(body=self.message, routing_key=manager.incoming_cfg.queue, headers={'title_number': 'DN1'})
            logger.debug(self.message)

        # Kill application; wait long enough for message to be stored.
        # N.B.: 1 second may be insufficient, for a full coverage check during testing.
        self.app.join(timeout=5)
        self.app.terminate()

        # Consume message from outgoing exchange.
        self.consume(cfg=manager.outgoing_cfg)

        self.assertEqual(self.message, self.payload)

    def test_default_topic_keys(self):
        """ Check that message with a suitable routing_key matches the default binding_key. """

        # We don't need the app to be running for this test.
        self.app.terminate()

        self.message = make_message()

        ROOT_KEY = 'feeder'

        # Use default binding key for the queue that is created via setup_producer().
        cfg = manager.outgoing_cfg

        with manager.setup_producer(cfg=cfg) as producer:
            routing_key = ROOT_KEY + '.test_default_topic_keys'
            producer.publish(body=self.message, routing_key=routing_key, headers={'title_number': 'DN1'})
            logger.debug(self.message)

        # Consume message from outgoing exchange.
        self.consume(cfg=cfg)

        self.assertEqual(self.message, self.payload)

    def test_valid_topic_keys(self):
        """ Check that message with a suitable routing_key matches corresponding binding_key. """

        # We don't need the app to be running for this test.
        self.app.terminate()

        self.message = make_message()

        ROOT_KEY = 'feeder'

        # Set binding key for the queue that is created via setup_producer().
        cfg = manager.outgoing_cfg._replace(binding_key=ROOT_KEY+'.*')

        with manager.setup_producer(cfg=cfg) as producer:
            routing_key = ROOT_KEY + '.test_valid_topic_keys'
            producer.publish(body=self.message, routing_key=routing_key, headers={'title_number': 'DN1'})
            logger.debug(self.message)

        # Consume message from outgoing exchange.
        self.consume(cfg=cfg)

        self.assertEqual(self.message, self.payload)

    def test_invalid_topic_keys(self):
        """ Check that message with a 'bad' routing_key does not match the queue's binding_key. """

        # We don't need the app to be running for this test.
        self.app.terminate()

        self.message = make_message()

        ROOT_KEY = 'feeder'

        # Set binding key for the queue that is created via setup_producer().
        cfg = manager.outgoing_cfg._replace(binding_key=ROOT_KEY+'.*')

        with manager.setup_producer(cfg=cfg) as producer:
            routing_key = 'FEEDER' + '.test_invalid_topic_keys'
            producer.publish(body=self.message, routing_key=routing_key, headers={'title_number': 'DN1'})
            logger.debug(self.message)

        # Attempt to consume message from outgoing exchange; should time out.
        try:
            self.consume(cfg=cfg)
        except socket.timeout:
            pass
        except Exception as e:
            raise

    def test_end_to_end(self, count=1):
        """ Send message from dummy "System Of Record", then consume and check it. """

        # Send a message to 'incoming' exchange - i.e. as if from source .
        # import pdb; pdb.set_trace()
        with manager.setup_producer(cfg=manager.incoming_cfg) as producer:
            for n in range(count):

                # Message to be sent.
                self.message = make_message()

                producer.publish(body=self.message, routing_key=manager.incoming_cfg.queue, headers={'title_number': 'DN1'})
                logger.debug(self.message)

                # Wait long enough message to be processed.
                self.app.join(timeout=1)

                # Consume message from outgoing exchange, via callback.
                self.consume(cfg=manager.outgoing_cfg)

                self.assertEqual(self.message, self.payload)


    def test_multiple_end_to_end(self):
        """ Check many messages. """

        self.test_end_to_end(10)

if __name__ == '__main__':
    unittest.main()
