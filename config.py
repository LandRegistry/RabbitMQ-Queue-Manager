#!/bin/python

import os
import kombu
from collections import namedtuple


APP_NAME = os.getenv('APP_NAME', 'AMQP MANAGER')


class Config(object):
    """ Get parameters from environment, or defaults if not specified. """

    # Flask DEBUG setting - not used by default.
    DEBUG = os.getenv('AMQP_FLASK_DEBUG', False)

    # Logging.
    LOG_THRESHOLD_LEVEL = os.getenv('LOG_THRESHOLD_LEVEL', 'ERROR')                 # Base threshold logging level.

    # Kombu.
    MAX_RETRIES = os.getenv('MAX_RETRIES', 10)                                      # Maximum 'ensure' limit.

    # AMQP. Note that the Outgoing Queue does not need to be named but this may be useful for monitoring etc.
    INCOMING_QUEUE_HOSTNAME = os.getenv('INCOMING_QUEUE_HOSTNAME', "")   # RabbitMQ IP address (amqp:// protocol)
    OUTGOING_QUEUE_HOSTNAME = os.getenv('OUTGOING_QUEUE_HOSTNAME', "")   # RabbitMQ IP address (amqp:// protocol)

    INCOMING_QUEUE = os.getenv('INCOMING_QUEUE', 'INCOMING_QUEUE')
    OUTGOING_QUEUE = os.getenv('OUTGOING_QUEUE', 'OUTGOING_QUEUE')

    # Queue binding keys: "Anything goes".
    INCOMING_KEY = os.getenv('INCOMING_KEY', '#')
    OUTGOING_KEY = os.getenv('OUTGOING_KEY', '#')

    # RabbitMQ Exchange default values:
    #   delivery_mode: '2' (persistent messages)
    #   durable: True (exchange remains 'active' on server re-start)
    # N.B.: 'name' is blank ("direct" type) by default, so it is required for non-direct types of exchange.
    INCOMING_EXCHANGE = kombu.Exchange(type="direct")
    OUTGOING_EXCHANGE = kombu.Exchange(type="topic", name="amq.topic")

    INCOMING_COUNT_EXCHANGE = kombu.Exchange(type="direct")
    OUTGOING_COUNT_EXCHANGE = kombu.Exchange(type="direct")

    # Collections: Incoming for a Consumer, Outgoing for a Producer.
    Configuration = namedtuple("Configuration", ['hostname', 'exchange', 'queue', 'binding_key'])
    INCOMING_CFG = Configuration(INCOMING_QUEUE_HOSTNAME, INCOMING_EXCHANGE, INCOMING_QUEUE, INCOMING_KEY)
    OUTGOING_CFG = Configuration(OUTGOING_QUEUE_HOSTNAME, OUTGOING_EXCHANGE, OUTGOING_QUEUE, OUTGOING_KEY)
    INCOMING_COUNT_CFG = Configuration(INCOMING_QUEUE_HOSTNAME, INCOMING_COUNT_EXCHANGE, INCOMING_QUEUE, INCOMING_KEY)
    OUTGOING_COUNT_CFG = Configuration(OUTGOING_QUEUE_HOSTNAME, OUTGOING_COUNT_EXCHANGE, OUTGOING_QUEUE, OUTGOING_KEY)


class DevelopmentConfig(Config):
    LOG_THRESHOLD_LEVEL = os.getenv('LOG_THRESHOLD_LEVEL', 'DEBUG')

class TestConfig(Config):
    LOG_THRESHOLD_LEVEL = os.getenv('LOG_THRESHOLD_LEVEL', 'INFO')

class PreviewConfig(Config):
    LOG_THRESHOLD_LEVEL = os.getenv('LOG_THRESHOLD_LEVEL', 'INFO')

class PreProductionConfig(Config):
    LOG_THRESHOLD_LEVEL = os.getenv('LOG_THRESHOLD_LEVEL', 'INFO')

class ProductionConfig(Config):
    LOG_THRESHOLD_LEVEL = os.getenv('LOG_THRESHOLD_LEVEL', 'INFO')
