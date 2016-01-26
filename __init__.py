import threading
from queue_manager.manager import run
from queue_manager.config import APP_NAME

"""
AMQP (RabbitMQ) Queue Manager
* Derived from https://github.com/LandRegistry/register-publisher/blob/develop/application/server.py
* N.B. This module is a good candidate for a common library package, perhaps deployed as a Git Submodule.

* AMQP defines four type of exchange, one of which is 'topic'; that enables clients to subscribe on an 'ad hoc' basis.
* RabbitMQ etc. should have default exchanges in place; 'amq.fanout' for example.

See http://www.rabbitmq.com/blog/2010/10/19/exchange-to-exchange-bindings for an alternative arrangement, which may be
unique to RabbitMQ. This might avoid the unpack/pack issue of 'process_message()' but it does not permit logging etc.

"""

# Run the flask app in a new thread.
process_thread = threading.Thread(name='{}'.format(APP_NAME), target=run)
process_thread.setDaemon(True)
process_thread.start()
