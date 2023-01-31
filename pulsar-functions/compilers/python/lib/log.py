#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# -*- encoding: utf-8 -*-

''' log.py '''
import logging
import logging.config
import logging.handlers
import os

from .Context_pb2 import PulsarMessage

# Create the logger
# pylint: disable=invalid-name
logging.basicConfig()
Log = logging.getLogger()

# time formatter - date - time - UTC offset
# e.g. "08/16/1988 21:30:00 +1030"
# see time formatter documentation for more
date_format = "%Y-%m-%d %H:%M:%S %z"


class LogGRPCHandler(logging.Handler):
    def __init__(self, topic, stub):
        logging.Handler.__init__(self)
        Log.info("Setting up grpc client for log")
        self.stub = stub
        self.topic = topic

    def emit(self, record):
        msg = self.format(record)
        self.stub.Publish(PulsarMessage(topic=self.topic, payload=msg.encode('utf-8')))


def remove_all_handlers():
    retval = None
    for handler in Log.handlers:
        Log.handlers.remove(handler)
        retval = handler
    return retval


def add_handler(stream_handler):
    log_format = "[%(asctime)s] [%(levelname)s]: %(message)s"
    formatter = logging.Formatter(fmt=log_format, datefmt=date_format)
    stream_handler.setFormatter(formatter)
    Log.addHandler(stream_handler)


def init_logger(level, logfile):
    # get log file location for function instance
    os.environ['LOG_FILE'] = logfile
    if level is not None:
        Log.setLevel(level)
        for h in Log.handlers:
            h.setLevel(level)
