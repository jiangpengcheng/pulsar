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

"""state_context.py: state context for accessing managed state
"""
from abc import abstractmethod


class StateContext(object):
    """Interface defining operations on managed state"""

    @abstractmethod
    def incr(self, key, amount):
        pass

    @abstractmethod
    def put(self, key, value):
        pass

    @abstractmethod
    def get_value(self, key):
        pass

    @abstractmethod
    def get_amount(self, key):
        pass


class NullStateContext(StateContext):
    """A state context that does nothing"""

    def incr(self, key, amount):
        return

    def put(self, key, value):
        return

    def get_value(self, key):
        return None

    def get_amount(self, key):
        return None
