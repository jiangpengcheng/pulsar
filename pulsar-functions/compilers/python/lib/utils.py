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

import importlib
import inspect
import json
import os
import pulsar
import sys
from .log import DebugLogger
from pulsar.functions import serde

PULSAR_API_ROOT = 'pulsar'
PULSAR_FUNCTIONS_API_ROOT = 'functions'
DEFAULT_SERIALIZER = serde.IdentitySerDe()


def import_class(from_path, full_class_name):
    from_path = str(from_path)
    full_class_name = str(full_class_name)
    try:
        return import_class_from_path(from_path, full_class_name)
    except Exception as e:
        DebugLogger.error("Failed to import class {} from path {} with error {}".format(full_class_name, from_path, e))
        our_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
        api_dir = os.path.join(our_dir, PULSAR_API_ROOT, PULSAR_FUNCTIONS_API_ROOT)
        try:
            return import_class_from_path(api_dir, full_class_name)
        except Exception as ex:
            DebugLogger.error("Failed to import class {} from path {} with error {}".format(full_class_name, api_dir, ex))
            return None


def import_class_from_path(from_path, full_class_name):
    split = full_class_name.split('.')
    classname_path = '.'.join(split[:-1])
    class_name = full_class_name.split('.')[-1]
    if from_path not in sys.path:
        sys.path.insert(0, from_path)
    if not classname_path:
        mod = importlib.import_module(class_name)
        return mod
    else:
        # Serde modules is being used in unqualified form instead of using
        # the full name `pulsar.functions.serde`, so we have to make sure
        # it gets resolved correctly.
        if classname_path == 'serde':
            mod = serde
        else:
            mod = importlib.import_module(classname_path)
        retval = getattr(mod, class_name)
        return retval


def get_schema(schema_type, type_class_name, schema_properties, work_dir):
    if type_class_name is None or type_class_name == "":
        return None
    if type_class_name.lower() == "string":
        schema = pulsar.schema.StringSchema()
    elif schema_type is None or schema_type == "":
        return None
    elif schema_type.lower() == "bytes":
        schema = pulsar.schema.BytesSchema()
    elif schema_type.lower() == "string":
        schema = pulsar.schema.StringSchema()
    elif schema_type.lower() == "json":
        record_kclass = get_record_class(type_class_name, work_dir)
        schema = pulsar.schema.JsonSchema(record_kclass)
    elif schema_type.lower() == "avro":
        record_kclass = get_record_class(type_class_name, work_dir)
        schema = pulsar.schema.AvroSchema(record_kclass, schema_properties)
    else:  # load custom schema
        record_kclass = get_record_class(type_class_name, work_dir)
        schema_kclass = import_class(os.path.dirname(work_dir), schema_type)
        try:
            args_count = len(inspect.signature(schema_kclass.__init__).parameters)
        except:  # for compatibility with python 2
            args_count = len(inspect.getargspec(schema_kclass.__init__).args)
        if args_count == 1:  # doesn't take any arguments
            schema = schema_kclass()
        elif args_count == 2:  # take one argument, it can be either schema properties or record class
            try:
                schema = schema_kclass(record_kclass)
            except TypeError:
                schema = schema_kclass(schema_properties)
        elif args_count == 3:  # take two or more arguments
            schema = schema_kclass(record_kclass, schema_properties)
        else:
            raise Exception("Invalid schema class %s" % schema_type)
    return schema


def get_record_class(class_name, work_dir):
    record_class = None
    if class_name is not None and len(class_name) > 0:
        try:
            record_class = import_class(os.path.dirname(work_dir), class_name)
        except:
            pass
    return record_class


def import_function_class(work_dir, function_class_name):
    function_kclass = import_class(os.path.dirname(work_dir), function_class_name)
    function_class = None
    function_pure_function = None
    try:
        function_class = function_kclass()
    except:
        function_pure_function = function_kclass
    return function_class, function_pure_function


def setup_input_serde(topics_to_serde_class_name, input_specs, type_class_name, work_dir):
    input_serdes = {}
    input_schemas = {}
    for topic, serde_class in topics_to_serde_class_name.items():
        if not serde_class:
            input_serdes[topic] = DEFAULT_SERIALIZER
        else:
            serde_kclass = import_class(os.path.dirname(work_dir), serde_class)
            input_serdes[topic] = serde_kclass()

    for topic, consumer_conf in input_specs.items():
        if not consumer_conf.get("serdeClassName"):
            input_serdes[topic] = DEFAULT_SERIALIZER
        else:
            serde_kclass = import_class(os.path.dirname(work_dir), consumer_conf.get("serdeClassName"))
            input_serdes[topic] = serde_kclass()
        properties = json.loads(consumer_conf.get("schemaProperties")) if consumer_conf.get("schemaProperties") else {}
        input_schemas[topic] = get_schema(consumer_conf.get("schemaType"), type_class_name, properties, work_dir)
    return input_serdes, input_schemas


def setup_output_serde(serde_class_name, schema_type, type_class_name, schema_properties, work_dir):
    output_schema = get_schema(schema_type, type_class_name, schema_properties, work_dir)
    if serde_class_name is not None and len(serde_class_name) > 0:
        serde_class = import_class(os.path.dirname(work_dir), serde_class_name)
        output_serde = serde_class()
    else:
        output_serde = DEFAULT_SERIALIZER
    return output_serde, output_schema


def get_fully_qualified_function_name(tenant, namespace, name):
    return "%s/%s/%s" % (tenant, namespace, name)


def get_fully_qualified_instance_id(tenant, namespace, name, instance_id):
    return "%s/%s/%s:%s" % (tenant, namespace, name, instance_id)
