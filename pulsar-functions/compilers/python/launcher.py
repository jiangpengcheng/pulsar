#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from __future__ import print_function
from sys import stdin
from sys import stdout
from sys import stderr
from os import fdopen
import sys, os, json, traceback, warnings, inspect, importlib, logging

Log = logging.getLogger()
PULSAR_API_ROOT = 'pulsar'
PULSAR_FUNCTIONS_API_ROOT = 'functions'

def import_class(from_path, full_class_name):
  from_path = str(from_path)
  full_class_name = str(full_class_name)
  try:
    return import_class_from_path(from_path, full_class_name)
  except Exception as e:
    our_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
    api_dir = os.path.join(our_dir, PULSAR_API_ROOT, PULSAR_FUNCTIONS_API_ROOT)
    try:
      return import_class_from_path(api_dir, full_class_name)
    except Exception as e:
      Log.info("Failed to import class %s from path %s" % (full_class_name, from_path))
      Log.info(e, exc_info=True)
      return None

def import_class_from_path(from_path, full_class_name):
  Log.debug('Trying to import %s from path %s' % (full_class_name, from_path))
  split = full_class_name.split('.')
  classname_path = '.'.join(split[:-1])
  class_name = full_class_name.split('.')[-1]
  if from_path not in sys.path:
    Log.debug("Add a new dependency to the path: %s" % from_path)
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

try:
  # if the directory 'virtualenv' is extracted out of a zip file
  path_to_virtualenv = os.path.abspath('./virtualenv')
  if os.path.isdir(path_to_virtualenv):
    # activate the virtualenv using activate_this.py contained in the virtualenv
    activate_this_file = path_to_virtualenv + '/bin/activate_this.py'
    if not os.path.exists(activate_this_file): # try windows path
      activate_this_file = path_to_virtualenv + '/Scripts/activate_this.py'
    if os.path.exists(activate_this_file):
      with open(activate_this_file) as f:
        code = compile(f.read(), activate_this_file, 'exec')
        exec(code, dict(__file__=activate_this_file))
    else:
      sys.stderr.write("Invalid virtualenv. Zip file does not include 'activate_this.py'.\n")
      sys.exit(1)
except Exception:
  traceback.print_exc(file=sys.stderr, limit=0)
  sys.exit(1)

function_kclass = import_class(os.path.dirname("action-src/"), "CLASS_NAME")

function_class = None
function_purefunction = None

try:
  function_class = function_kclass()
except:
  function_purefunction = function_kclass

env = os.environ
while True:
  line = stdin.readline().rstrip()
  if not line: break
  res = None
  try:
    if function_class is not None:
      res = function_class.process(line, None)
    else:
      res = function_purefunction.process(line)
  except Exception as ex:
    print(traceback.format_exc(), file=stderr)
    res = json.dumps({"error": str(ex)}, ensure_ascii=False)
  stderr.write("=======got response: %s\n" % res)
  stdout.write(res)
  stdout.write('\n')
  stdout.flush()
  stderr.flush()
