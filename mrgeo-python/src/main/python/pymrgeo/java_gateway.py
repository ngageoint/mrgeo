#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
The gateway to communicate with MrGeo running in a JVM.

The interface is inspired by the python interface in Spark (PySpark)
https://spark.apache.org/docs/latest/api/python/index.html

"""
import atexit
import fnmatch
import os
import select
import signal
import socket
import struct
import sys
import time
from subprocess import Popen, PIPE

from py4j.java_collections import ListConverter
from py4j.java_gateway import java_import, JavaGateway, GatewayClient, get_method

if sys.version >= '3':
    xrange = range

_forked_proc = None
_isremote = False


# patching ListConverter, or it will convert bytearray into Java ArrayList
def can_convert_list(self, obj):
    return isinstance(obj, (list, tuple, xrange))


ListConverter.can_convert = can_convert_list


def find_script():
    if "MRGEO_COMMON_HOME" in os.environ:
        mrgeo_home = os.environ["MRGEO_COMMON_HOME"]
    elif "MRGEO_HOME" in os.environ:
        mrgeo_home = os.environ["MRGEO_HOME"]
        print("MRGEO_HOME has been deprecated, use MRGEO_COMMON_HOME and MRGEO_CONF_DIR instead.")
    else:
        raise Exception("MRGEO_COMMON_HOME is not set!")

    script = "mrgeo"

    if os.path.isfile(os.path.join(mrgeo_home, script)):
        return os.path.join(mrgeo_home, script)

    if os.path.isfile(os.path.join(mrgeo_home + "/bin", script)):
        return os.path.join(mrgeo_home + "/bin", script)

    for root, dirnames, filenames in os.walk(mrgeo_home):
        for filename in fnmatch.filter(filenames, script):
            return os.path.join(root, filename)

    raise Exception('Can not find "' + script + '" within MRGEO_COMMON_HOME (' + mrgeo_home + ')')


def is_remote():
    return _isremote


def terminate():
    global _forked_proc
    if _forked_proc is not None:
        pid = _forked_proc.pid
        os.killpg(os.getpgid(pid), signal.SIGTERM)
        # _forked_proc.kill()
        _forked_proc = None


def launch_gateway(host=None, port=None):
    global _isremote
    global _forked_proc
    requesthost = socket.gethostname()
    requestport = 0

    # Launch the Py4j gateway using the MrGeo command so that we pick up the proper classpath

    fork = True

    if host is not None and port is not None:
        requesthost = host
        requestport = port
        fork = False
    else:
        if "MRGEO_HOST" in os.environ:
            requesthost = os.environ["MRGEO_HOST"]
            fork = False

        if "MRGEO_PORT" in os.environ:
            requestport = int(os.environ["MRGEO_PORT"])
            fork = False

    if port is not None and requestport == 0:
        requestport = port

    # If we didn't get a request port, get one.  We open a socket to make sure we get an unused
    # port, without guessing,
    if requestport == 0:
        tmp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tmp_socket.settimeout(0.01)
        tmp_socket.bind((requesthost, 0))
        # tmp_socket.listen(1)

        name, requestport = tmp_socket.getsockname()
        tmp_socket.close()

    if fork:
        # Start a socket that will be used by PythonGatewayServer to communicate its port to us

        script = find_script()

        # command = [script, "python", "-v", "-p", str(requestport)]
        command = [script, "python", "-p", str(requestport)]

        environ = os.environ
        # Add some more memory
        environ['HADOOP_CLIENT_OPTS'] = '-Xmx12G ' + environ.get('HADOOP_CLIENT_OPTS', '')

        # Allow remote debugging
        # environ['HADOOP_CLIENT_OPTS'] = '-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 ' + environ.get('HADOOP_CLIENT_OPTS', '')

        # Launch the Java gateway.
        # We open a pipe to stdin so that the Java gateway can die when the pipe is broken
        # Don't send ctrl-c / SIGINT to the Java gateway:
        def preexec_func():
            os.setsid()
            signal.signal(signal.SIGINT, signal.SIG_IGN)

        _forked_proc = Popen(command, stdin=PIPE, preexec_fn=preexec_func, env=environ, bufsize=1,
                             universal_newlines=True)

        # while True:
        #     out = _forked_proc.stdout.read(1)
        #
        #     # print("[" + out + "] " + str(_forked_proc.poll()))
        #     if out != '':
        #         break
        #
        #     if _forked_proc.poll() is not None:
        #         raise Exception("Java gateway process exited before sending the driver its port number: returned: " +
        #                         str(_forked_proc.poll()))

        # time.sleep(5)
        # We use select() here in order to avoid blocking indefinitely if the subprocess dies
        # before connecting
        # while proc.poll() is None:
        #     pass

        # _forked_proc.stdout = subprocess.STDOUT

        atexit.register(terminate)

    timeout = 30  # (seconds)
    request_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    request_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    start = time.time()
    connected = -1
    while (time.time() - start) < timeout and connected != 0:
        connected = request_socket.connect_ex((requesthost, requestport))
        time.sleep(0.5)

    if connected != 0:
        raise Exception("Could not connect to the java gateway process")

    readable, writable, error = select.select([request_socket], [], [], timeout)

    # read the communication port from the server
    if request_socket in readable:

        data = ""
        while len(data) < 8:
            # keep it to 4 bytes (an int)
            data += request_socket.recv(8)

        java_python_port, python_java_port = struct.unpack("!ii", data)
        request_socket.close()
    else:
        raise Exception("Port is not readable")

    _isremote = not fork

    if java_python_port is None:
        raise Exception("Java gateway process exited before sending the driver its port number")

    print("Talking with MrGeo on port " + str(java_python_port))

    # Connect to the gateway
    gateway_client = GatewayClient(address=requesthost, port=java_python_port)
    gateway = JavaGateway(gateway_client=gateway_client, auto_convert=True, python_proxy_port=python_java_port)

    # Import the classes used by MrGeo
    java_import(gateway.jvm, "org.mrgeo.python.*")

    # Import classes used by Spark
    java_import(gateway.jvm, "org.apache.spark.SparkConf")
    java_import(gateway.jvm, "org.apache.spark.api.java.*")
    java_import(gateway.jvm, "org.apache.spark.api.python.*")
    java_import(gateway.jvm, "org.apache.spark.mllib.api.python.*")

    return gateway, gateway_client

    # Scala classes have automatic getters & setters generated for
    # public fields, <field>() is the getter, <field>_$eq(<type>) is the setter


def get_field(java_object, field_name):
    method = get_method(java_object, field_name)
    return method()


def set_field(java_object, field_name, value):
    method = get_method(java_object, field_name + "_$eq")
    return method(value)
