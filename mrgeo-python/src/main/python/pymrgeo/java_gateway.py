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

import fnmatch
import os
import sys
import select
import signal
import socket
import struct
from subprocess import Popen, PIPE
from py4j.java_gateway import java_import, JavaGateway, GatewayClient, get_method
from py4j.java_collections import ListConverter

if sys.version >= '3':
    xrange = range


_isremote = False


# patching ListConverter, or it will convert bytearray into Java ArrayList
def can_convert_list(self, obj):
    return isinstance(obj, (list, tuple, xrange))


ListConverter.can_convert = can_convert_list


def read_int(stream):
    length = stream.read(4)
    if not length:
        raise EOFError
    return struct.unpack("!i", length)[0]


def find_script():
    if "MRGEO_COMMON_HOME" in os.environ:
        mrgeo_home = os.environ["MRGEO_COMMON_HOME"]
    elif "MRGEO_HOME" in os.environ:
        mrgeo_home = os.environ["MRGEO_HOME"]
        print("MRGEO_HOME has been deprecated, use MRGEO_COMMON_HOME and MRGEO_CONF_DIR instead.")
    else:
        raise Exception("MRGEO_HOME is not set!")

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

def launch_gateway():
    global _isremote
    requesthost = socket.gethostname()
    requestport = 0

    if "MRGEO_GATEWAY_PORT" in os.environ:
        gateway_port = int(os.environ["MRGEO_GATEWAY_PORT"])
    else:
        # Launch the Py4j gateway using the MrGeo command so that we pick up the proper classpath

        script = find_script()

        fork = True
        if "MRGEO_HOST" in os.environ:
            requesthost = os.environ["MRGEO_HOST"]
            fork = False

        if "MRGEO_PORT" in os.environ:
            requestport = int(os.environ["MRGEO_PORT"])
            fork = False

        # Start a socket that will be used by PythonGatewayServer to communicate its port to us
        callback_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        callback_socket.bind(('127.0.0.1', 0))

        callback_socket.listen(1)
        callback_host, callback_port = callback_socket.getsockname()

        gateway_port = None

        if fork:
            command = [script, "python", "-v", "-h", callback_host, "-p", str(callback_port)]

            # Launch the Java gateway.
            # We open a pipe to stdin so that the Java gateway can die when the pipe is broken
            # Don't send ctrl-c / SIGINT to the Java gateway:
            def preexec_func():
                signal.signal(signal.SIGINT, signal.SIG_IGN)

            proc = Popen(command, stdin=PIPE, preexec_fn=preexec_func)

            # We use select() here in order to avoid blocking indefinitely if the subprocess dies
            # before connecting
            while gateway_port is None and proc.poll() is None:
                timeout = 1  # (seconds)
                readable, _, _ = select.select([callback_socket], [], [], timeout)
                if callback_socket in readable:
                    gateway_connection = callback_socket.accept()[0]
                    # Determine which ephemeral port the server started on:
                    gateway_port = read_int(gateway_connection.makefile(mode="rb"))

                    gateway_connection.close()
                    callback_socket.close()
        else:
            connection_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            connection_socket.connect((requesthost, requestport))

            connection_socket.send(callback_host + "\n")
            connection_socket.send(str(callback_port) + "\n")

            connection_socket.close()

            timeout = 60  # (seconds)
            readable, _, _ = select.select([callback_socket], [], [], timeout)
            if callback_socket in readable:
                gateway_connection = callback_socket.accept()[0]
                # Determine which ephemeral port the server started on:
                gateway_port = read_int(gateway_connection.makefile(mode="rb"))
                gateway_connection.close()
                callback_socket.close()

        _isremote = not fork

        if gateway_port is None:
                    raise Exception("Java gateway process exited before sending the driver its port number")

    print("Talking with MrGeo on port " + str(gateway_port))

    # Connect to the gateway
    gateway = JavaGateway(GatewayClient(address=requesthost, port=gateway_port), auto_convert=True)

    # Import the classes used by MrGeo
    java_import(gateway.jvm, "org.mrgeo.python.*")

    # Import classes used by Spark
    java_import(gateway.jvm, "org.apache.spark.SparkConf")
    java_import(gateway.jvm, "org.apache.spark.api.java.*")
    java_import(gateway.jvm, "org.apache.spark.api.python.*")
    java_import(gateway.jvm, "org.apache.spark.mllib.api.python.*")

    return gateway

    # Scala classes have automatic getters & setters generated for
    # public fields, <field>() is the getter, <field>_$eq(<type>) is the setter


def get_field(java_object, field_name):
    method = get_method(java_object, field_name)
    return method()


def set_field(java_object, field_name, value):
    method = get_method(java_object, field_name + "_$eq")
    return method(value)
