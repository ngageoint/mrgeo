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
PyMrGeo is the Python API for MrGeo.

The interface is inspired by the python interface in Spark (PySpark)
https://spark.apache.org/docs/latest/api/python/index.html


Public classes:

  - :class:`MrGeo`:
      Main entry point for MrGeo functionality.

"""

import os
import sys


def walker(dirname, filename):
    for (root, names, files) in os.walk(dirname):
        # print('looking in ' + root)
        for f in files:
            if f == filename:
                return root + '/' + filename
    return None


def loadlib(lib):
    if lib in sys.path:
        print(lib + ' aready in path')
        return

    libpath = None

    try:
        pypath = os.environ['PYTHONPATH'].split(os.pathsep)
    except KeyError:
        pypath = []

    # print('pypath is: ' + str(pypath))
    for dirname in pypath:
        libpath = walker(dirname, lib)
        if libpath is not None:
            sys.path.append(libpath)
            break

    if libpath is None:
        libpath = walker('..', lib)
        if libpath is not None:
            sys.path.append(libpath)

    if libpath is None:
        print('Can\'t find library! ' + lib)
    else:
        print('Found ' + lib + ' in: ' + str(libpath))


# print(os.environ['PYTHONPATH'].split(os.pathsep))
# print(sys.path)

# loadlib('py4j-0.8.2.1-src.zip')

from pymrgeo.mrgeo import MrGeo
from pymrgeo.rastermapop import RasterMapOp
from pymrgeo.vectormapop import VectorMapOp

__all__ = [
    "MrGeo"
    "RasterMapOp"
    "VectorMapOp"
]
