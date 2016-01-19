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

import sys
from os import walk

# find the py4j lib in the mrgeo path
for (root, names, files) in walk('..'):
    for file in files:
        if file == 'py4j-0.8.2.1-src.zip':
            sys.path.append(root + '/py4j-0.8.2.1-src.zip')

from pymrgeo.mrgeo import MrGeo
from pymrgeo.rastermapop import RasterMapOp


__all__ = [
    "MrGeo"
    "RasterMapOp"
]
