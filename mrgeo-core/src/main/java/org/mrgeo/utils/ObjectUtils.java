/*
 * Copyright 2009-2017. DigitalGlobe, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.mrgeo.utils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.*;

class ObjectUtils
{
static byte[] encodeObject(Object obj) throws IOException
{
  try (ByteArrayOutputStream baos = new ByteArrayOutputStream())
  {
    try (ObjectOutputStream oos = new ObjectOutputStream(baos))
    {
      oos.writeObject(obj);
      return baos.toByteArray();
    }
  }
}

@SuppressFBWarnings(value = "OBJECT_DESERIALIZATION", justification = "only decoding metadata, and package local")
static Object decodeObject(byte[] bytes) throws IOException, ClassNotFoundException
{
  try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes))
  {
    try (ObjectInputStream ois = new ObjectInputStream(bais))
    {
      return ois.readObject();
    }
  }
}
}
