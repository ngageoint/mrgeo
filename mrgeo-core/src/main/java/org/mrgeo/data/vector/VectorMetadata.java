/*
 * Copyright 2009-2014 DigitalGlobe, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.mrgeo.data.vector;

import org.mrgeo.utils.Bounds;

import java.util.HashMap;
import java.util.Map;

public class VectorMetadata
{
  private Map<String, Object> metadata = new HashMap<String, Object>();
  private Bounds bounds;

  public VectorMetadata()
  {
  }

  public Bounds getBounds()
  {
    return bounds;
  }

  public void setBounds(final Bounds bounds)
  {
    this.bounds = bounds;
  }

  public void set(final String key, final String value)
  {
    metadata.put(key, value);
  }

  public void set(final String key, final int value)
  {
    metadata.put(key, value);
  }

  public void set(final String key, final long value)
  {
    metadata.put(key, value);
  }

  public void set(final String key, final float value)
  {
    metadata.put(key, value);
  }

  public void set(final String key, final double value)
  {
    metadata.put(key, value);
  }

  public void set(final String key, final Object value)
  {
    metadata.put(key, value);
  }

  public String getString(final String key)
  {
    Object value = metadata.get(key);
    if (value == null)
    {
      return null;
    }
    if (value instanceof String)
    {
      return (String)value;
    }
    return null;
  }

  public int getInt(final String key, final int defaultValue)
  {
    Object value = metadata.get(key);
    if (value == null)
    {
      return defaultValue;
    }
    if (value instanceof Integer)
    {
      return ((Integer)value).intValue();
    }
    return defaultValue;
  }

  public long getLong(final String key, final long defaultValue)
  {
    Object value = metadata.get(key);
    if (value == null)
    {
      return defaultValue;
    }
    if (value instanceof Long)
    {
      return ((Long)value).longValue();
    }
    return defaultValue;
  }

  public float getFloat(final String key, final float defaultValue)
  {
    Object value = metadata.get(key);
    if (value == null)
    {
      return defaultValue;
    }
    if (value instanceof Float)
    {
      return ((Float)value).floatValue();
    }
    return defaultValue;
  }

  public double getDouble(final String key, final double defaultValue)
  {
    Object value = metadata.get(key);
    if (value == null)
    {
      return defaultValue;
    }
    if (value instanceof Double)
    {
      return ((Double)value).doubleValue();
    }
    return defaultValue;
  }

  public Object getObject(final String key)
  {
    return metadata.get(key);
  }
}
