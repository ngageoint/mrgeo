package org.mrgeo.hdfs.utils;

import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by ericwood on 6/13/16.
 */
public class MapFileReaderBuilder
{
private static final Logger logger = LoggerFactory.getLogger(MapFileReaderBuilder.class);

private Reader mapFileReader;
private KeyValueHelper keyValueHelper;

public MapFileReaderBuilder()
{
  mapFileReader = mock(Reader.class);
  keyValueHelper = new KeyValueHelper();
}

public MapFileReaderBuilder keyClass(Class keyClass)
{
  keyValueHelper.keyClass(keyClass);

  return this;
}

public MapFileReaderBuilder valueClass(Class valueClass)
{
  keyValueHelper.valueClass(valueClass);

  return this;
}

public MapFileReaderBuilder keys(WritableComparable[] keys)
{
  keyValueHelper.keys(keys);

  return this;
}

public MapFileReaderBuilder values(Writable[] values)
{
  keyValueHelper.values(values);

  return this;
}

public Reader build() throws IOException
{
  when(mapFileReader.getKeyClass()).thenReturn(keyValueHelper.getKeyClass());
  when(mapFileReader.getValueClass()).thenReturn(keyValueHelper.getValueClass());

  when(mapFileReader.next(any(WritableComparable.class), any(Writable.class))).thenAnswer(new Answer<Boolean>()
  {
    @Override
    public Boolean answer(InvocationOnMock invocationOnMock) throws Throwable
    {
      // Get the key and value
      Object[] args = invocationOnMock.getArguments();
      Writable key = (Writable) args[0];
      Writable value = (Writable) args[1];
      return keyValueHelper.next(key, value);
    }
  });

  when(mapFileReader.getClosest(any(WritableComparable.class), any(Writable.class))).thenAnswer(new Answer<Writable>()
  {
    @Override
    public Writable answer(InvocationOnMock invocationOnMock) throws Throwable
    {
      // Get the key and value
      Object[] args = invocationOnMock.getArguments();
      WritableComparable key = (WritableComparable) args[0];
      Writable value = (Writable) args[1];
      return keyValueHelper.getClosest(key, value);
    }
  });

  when(mapFileReader.getClosest(any(WritableComparable.class), any(Writable.class), anyBoolean()))
      .thenAnswer(new Answer<Writable>()
      {
        @Override
        public Writable answer(InvocationOnMock invocationOnMock) throws Throwable
        {
          // Get the key and value
          Object[] args = invocationOnMock.getArguments();
          WritableComparable key = (WritableComparable) args[0];
          Writable value = (Writable) args[1];
          return keyValueHelper.getClosest(key, value, (Boolean) args[2]);
        }
      });

  return mapFileReader;
}
}
