/*
 * Copyright 2009-2015 DigitalGlobe, Inc.
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

package org.mrgeo.imageio;

import com.sun.media.jai.codec.SeekableStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.stream.ImageInputStreamImpl;
import java.io.IOException;
import java.io.InputStream;
import java.util.Stack;

public class SeekableImageInputStream extends ImageInputStreamImpl
{
  private static Logger log = LoggerFactory.getLogger(SeekableImageInputStream.class);

  final private SeekableStream _input;
  final private Stack<Long> stack = new Stack<Long>();

  public SeekableImageInputStream(final SeekableStream input)
  {
    try
    {
      bitOffset = 0;
      _input = input;

      // NOTE: Geotools reuses the input stream (FSDataInputStream) in multiple
      // ImageInputStream instances. They also seem to assume the stream is at
      // position 0 when the ImageInputStream is created, so we seek to 0 here
      // to satisfy that. It's not ideal, but it works.
      seek(0);
    }
    catch (final Exception e)
    {
      throw new RuntimeException(e);
    }
  }

  // NOTE:  Because Geotools resuses the input stream, we can't close it here.  
  // Instead, the person originally opening the stream is responsible for 
  // closing it in the appropriate place. Yuck!
  //  @Override
  //  public void close() throws IOException
  //  {
  //    _input.close();
  //  }

  public InputStream getStream()
  {
    return _input;
  }


  @Override
  public void mark()
  {
    try
    {
      stack.add(getStreamPosition());
    }
    catch (final IOException e)
    {
      throw new RuntimeException(e);
    }

  }

  @Override
  public int read() throws IOException
  {
    long start = System.currentTimeMillis();

    bitOffset = 0;
    final int result = _input.read();
    streamPos += result;

    log.debug("read (1) - " + result + " (pos: " + streamPos + 
        ", time: " + (System.currentTimeMillis() - start) + "ms)" );

    return result;
  }

  @Override
  public int read(final byte[] b, final int off, final int len) throws IOException
  {
    //    long start = System.currentTimeMillis();

    bitOffset = 0;

    
    // 
    // FSDataInputStream documentation: read() - Reads up to len bytes of data from the 
    // contained input stream into an array of bytes. An attempt is made to read as 
    // many as len bytes, but a smaller number may be read, possibly zero. 
    // The number of bytes actually read is returned as an integer.
    //
    // this means we need to loop until the correct number of bytes are read, -1 is returned

    int bytesread = 0;
    while (bytesread < len)
    {
      final int result = _input.read(b, bytesread + off, len - bytesread);

      // no data left, return -1;
      if (result == -1)
      {
        return result;
      }

      streamPos += result;
      bytesread += result;
    }

    return bytesread;
  }

  @Override
  public void reset() throws IOException
  {
    bitOffset = 0;
    if (!stack.isEmpty())
    {
      seek(stack.pop());
    }
  }

  @Override
  public void seek(final long pos)
  {
    try
    {
      _input.seek(pos);
      streamPos = pos;

      if (pos != streamPos)
      {
        log.debug("seek error!!!");
      }
    }
    catch (final IOException e)
    {
      streamPos = -1;
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("unused")
  private String printaddr()
  {
    return Integer.toHexString(hashCode()) + ":" + Integer.toHexString(_input.hashCode()) + ":"
        + Long.toHexString(Thread.currentThread().getId());
  }

}
