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

package org.mrgeo.core;

import com.sun.media.jai.codec.FileSeekableStream;
import com.sun.media.jai.codec.SeekableStream;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;

/**
 * 
 */
public class SerializableSeekableFile extends SeekableStream implements Serializable
{
  private static final long serialVersionUID = 1L;
  File file;
  private transient FileSeekableStream stream;

  /**
   * @param file
   * @throws IOException
   */
  public SerializableSeekableFile(File file) throws IOException
  {
    stream = new FileSeekableStream(file);
    this.file = file;
  }

  /**
   * Convenience function similar to above
   * 
   * @param fileName
   * @throws IOException
   */
  public SerializableSeekableFile(String fileName) throws IOException
  {
    this(new File(fileName));
  }

  @Override
  public boolean canSeekBackwards()
  {
    return stream.canSeekBackwards();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.sun.media.jai.codec.SeekableStream#getFilePointer()
   */
  @Override
  public long getFilePointer() throws IOException
  {
    return stream.getFilePointer();
  }

  @Override
  public synchronized void mark(int readLimit)
  {
    stream.mark(readLimit);
  }

  @Override
  public boolean markSupported()
  {
    return stream.markSupported();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.sun.media.jai.codec.SeekableStream#read()
   */
  @Override
  public int read() throws IOException
  {
    return stream.read();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.sun.media.jai.codec.SeekableStream#read(byte[], int, int)
   */
  @Override
  public int read(byte[] arg0, int arg1, int arg2) throws IOException
  {
    return stream.read(arg0, arg1, arg2);
  }

  private void readObject(ObjectInputStream aInputStream) throws ClassNotFoundException,
      IOException
  {
    aInputStream.defaultReadObject();
    stream = new FileSeekableStream(file);
  }

  @Override
  public synchronized void reset() throws IOException
  {
    stream.reset();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.sun.media.jai.codec.SeekableStream#seek(long)
   */
  @Override
  public void seek(long arg0) throws IOException
  {
    stream.seek(arg0);
  }

  public int skip(int n) throws IOException
  {
    return stream.skip(n);
  }
}
