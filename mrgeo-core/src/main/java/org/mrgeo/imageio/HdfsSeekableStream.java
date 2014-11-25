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

package org.mrgeo.imageio;

import com.sun.media.jai.codec.SeekableStream;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.utils.LeakChecker;

import java.io.IOException;

public class HdfsSeekableStream extends SeekableStream
{
  FSDataInputStream stream;
  private final boolean profile;

  public HdfsSeekableStream(Path path) throws IOException
  {
    //FileSystem fs = HadoopFileUtils.getFileSystem(path);
    
    stream = new FSDataInputStream(HadoopFileUtils.open(path)); // fs.open(path);
    
    if (System.getProperty("mrgeo.profile", "false").compareToIgnoreCase("true") == 0)
    {
      profile = true;
      if (profile)
      {
        LeakChecker.instance().add(this, ExceptionUtils.getFullStackTrace(new Throwable("HdfsMrsImageReader creation stack(ignore the Throwable...)")));
      }

    }
    else
    {
      profile = false;
    }

  }

  public HdfsSeekableStream(FSDataInputStream input)
  {
    stream = input;
    
    if (System.getProperty("mrgeo.profile", "false").compareToIgnoreCase("true") == 0)
    {
      profile = true;
      if (profile)
      {
        LeakChecker.instance().add(this, ExceptionUtils.getFullStackTrace(new Throwable("HdfsMrsImageReader creation stack(ignore the Throwable...)")));
      }

    }
    else
    {
      profile = false;
    }

  }

  @Override
  public void close()
  {
    if (stream != null)
    {
      try
      {
        stream.close();
        if (profile)
        {
          LeakChecker.instance().remove(this);
        }
      }
      catch (IOException e)
      {
        e.printStackTrace();
      }
      stream = null;
    }
  }
  
  @Override
  public long getFilePointer() throws IOException
  {
    return stream.getPos();
  }

  @Override
  public int read() throws IOException
  {
    return stream.read();
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException
  {
    return stream.read(b, off, len);
  }

  @Override
  public void seek(long pos) throws IOException
  {
    stream.seek(pos);
  }

}
