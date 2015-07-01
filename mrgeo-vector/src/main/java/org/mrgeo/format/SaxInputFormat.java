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

package org.mrgeo.format;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.xerces.parsers.SAXParser;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.xml.sax.InputSource;

import java.io.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Reads specified chunks of XML from an XML document
 * 
 * This class assumes that there are no CDATA sections to the data.
 */
public class SaxInputFormat<KEY, VALUE> extends FileInputFormat<KEY, VALUE> implements Serializable
{
  private static final long serialVersionUID = 1L;

  @Override
  public RecordReader<KEY, VALUE> createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException
      {
    SaxRecordReader<KEY, VALUE> reader = new SaxRecordReader<KEY, VALUE>();
    reader.initialize(split, context);
    return reader;
      }

  static public class SaxRecordReader<KEY, VALUE> extends RecordReader<KEY, VALUE> implements
  Runnable
  {
    private InputStream is;
    private FSDataInputStream fdis;
    private Pattern beginPattern;
    private int maxSize = 400000;
    private int readChunkSize = 2048;
    private StringBuffer buffer = new StringBuffer();
    private KEY key;
    private VALUE value;
    private FileSplit split;
    private String patternStr = null;
    private String rootTag = null;
    private SaxContentHandler<KEY, VALUE> handler = null;
    private volatile Exception threadException = null;
    private TaskAttemptContext context;

    SaxRecordReader()
    {
    }

    @Override
    public void close() throws IOException
    {
      if (is != fdis)
      {
        is.close();
      }
      
      fdis.close();
    }

    @Override
    public float getProgress() throws IOException
    {
      long size = split.getLength();
      return (float) (fdis.getPos() - split.getStart()) / (float) size;
    }

    @SuppressWarnings("hiding")
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException
    {
      this.split = (FileSplit) split;
      this.context = context;

      try
      {
        synchronized (this)
        {
          new Thread(this).start();
          wait();
        }
      }
      catch (InterruptedException e)
      {
        throw new IOException(e);
      }
    }

    @Override
    public void run()
    {

      try
      {
        FileSystem fs = HadoopFileUtils.getFileSystem(this.split.getPath());
        fdis = fs.open(this.split.getPath());
        fdis.seek(this.split.getStart());

        if (patternStr == null)
        {
          patternStr = context.getConfiguration().get("xml.pattern");
        }
        rootTag = context.getConfiguration().get("xml.root.tag");

        // if this is not the first split we need to add a surrogate root tag.
        if (this.split.getStart() != 0)
        {
          // use this to look for a good starting point.
          beginPattern = Pattern.compile("<(" + patternStr + ")(([^>]*/>)| [^>]*>|>)");
          byte[] buf = new byte[readChunkSize];

          // start at the beginning and look for the first good starting point based on the input
          // pattern this operation may be slow so we'll start w/ about 2k worth of data and add
          // more.
          Matcher beginMatcher = beginPattern.matcher(buffer);
          while (beginMatcher.find() == false && fdis.getPos() - this.split.getStart() < maxSize)
          {
            int bytesRead = fdis.read(buf, 0, readChunkSize);
            buffer.append(new String(buf, 0, bytesRead, "UTF-8"));
            beginMatcher = beginPattern.matcher(buffer);
          }
          if (beginMatcher.hitEnd())
          {
            throw new IOException(String.format(
                "Could not find beginning tag in the first %d bytes", buffer.length()));
          }

          ByteArrayInputStream bais = new ByteArrayInputStream(rootTag.getBytes("UTF-8"));
          fdis.seek(this.split.getStart() + beginMatcher.start());
          is = new SequenceInputStream(bais, fdis);
        }
        else
        {
          is = fdis;
        }

        Class<?> c = Class.forName(context.getConfiguration().get("xml.content.handler"));
        handler = (SaxContentHandler<KEY, VALUE>) c.newInstance();
        handler.init(fdis, this.split.getStart() + this.split.getLength());

        SAXParser p = new SAXParser();
        p.setContentHandler(handler);

        synchronized (this)
        {
          notifyAll();
        }

        p.parse(new InputSource(new InputStreamReader(is, "UTF-8")));
      }
      catch (DoneSaxException done)
      {
        // ignore the exception. Simply a way out of the SAX processor.
      }
      catch (Exception e)
      {
        e.printStackTrace();
        threadException = e;
      }
      handler.done();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException
    {
      SaxContentHandler<KEY, VALUE>.Pair p = handler.getPair();
      if (threadException != null)
      {
        throw new IOException("Error in reading thread.", threadException);
      }
      if (p == null)
      {
        return false;
      }
      key = p.key;
      value = p.value;
      return true;
    }

    @Override
    public KEY getCurrentKey() throws IOException
    {
      return key;
    }

    @Override
    public VALUE getCurrentValue() throws IOException
    {
      return value;
    }

    public void setPatternString(String pattern)
    {
      patternStr = pattern;
    }
  }
}
