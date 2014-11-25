/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.format;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.mrgeo.hdfs.utils.HadoopFileUtils;

import java.io.IOException;
import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reads specified chunks of XML from an XML document
 * 
 * This class assumes that there are no CDATA sections to the data.
 */
public class XmlInputFormat extends FileInputFormat<Text, Text> implements Serializable
{
  private static final long serialVersionUID = 1L;

  @Override
  public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException
  {
    XmlRecordReader reader = new XmlRecordReader();
    reader.initialize(split, context);
    return reader;
  }

  static public class XmlRecordReader extends RecordReader<Text, Text>
  {
    private FSDataInputStream is;
    private Pattern beginPattern, endPattern;
    private int maxSize = 400000;
    private StringBuffer buffer = new StringBuffer();
    private Text key = new Text();
    private Text value = new Text();
    private FileSplit split;
    private String patternStr = null;

    XmlRecordReader()
    {
    }

    @Override
    public void close() throws IOException
    {
      is.close();
    }

    @Override
    public float getProgress() throws IOException
    {
      long size = split.getLength();
      return (float) (is.getPos() - split.getStart()) / (float) size;
    }

    @SuppressWarnings("hiding")
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException
    {
      this.split = (FileSplit) split;
      FileSystem fs = HadoopFileUtils.getFileSystem(this.split.getPath());
      is = fs.open(this.split.getPath());
      is.seek(this.split.getStart());

      if (patternStr == null)
      {
        patternStr = context.getConfiguration().get("xml.pattern");
      }

      beginPattern = Pattern.compile("<(" + patternStr + ")(([^>]*/>)| [^>]*>|>)");
      // this is only used when the end pattern is on a separate line.
      endPattern = Pattern.compile("</(" + patternStr + ")>");
    }

    @Override
    public boolean nextKeyValue() throws IOException
    {
      int bytesRead = 1;
      while (buffer.length() < maxSize && bytesRead > 0)
      {
        byte[] buf = new byte[2048];
        bytesRead = is.read(buf, 0, 2048);
        if (bytesRead > 0)
        {
          String sbuf = new String(buf, 0, bytesRead, "UTF-8");
          buffer.append(sbuf);
        }
      }

      if (buffer.length() <= 0)
      {
        return false;
      }

      Matcher beginMatcher = beginPattern.matcher(buffer);

      // if the beginning string isn't found or it starts after the end of this split
      if (beginMatcher.find() == false
          || (beginMatcher.start() + (is.getPos() - buffer.length())) > (split.getStart() + split
              .getLength()))
      {
        return false;
      }

      String s = beginMatcher.group(1);
      key.set(s);

      String s2 = beginMatcher.group(3);

      if (s2 != null && s2.length() != 0)
      {
        String result = beginMatcher.group();
        buffer.delete(0, beginMatcher.end());
        value.set(result);
        return true;
      }

      Matcher endMatcher = endPattern.matcher(buffer.substring(beginMatcher.end()));
      if (endMatcher.find() == false)
      {
        buffer.delete(0, beginMatcher.end());
        return false;
      }

      String result = buffer.substring(beginMatcher.start(), beginMatcher.end() + endMatcher.end());
      value.set(result);
      buffer.delete(0, beginMatcher.end() + endMatcher.end());
      return true;
    }

    @Override
    public Text getCurrentKey() throws IOException
    {
      return key;
    }

    @Override
    public Text getCurrentValue() throws IOException
    {
      return value;
    }

    public void setPatternString(String pattern)
    {
      patternStr = pattern;
    }
  }
}
