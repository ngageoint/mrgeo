/*
 * Copyright 2009-2016 DigitalGlobe, Inc.
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
 *
 */

package org.mrgeo.hdfs.vector.shp;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.mrgeo.hdfs.utils.HadoopFileUtils;

import java.io.IOException;


public class SeekableHdfsInput implements SeekableDataInput
{

private FSDataInputStream src;

public SeekableHdfsInput(Path p) throws IOException
{
  src = new FSDataInputStream(HadoopFileUtils.open(p));

//    FileSystem fs = HadoopFileUtils.getFileSystem(p);
//    src = fs.open(p);
}

@Override
public void close() throws IOException
{
  src.close();
}

@Override
public long getPos() throws IOException
{
  return src.getPos();
}

@Override
public boolean readBoolean() throws IOException
{
  return src.readBoolean();
}

@Override
public byte readByte() throws IOException
{
  return src.readByte();
}

@Override
public char readChar() throws IOException
{
  return src.readChar();
}

@Override
public double readDouble() throws IOException
{
  return src.readDouble();
}

@Override
public float readFloat() throws IOException
{
  return src.readFloat();
}

@Override
public void readFully(byte[] b) throws IOException
{
  src.readFully(b);
}

@Override
public void readFully(byte[] b, int off, int len) throws IOException
{
  src.readFully(b, off, len);
}

@Override
public int readInt() throws IOException
{
  return src.readInt();
}

@SuppressWarnings("deprecation")
@Override
public String readLine() throws IOException
{
  return src.readLine();
}

@Override
public long readLong() throws IOException
{
  return src.readLong();
}

@Override
public short readShort() throws IOException
{
  return src.readShort();
}

@Override
public int readUnsignedByte() throws IOException
{
  return src.readUnsignedByte();
}

@Override
public int readUnsignedShort() throws IOException
{
  return src.readUnsignedShort();
}

@Override
public String readUTF() throws IOException
{
  return src.readUTF();
}

@Override
public void seek(long pos) throws IOException
{
  src.seek(pos);
}

@Override
public int skipBytes(int n) throws IOException
{
  return src.skipBytes(n);
}
}
