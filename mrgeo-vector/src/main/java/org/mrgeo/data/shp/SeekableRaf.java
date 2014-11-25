/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.data.shp;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Wraps RandomAccessFile in a Seekable interface.
 * 
 * @author jason.surratt
 * 
 */
public class SeekableRaf implements SeekableDataInput
{

  private RandomAccessFile src;

  public SeekableRaf(RandomAccessFile src)
  {
    this.src = src;
  }

  @Override
  public void close() throws IOException
  {
    src.close();
  }

  @Override
  public long getPos() throws IOException
  {
    return src.getFilePointer();
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
