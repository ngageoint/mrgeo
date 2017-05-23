package org.mrgeo.data.vector.pg;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PgInputSplit extends InputSplit implements Writable
{
  private long offset = 0;
  private long limit = 0;

  public PgInputSplit()
  {
  }

  public PgInputSplit(long offset, long limit)
  {
    this.offset = offset;
    this.limit = limit;
  }

  public long getOffset() { return offset; }
  public long getLimit() { return limit; }

  @Override
  public long getLength() throws IOException, InterruptedException
  {
    return offset - limit;
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException
  {
    return new String[0];
  }

  @Override
  public void write(DataOutput out) throws IOException
  {
    out.writeLong(offset);
    out.writeLong(limit);
  }

  @Override
  public void readFields(DataInput in) throws IOException
  {
    offset = in.readLong();
    limit = in.readLong();
  }
}
