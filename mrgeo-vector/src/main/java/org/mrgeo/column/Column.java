/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.column;

import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Column implements Writable
{
  // be sure and look at GuessColumnTypesReducer#reduce before adding or
  // changing this enumeration.
  public enum FactorType {
    Ignored, Nominal, Numeric, Unknown
  }

  @SuppressWarnings("unused")
  private static final Logger log = LoggerFactory.getLogger(Column.class);

  private long count = 0;

  private double max = -Double.MAX_VALUE;

  private double min = Double.MAX_VALUE;

  private String name = null;

  private double sum = 0.0;
  
  private double quartile1 = Double.MAX_VALUE;
  
  private double quartile2 = Double.MAX_VALUE;
  
  private double quartile3 = Double.MAX_VALUE;

  private FactorType type = FactorType.Unknown;

  public Column()
  {

  }

  public Column(String name, double min, double max)
  {
    this.name = name;
    this.min = min;
    this.max = max;
    this.type = FactorType.Numeric;
  }

  public Column(String name, FactorType type)
  {
    this.name = name;
    this.type = type;
  }

  public void addValue(String value)
  {
    if (value != null && !value.isEmpty())
    {
      if (type == FactorType.Unknown)
      {
        try
        {
          Double.parseDouble(value);
          setType(FactorType.Numeric);
        }
        catch (NumberFormatException e)
        {
          setType(FactorType.Nominal);
        }
      }
      switch (type)
      {
      case Numeric:
        try
        {
          double dv = Double.parseDouble(value);
          min = Math.min(dv, min);
          max = Math.max(dv, max);
          sum += dv;
          count++;
        }
        catch (NumberFormatException e)
        {
          // this shouldn't happen if it does we just treat the invalid number
          // as a null.
        }
        break;
      case Nominal:
        count++;
        break;
      case Ignored:
        break;
      case Unknown:
        break;
      default:
        break;
      }
    }
  }

  public void combine(Column other)
  {
    count += other.count;
    name = name == null ? other.name : name;
    if (type == FactorType.Unknown)
    {
      type = other.type;
      min = other.min;
      max = other.max;
      sum = other.sum;
      count = other.count;
    }
    else if (type == FactorType.Numeric && other.type == FactorType.Numeric)
    {
      min = Math.min(min, other.min);
      max = Math.max(max, other.max);
      sum += other.sum;
    }
    else if (type == FactorType.Nominal || other.type == FactorType.Nominal)
    {
      type = FactorType.Nominal;
    }
  }

  public long getCount()
  {
    return count;
  }

  public double getMax()
  {
    return max;
  }

  public double getMin()
  {
    return min;
  }

  public double getQuartile1()
  {
    return quartile1;
  }

  public double getQuartile2()
  {
    return quartile2;
  }

  public double getQuartile3()
  {
    return quartile3;
  }

  public String getName()
  {
    return name;
  }

  public double getSum()
  {
    return sum;
  }

  public FactorType getType()
  {
    return type;
  }
  
  public boolean isMaxValid()
  {
    return max != -Double.MAX_VALUE;
  }

  public boolean isMinValid()
  {
    return min != Double.MAX_VALUE;
  }
  
  public boolean isQuartile1Valid()
  {
    return quartile1 != Double.MAX_VALUE;
  }

  public boolean isQuartile2Valid()
  {
    return quartile2 != Double.MAX_VALUE;
  }

  public boolean isQuartile3Valid()
  {
    return quartile3 != Double.MAX_VALUE;
  }

  @Override
  public void readFields(DataInput in) throws IOException
  {
    name = in.readUTF();
    int typeIndex = in.readInt();
    type = FactorType.values()[typeIndex];
    min = in.readDouble();
    max = in.readDouble();
    quartile1 = in.readDouble();
    quartile2 = in.readDouble();
    quartile3 = in.readDouble();
    sum = in.readDouble();
    count = in.readLong();
    System.out.printf(toString());
  }

  public void setCount(long count)
  {
    this.count = count;
  }

  public void setMax(double max)
  {
    this.max = max;
  }

  public void setMin(double min)
  {
    this.min = min;
  }

  public void setQuartile1(double q1)
  {
    quartile1 = q1;
  }

  public void setQuartile2(double q2)
  {
    quartile2 = q2;
  }

  public void setQuartile3(double q3)
  {
    quartile3 = q3;
  }

  public void setName(String name)
  {
    this.name = name;
  }

  public void setSum(double sum)
  {
    this.sum = sum;
  }

  public void setType(FactorType type)
  {
    this.type = type;
  }

  @Override
  public String toString()
  {
    StringBuffer result = new StringBuffer();
    result.append("Name: " + name + "\n");
    result.append("Type: " + type.toString() + "\n");
    result.append(String.format("  count: %d\n", count));
    if (type == FactorType.Numeric)
    {
      result.append(String.format("  min: %f\n", min));
      result.append(String.format("  max: %f\n", max));
      result.append(String.format("  sum: %f\n", sum));
      if (isQuartile1Valid())
      {
        result.append(String.format("  quartile1: %f\n", quartile1));
      }
      else
      {
        result.append("  quartile1: not available\n");
      }
      if (isQuartile2Valid())
      {
        result.append(String.format("  quartile2: %f\n", quartile2));
      }
      else
      {
        result.append("  quartile2: not available\n");
      }
      if (isQuartile3Valid())
      {
        result.append(String.format("  quartile3: %f\n", quartile3));
      }
      else
      {
        result.append("  quartile3: not available\n");
      }
    }
    return result.toString();
  }

  @Override
  public void write(DataOutput out) throws IOException
  {
    out.writeUTF(name == null ? "" : name);
    out.writeInt(type.ordinal());
    out.writeDouble(min);
    out.writeDouble(max);
    out.writeDouble(quartile1);
    out.writeDouble(quartile2);
    out.writeDouble(quartile3);
    out.writeDouble(sum);
    out.writeLong(count);
  }
}
