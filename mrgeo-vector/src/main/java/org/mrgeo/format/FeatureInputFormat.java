package org.mrgeo.format;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.mrgeo.geometry.Geometry;

import java.io.Serializable;

public abstract class FeatureInputFormat extends InputFormat<LongWritable, Geometry> implements
    Serializable
{
  private static final long serialVersionUID = 1L;
}
