package org.mrgeo.mapreduce.readers;

import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;
import org.mrgeo.vector.mrsvector.VectorTileWritable;
import org.mrgeo.data.tile.TileIdWritable;

public class HdfsMrsVectorPyramidRecordReader extends SequenceFileRecordReader<TileIdWritable,VectorTileWritable>
{
}
