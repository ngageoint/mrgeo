package org.mrgeo.data.geowave.vector;

import java.util.Properties;

import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.mrgeo.data.DataProviderException;
import org.mrgeo.data.vector.VectorInputFormatContext;
import org.mrgeo.data.vector.VectorInputFormatProvider;
import org.mrgeo.geometry.Geometry;

public class GeoWaveVectorInputFormatProvider extends VectorInputFormatProvider
{
  private GeoWaveConnectionInfo connectionInfo;
//  private Integer minInputSplits; // ?
//  private Integer maxInputSplits; // ?
  
  public GeoWaveVectorInputFormatProvider(VectorInputFormatContext context,
      GeoWaveConnectionInfo connectionInfo)
  {
    super(context);
    this.connectionInfo = connectionInfo;
  }

  @Override
  public InputFormat<LongWritable, Geometry> getInputFormat(String input)
  {
    return new GeoWaveVectorInputFormat();
  }

  @Override
  public void setupJob(Job job, Properties providerProperties) throws DataProviderException
  {
    super.setupJob(job, providerProperties);
    GeoWaveInputFormat.setAccumuloOperationsInfo(
        job,
        connectionInfo.getZookeeperServers(),
        connectionInfo.getInstanceName(),
        connectionInfo.getUserName(),
        connectionInfo.getPassword(),
        connectionInfo.getNamespace());
    this.connectionInfo.writeToConfig(job.getConfiguration());
//    if ((adapters != null) && (adapters.size() > 0)) {
//      for (final DataAdapter<?> adapter : adapters) {
//        GeoWaveInputFormat.addDataAdapter(
//            job,
//            adapter);
//      }
//    }
//    if ((indices != null) && (indices.size() > 0)) {
//      for (final Index index : indices) {
//        GeoWaveInputFormat.addIndex(
//            job,
//            index);
//      }
//    }
//    if (query != null) {
//      GeoWaveInputFormat.setQuery(
//          job,
//          query);
//    }
//    if (minInputSplits != null) {
//      GeoWaveInputFormat.setMinimumSplitCount(
//          job,
//          minInputSplits);
//    }
//    if (maxInputSplits != null) {
//      GeoWaveInputFormat.setMaximumSplitCount(
//          job,
//          maxInputSplits);
//    }
  }

  @Override
  public void teardown(Job job) throws DataProviderException
  {
  }
}
