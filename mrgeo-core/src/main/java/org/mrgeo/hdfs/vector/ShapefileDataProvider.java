package org.mrgeo.hdfs.vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.vector.*;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class ShapefileDataProvider extends VectorDataProvider
{
  private static Logger log = LoggerFactory.getLogger(ShapefileDataProvider.class);

  private Configuration conf;
  private Path resourcePath;
  private ProviderProperties providerProperties;

  public ShapefileDataProvider(final Configuration conf,
                               final String prefix, final String resourceName,
                               final ProviderProperties providerProperties)
  {
    super(prefix, resourceName);
    this.conf = conf;
    this.providerProperties = providerProperties;
  }

  public String getResolvedResourceName(final boolean mustExist) throws IOException
  {
    return resolveNameToPath(conf, getResourceName(), providerProperties, mustExist).toUri().toString();
    //return new Path(getImageBasePath(), getResourceName()).toUri().toString();
  }

  public Path getResourcePath(boolean mustExist) throws IOException
  {
    if (resourcePath == null)
    {
      resourcePath = resolveName(getConfiguration(), getResourceName(),
                                 providerProperties, mustExist);
    }
    return resourcePath;
  }

  @Override
  public VectorMetadataReader getMetadataReader()
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public VectorMetadataWriter getMetadataWriter()
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public VectorReader getVectorReader() throws IOException
  {
//    return new ShapefileVectorReader(this, new VectorReaderContext(), conf);
    return null;
  }

  @Override
  public VectorReader getVectorReader(VectorReaderContext context) throws IOException
  {
//    return new ShapefileVectorReader(this, context, conf);
    return null;
  }

  @Override
  public VectorWriter getVectorWriter()
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public VectorWriter getVectorWriter(VectorWriterContext context)
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public RecordReader<LongWritable, Geometry> getRecordReader()
  {
//    return new ShpInputFormat.ShapefileRecordReader();
    return null;
  }

  @Override
  public RecordWriter<LongWritable, Geometry> getRecordWriter()
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public VectorInputFormatProvider getVectorInputFormatProvider(VectorInputFormatContext context)
  {
//    return new ShapefileInputFormatProvider(context);
    return null;
  }

  @Override
  public VectorOutputFormatProvider getVectorOutputFormatProvider(VectorOutputFormatContext context)
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void delete() throws IOException
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void move(String toResource) throws IOException
  {
    // TODO Auto-generated method stub

  }

  public static boolean canOpen(final Configuration conf, String input,
                                final ProviderProperties providerProperties) throws IOException
  {
    Path p;
    try
    {
      p = resolveName(conf, input, providerProperties, false);
      String lowerPath = p.toString().toLowerCase();
      if (p != null && lowerPath.endsWith(".tsv") || lowerPath.endsWith(".csv"))
      {
        return hasMetadata(conf, p);
      }
    }
    catch (IOException e)
    {
    }
    return false;
  }

  public static boolean exists(final Configuration conf, String input,
                               final ProviderProperties providerProperties) throws IOException
  {
    return resolveNameToPath(conf, input, providerProperties, true) != null;
  }

  public static void delete(final Configuration conf, String input,
                            final ProviderProperties providerProperties) throws IOException
  {
    Path p = resolveNameToPath(conf, input, providerProperties, false);
    Path columns = new Path(p.toString() + ".columns");
    // In case the resource was moved since it was created
    if (p != null)
    {
      HadoopFileUtils.delete(conf, p);
    }
    if (columns != null)
    {
      HadoopFileUtils.delete(conf, columns);
    }
  }

  public static boolean canWrite(final Configuration conf, String input,
                                 final ProviderProperties providerProperties) throws IOException
  {
    // The return value of resolveNameToPath will be null if the input
    // path does not exist. It wil throw an exception if there is a problem
    // with building the path.
    Path p = resolveNameToPath(conf, input, providerProperties, true);
    return (p == null);
  }

  private static Path resolveName(final Configuration conf, final String input,
                                  final ProviderProperties providerProperties, final boolean mustExist) throws IOException
  {
    Path result = resolveNameToPath(conf, input, providerProperties, mustExist);
    if (result != null && hasMetadata(conf, result))
    {
      return result;
    }

    throw new IOException("Invalid image: " + input);
  }

  private static Path resolveNameToPath(final Configuration conf, final String input,
                                        final ProviderProperties providerProperties, final boolean mustExist) throws IOException
  {
    if (input.indexOf('/') >= 0)
    {
      // The requested input is absolute. If it exists in the local file
      // system, then return that path. Otherwise, check HDFS, and if it
      // doesn't exist there either, but mustExist is false, return the
      // HDFS path (this resource is being created). We never create resources
      // in the local file system.
      File f = new File(input);
      if (f.exists())
      {
        try
        {
          return new Path(new URI("file://" + input));
        }
        catch (URISyntaxException e)
        {
          // The URI is invalid, so let's continue to try to open it in HDFS
        }
      }
      Path p = null;
      try
      {
        p = new Path(input);
      }
      catch(IllegalArgumentException e)
      {
        // If the path is a bad URI (which can happen if this is not an HDFS resource),
        // then an IllegalArgumentException is thrown.
        return null;
      }
      if (mustExist)
      {
        FileSystem fs = HadoopFileUtils.getFileSystem(conf, p);
        if (fs.exists(p))
        {
          return p;
        }
      }
      else
      {
        return p;
      }
      return null;
    }
    else
    {
      // The input is relative, check it using the image base path.
      Path p = null;
      try
      {
        p = new Path(getBasePath(conf), input);
      }
      catch(IllegalArgumentException e)
      {
        // If the path is a bad URI (which can happen if this is not an HDFS resource),
        // then an IllegalArgumentException is thrown.
        return null;
      }
      if (mustExist)
      {
        FileSystem fs = HadoopFileUtils.getFileSystem(conf, p);
        if (fs.exists(p))
        {
          return p;
        }
      }
      else
      {
        return p;
      }
      return null;
    }
  }

  private static boolean hasMetadata(final Configuration conf, final Path p)
  {
    FileSystem fs;
    try
    {
      fs = HadoopFileUtils.getFileSystem(conf, p);
      if (fs.exists(p))
      {
        String columnsPath = p.toString() + ".columns";
        return (fs.exists(new Path(columnsPath)));
      }
    }
    catch (IOException e)
    {
    }
    return false;
  }

  static Path getBasePath(final Configuration conf)
  {
    String basePathKey = "hdfs." + MrGeoConstants.MRGEO_HDFS_VECTOR;
    Path basePath = null;
    String strBasePath = null;
    // First check to see if the vector base path is defined in the configuration.
    // This happens when it's being called from within code that runs on a
    // Hadoop data node.
    // If it's not there, then check for the base path definition in the mrgeo conf,
    // which will only work when it's called from the name node.
    if (conf != null)
    {
      strBasePath = conf.get(basePathKey);
    }
    if (strBasePath == null)
    {
      strBasePath = MrGeoProperties.getInstance().getProperty(MrGeoConstants.MRGEO_HDFS_VECTOR);
    }
    if (strBasePath != null)
    {
      try
      {
        basePath = new Path(new URI(strBasePath));
      }
      catch (URISyntaxException e)
      {
        log.error("Invalid HDFS base path for vectors: " + strBasePath, e);
      }
    }
    // Use default if we couldn't get the base path
    if (basePath == null)
    {
      basePath = new Path("/mrgeo/vectors");
    }
    return basePath;
  }

  private Configuration getConfiguration()
  {
    return conf;
  }
}
