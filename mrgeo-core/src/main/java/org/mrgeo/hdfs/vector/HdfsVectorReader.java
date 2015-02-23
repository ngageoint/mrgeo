package org.mrgeo.hdfs.vector;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.mrgeo.data.CloseableKVIterator;
import org.mrgeo.data.vector.VectorReader;
import org.mrgeo.data.vector.VectorReaderContext;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.utils.Bounds;

public class HdfsVectorReader implements VectorReader
{
  private HdfsVectorDataProvider provider;
  private VectorReaderContext context;
  private Configuration conf;

  public class HdfsFileReader implements LineProducer
  {
    private BufferedReader reader;
    private Path sourcePath;

    public void initialize(Configuration conf, Path p) throws IOException
    {
      this.sourcePath = p;
      InputStream is = HadoopFileUtils.open(conf, p);
      reader = new BufferedReader(new InputStreamReader(is));
    }

    @Override
    public void close() throws IOException
    {
      if (reader != null)
      {
        reader.close();
      }
    }

    @Override
    public String nextLine() throws IOException
    {
      return reader.readLine();
    }
    
    public String toString()
    {
      return (sourcePath != null) ? sourcePath.toString() : "unknown" ;
    }
  }

  public class FeatureIdRangeVisitor implements DelimitedReader.DelimitedReaderVisitor
  {
    private long minFeatureId = -1;
    private long maxFeatureId = -1;

    /**
     * Constructs a FeatureIdRangeVisitor which sets lower and upper bounds
     * for acceptable features. The min/max feature id values passed in are
     * inclusive. If either of the values are <= zero,
     * it will not be checked. In other words, if the minFeatureId is <= 0, then
     * there will be no minimum boundary on acceptable feature ids. Likewise,
     * if maxFeatureId is <= 0, then there will be no upper boundary on
     * acceptable feature ids.
     * 
     * If both min and max feature id are <= 0, then this visitor will accept
     * all features.
     * 
     * @param minFeatureId
     * @param maxFeatureId
     */
    public FeatureIdRangeVisitor(long minFeatureId, long maxFeatureId)
    {
      this.minFeatureId = minFeatureId;
      this.maxFeatureId = maxFeatureId;
    }

    @Override
    public boolean accept(long id, Geometry geometry)
    {
      boolean acceptable = true;
      if (acceptable && minFeatureId > 0)
      {
        acceptable = (id >= minFeatureId);
      }
      if (acceptable && maxFeatureId > 0)
      {
        acceptable = (id <= maxFeatureId);
      }
      return acceptable;
    }

    @Override
    public boolean stopReading(long id, Geometry geometry)
    {
      return (maxFeatureId > 0 && id > maxFeatureId);
    }
  }

  public class BoundsVisitor implements DelimitedReader.DelimitedReaderVisitor
  {
    private Bounds bounds;
    
    public BoundsVisitor(Bounds bounds)
    {
      this.bounds = bounds;
    }

    @Override
    public boolean accept(long id, Geometry geometry)
    {
      Bounds geomBounds = geometry.getBounds();
      if (geomBounds != null)
      {
        return geomBounds.intersects(bounds);
      }
      return false;
    }

    @Override
    public boolean stopReading(long id, Geometry geometry)
    {
      // We must read all records since there is no ordering by geometry
      // that would allow us to stop reading early.
      return false;
    }
  }

  public HdfsVectorReader(HdfsVectorDataProvider dp,
      VectorReaderContext context,
      Configuration conf)
  {
    this.provider = dp;
    this.context = context;
    this.conf = conf;
  }

  @Override
  public void close()
  {
  }

  private DelimitedParser getDelimitedParser() throws IOException
  {
    char delimiter = ',';
    if (provider.getResourceName().toLowerCase().endsWith(".tsv"))
    {
      delimiter = '\t';
    }
    Path columnsPath = new Path(provider.getResolvedResourceName(true) + ".columns");
    List<String> attributeNames = new ArrayList<String>();
    int xCol = -1;
    int yCol = -1;
    int geometryCol = -1;
    boolean skipFirstLine = false;
    FileSystem fs = HadoopFileUtils.getFileSystem(conf, columnsPath);
    if (fs.exists(columnsPath))
    {
      InputStream in = null;
      try
      {
        in = HadoopFileUtils.open(conf, columnsPath); // fs.open(columnPath);
        ColumnDefinitionFile cdf = new ColumnDefinitionFile(in);
        skipFirstLine = cdf.isFirstLineHeader();

        int i = 0;
        for (Column col : cdf.getColumns())
        {
          String c = col.getName();

          if (col.getType() == Column.FactorType.Numeric)
          {
            if (c.equals("x"))
            {
              xCol = i;
            }
            else if (c.equals("y"))
            {
              yCol = i;
            }
          }
          else
          {
            if (c.toLowerCase().equals("geometry"))
            {
              geometryCol = i;
            }
          }

          attributeNames.add(c);
          i++;
        }
      }
      finally
      {
        if (in != null)
        {
          in.close();
        }
      }
    }
    else
    {
      throw new IOException("Column file was not found.");
    }
    DelimitedParser delimitedParser = new DelimitedParser(attributeNames,
        xCol, yCol, geometryCol, delimiter, '\"', skipFirstLine);
    return delimitedParser;
  }

  @Override
  public CloseableKVIterator<LongWritable, Geometry> get() throws IOException
  {
    HdfsFileReader fileReader = new HdfsFileReader();
    fileReader.initialize(conf, new Path(provider.getResolvedResourceName(true)));
    DelimitedParser delimitedParser = getDelimitedParser();
    DelimitedReader reader = new DelimitedReader(fileReader, delimitedParser);
    return reader;
  }

  @Override
  public boolean exists(LongWritable featureId) throws IOException
  {
    Geometry geometry = get(featureId);
    return (geometry != null);
  }

  @Override
  public Geometry get(LongWritable featureId) throws IOException
  {
    HdfsFileReader fileReader = new HdfsFileReader();
    fileReader.initialize(conf, new Path(provider.getResolvedResourceName(true)));
    DelimitedParser delimitedParser = getDelimitedParser();
    FeatureIdRangeVisitor visitor = new FeatureIdRangeVisitor(featureId.get(), featureId.get());
    DelimitedReader reader = new DelimitedReader(fileReader, delimitedParser, visitor);
    if (reader.hasNext())
    {
      Geometry geometry = reader.next();
      return geometry;
    }
    return null;
  }

  @Override
  public CloseableKVIterator<LongWritable, Geometry> get(Bounds bounds) throws IOException
  {
    HdfsFileReader fileReader = new HdfsFileReader();
    fileReader.initialize(conf, new Path(provider.getResolvedResourceName(true)));
    DelimitedParser delimitedParser = getDelimitedParser();
    BoundsVisitor visitor = new BoundsVisitor(bounds);
    DelimitedReader reader = new DelimitedReader(fileReader, delimitedParser, visitor);
    return reader;
  }
}
