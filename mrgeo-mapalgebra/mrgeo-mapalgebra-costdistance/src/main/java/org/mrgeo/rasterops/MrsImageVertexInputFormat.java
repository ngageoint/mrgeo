package org.mrgeo.rasterops;

import java.awt.image.Raster;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexReader;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.log4j.Logger;
import org.mrgeo.image.MrsImageException;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.mapreduce.formats.TileCollection;
import org.mrgeo.rasterops.CostDistanceVertex.Position;
import org.mrgeo.rasterops.EdgeBuilder.PositionEdge;
import org.mrgeo.data.image.MrsImagePyramidInputFormat;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.data.tile.TiledInputFormatContext;
import org.mrgeo.utils.HadoopUtils;
import org.mrgeo.utils.TMSUtils;
import org.mrgeo.utils.TMSUtils.Bounds;

import com.google.common.collect.Lists;

public class MrsImageVertexInputFormat extends 
             VertexInputFormat<TileIdWritable,InMemoryRasterWritable,ByteWritable> {
  private static final Logger LOG = Logger.getLogger(MrsImageVertexInputFormat.class);

  /** Internal input format */
  
  protected InputFormat<TileIdWritable, TileCollection<Raster>> delegatedInputFormat;

  public MrsImageVertexInputFormat()
  {
    super();

    // initialize delegatedInputFormat based on backend
    delegatedInputFormat = new MrsImagePyramidInputFormat();
  }

  @Override
  public List<InputSplit> getSplits(JobContext context, int numWorkers)
      throws IOException, InterruptedException {
    return delegatedInputFormat.getSplits(context);
  }

  @Override
  public VertexReader<TileIdWritable, 
                        InMemoryRasterWritable, 
                        ByteWritable>  createVertexReader(InputSplit split, 
                                                                   TaskAttemptContext context) 
                                                                       throws IOException {
    try
    {
      return new MrsImageVertexReader(delegatedInputFormat.createRecordReader(split, context));
    }
    catch (InterruptedException e)
    {
      e.printStackTrace();
      throw new MrsImageException(e);
    }
  }

  /**
   * Vertex reader used with {@link MrsImageVertexInputFormat}.
   *
   * @param <I> Vertex id
   * @param <V> Vertex data
   * @param <E> Edge data
   * @param <M> Message data
   * @param <X> Value type
   */
  public static class MrsImageVertexReader extends VertexReader<TileIdWritable, 
                                                                    InMemoryRasterWritable, 
                                                                    ByteWritable> {
    private static final int MAX_EDGES_PER_VERTEX = 8;
    
    /** Internal record reader from {@link SequenceFileInputFormat} */
    private final RecordReader<TileIdWritable, TileCollection<Raster>> recordReader;
    /** Saved configuration */
    @SuppressWarnings("rawtypes")
    private ImmutableClassesGiraphConfiguration configuration;

    private MrsImagePyramidMetadata metadata;
    private int zoomLevel;
    
    private EdgeBuilder edgeBuilder;
    
    private Bounds userBounds;
    /**
     * Constructor with record reader.
     *
     * @param recordReader Reader from {@link SequenceFileInputFormat}.
     */
    public MrsImageVertexReader(RecordReader<TileIdWritable, TileCollection<Raster>> recordReader) {
      this.recordReader = recordReader;
    }

    @SuppressWarnings("rawtypes")
    @Override 
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) 
                   throws IOException, InterruptedException {
      this.configuration = new ImmutableClassesGiraphConfiguration(context.getConfiguration());
      recordReader.initialize(inputSplit, context);

      // get metadata from friction surface
      metadata = HadoopUtils.getSingleMetadata(configuration);

      // initialize the bounds
      TiledInputFormatContext ifContext = TiledInputFormatContext.load(context.getConfiguration());
      userBounds = Bounds.convertOldToNewBounds(ifContext.getBounds());
      assert(userBounds != null);

      zoomLevel = ifContext.getZoomLevel();
      assert(zoomLevel != -1);
      
      /* 
       * While the MrsImagePyramidInputFormat and MrsImageVertexInputFormat - the consumers 
       * of the bounds list stored in the job configuration have a bounds list in their API, the 
       * algorithms guarantee that there will only a single bounds in that list 
       * see CostDistanceBoundsConfiguration.getBounds
       */
      TMSUtils.TileBounds tb = TMSUtils.boundsToTile(userBounds, zoomLevel, metadata.getTilesize());
      edgeBuilder = new EdgeBuilder(tb.w, tb.s, tb.e, tb.n, zoomLevel);

      LOG.info(String.format("MrsImageVertexInputFormat: using zoomlevel %d and bounds %d %d %d %d",
          zoomLevel, tb.w, tb.s, tb.e, tb.n));
    }

    @Override 
    public boolean nextVertex() throws IOException, InterruptedException {      
      boolean foundTileInBounds = false, hasNextKeyValue = false;
      do {
        hasNextKeyValue = recordReader.nextKeyValue();
        if(hasNextKeyValue) {
          TileIdWritable key = recordReader.getCurrentKey();
          TileCollection<Raster> tileCollection = recordReader.getCurrentValue();
          RasterWritable value = RasterWritable.toWritable(tileCollection.get());
          foundTileInBounds = doesTileFallInBounds(key,value);
        }
      } while(!foundTileInBounds && hasNextKeyValue);
      
      return foundTileInBounds;
    }

    private boolean doesTileFallInBounds(TileIdWritable key, RasterWritable value) {
      TMSUtils.Tile tile = TMSUtils.tileid(key.get(), zoomLevel);
      Bounds tileBounds = TMSUtils.tileBounds(tile.tx, tile.ty, 
                                                metadata.getMaxZoomLevel(), metadata.getTilesize());

      return userBounds.intersect(tileBounds);
    }
    
    @Override 
    public Vertex<TileIdWritable, InMemoryRasterWritable, ByteWritable, CostDistanceMessage> 
              getCurrentVertex() throws IOException, InterruptedException {

      Vertex<TileIdWritable, InMemoryRasterWritable, ByteWritable, CostDistanceMessage>
      vertex = configuration.createVertex();

      TileIdWritable key = recordReader.getCurrentKey();
      TileCollection<Raster> tileCollection = recordReader.getCurrentValue();
      RasterWritable value = RasterWritable.toWritable(tileCollection.get());

      TileIdWritable newKey = new TileIdWritable(key);
      InMemoryRasterWritable newValue = InMemoryRasterWritable.toInMemoryRasterWritable(value);

      List<PositionEdge> neighbors = edgeBuilder.getNeighbors(key.get());
      List<Edge<TileIdWritable,ByteWritable>> edges;
      if(neighbors == null) {
        edges = Collections.EMPTY_LIST;
      }
      else {
        edges = Lists.newArrayListWithCapacity(MAX_EDGES_PER_VERTEX);
        for(PositionEdge neighbor : neighbors) {
          edges.add(EdgeFactory.create(new TileIdWritable(neighbor.targetVertexId), new ByteWritable(Position.toByte(neighbor.position))));
        }
      }

      vertex.initialize(newKey, newValue, edges);

      if(LOG.isDebugEnabled()) {
        TMSUtils.Tile tile = TMSUtils.tileid(vertex.getId().get(), zoomLevel);
        Bounds tileBounds = TMSUtils.tileBounds(tile.tx, tile.ty, 
            metadata.getMaxZoomLevel(), metadata.getTilesize());
        LOG.debug(String.format("next: Return vertexId=%d bounds=%f,%f,%f,%f, edges=%s",
            vertex.getId().get(), 
            tileBounds.w, tileBounds.s, tileBounds.e, tileBounds.n,
            vertex.getEdges()));
      }
      return vertex;      
    }


    @Override 
    public void close() throws IOException {
      recordReader.close();
    }

    @Override 
    public float getProgress() throws IOException,
    InterruptedException {
      return recordReader.getProgress();
    }
  }
}
