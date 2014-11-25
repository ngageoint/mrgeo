package org.mrgeo.vector.mrsvector;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.vector.mrsvector.pbf.OsmFormat.DenseNodes;
import org.mrgeo.vector.mrsvector.pbf.OsmFormat.HeaderBBox;
import org.mrgeo.vector.mrsvector.pbf.OsmFormat.Node;
import org.mrgeo.vector.mrsvector.pbf.OsmFormat.Relation;
import org.mrgeo.vector.mrsvector.pbf.OsmFormat.Relation.MemberType;
import org.mrgeo.vector.mrsvector.pbf.OsmFormat.StringTable;
import org.mrgeo.vector.mrsvector.pbf.OsmFormat.Way;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.data.image.MrsImageOutputFormatProvider;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.HadoopUtils;
import org.mrgeo.utils.HadoopVectorUtils;
import org.mrgeo.utils.TMSUtils;

import com.carrotsearch.hppc.ObjectIntOpenHashMap;
import com.google.protobuf.GeneratedMessage;
import com.vividsolutions.jts.geom.TopologyException;

public class OSMTileIngester extends WritableVectorTile
{
  private static class ProcessNodesReducer extends Reducer<LongWritable, Text, TileIdWritable, Text>
  {
    int tilesize;
    int zoomlevel;

    int granularity;
    long latOffset;
    long lonOffset;

    private Writer wayWriter = null;
    private Writer relationWriter = null;

    private Counter nodeCount = null;
    private Counter orphanCount = null;
    private Counter wayCount = null;
    private Counter relationCount = null;

    @Override
    protected void cleanup(final Context context) throws IOException, InterruptedException
    {
      if (wayWriter != null)
      {
        wayWriter.close();
      }
      if (relationWriter != null)
      {
        relationWriter.close();
      }
    }

    @Override
    protected void reduce(final LongWritable key, final Iterable<Text> values, final Context context)
        throws IOException, InterruptedException
        {
      Node node = null;
      final List<Way> ways = new ArrayList<Way>();
      final List<Relation> relations = new ArrayList<Relation>();

      for (final Text text : values)
      {
        // get the bytes of the buffer without copying...
        final ByteBuffer buffer = ByteBuffer.wrap(text.getBytes(), 0, text.getLength());
        final byte type = buffer.get();
        final byte[] bytes = new byte[text.getLength() - 1];
        buffer.get(bytes);

        if (type == NODE)
        {
          final Node.Builder nb = Node.newBuilder();
          nb.mergeFrom(bytes);

          node = nb.build();

          TMSUtils.Tile t = calculateTileId(node, granularity, latOffset, lonOffset, zoomlevel, tilesize);
          final TileIdWritable tileid = new TileIdWritable(TMSUtils.tileid(t.tx, t.ty, zoomlevel));

          context.write(tileid, text);
          
          nodeCount.increment(1);
        }
        else if (type == WAY)
        {
          final Way.Builder wb = Way.newBuilder();
          wb.mergeFrom(bytes);
          ways.add(wb.build());
        }
        else if (type == RELATION)
        {
          final Relation.Builder rb = Relation.newBuilder();
          rb.mergeFrom(bytes);
          relations.add(rb.build());
        }
      }
        
//        final GeneratedMessage msg = fromBytes(getBytes(text));
//
//        if (msg instanceof Node)
//        {
//          if (node != null)
//          {
//            System.out.println("ERROR!  There are two nodes with the same id!!!");
//          }
//
//          nodeCount.increment(1);
//
//          node = (Node) msg;
//
//          TMSUtils.Tile t = calculateTileId(node, granularity, latOffset, lonOffset, zoomlevel, tilesize);
//          final TileIdWritable tileid = new TileIdWritable(TMSUtils.tileid(t.tx, t.ty, zoomlevel));
//
//          context.write(tileid, text);
//        }
//        else if (msg instanceof Way)
//        {
//          ways.add((Way) msg);
//        }
//        else if (msg instanceof Relation)
//        {
//          relations.add((Relation) msg);
//        }
//      }

      if (node != null)
      {
        for (final Way w : ways)
        {
          write(wayWriter, node, w.getId());
          wayCount.increment(1);
        }
        for (final Relation r : relations)
        {
          write(relationWriter, node, r.getId());
          relationCount.increment(1);
        }
      }
      else
      {
        // orphan node in a way/relation
        orphanCount.increment(1);
      }
        }

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException
    {
      final Configuration conf = context.getConfiguration();

      zoomlevel = conf.getInt(ZOOMLEVEL, 18);
      tilesize = conf.getInt(TILESIZE, 512);
      final Path output = new Path(conf.get(OUTPUT, ""));

      granularity = conf.getInt(GRANULATIRY, 100);
      latOffset = conf.getLong(LATOFFSET, 0);
      lonOffset = conf.getLong(LONOFFSET, 0);

      wayWriter = openWriter(conf, output, WAYS);
      relationWriter = openWriter(conf, output, RELATIONS);

      nodeCount = context.getCounter("OSM Ingest", "Valid Nodes");
      orphanCount = context.getCounter("OSM Ingest", "Orphan Nodes");
      wayCount = context.getCounter("OSM Ingest", "Ways");
      relationCount = context.getCounter("OSM Ingest", "Relations");

    }
  }

  private static class ProcessRelationsReducer extends Reducer<LongWritable, Text, TileIdWritable, Text>
  {
    int tilesize;
    int zoomlevel;

    int granularity;
    long latOffset;
    long lonOffset;

    private Writer relationWriter = null;

    private Counter nodeCount = null;
    private Counter orphanCount = null;
    private Counter wayCount = null;
    private Counter relationCount = null;
    private Counter nestedRelationCount = null;

    private String[] strings;

    @Override
    protected void cleanup(final Context context) throws IOException, InterruptedException
    {
      if (relationWriter != null)
      {
        relationWriter.close();
      }
    }

    @Override
    protected void reduce(final LongWritable key, final Iterable<Text> values, final Context context)
        throws IOException, InterruptedException
        {
      Relation relation = null;
      final List<Node> nodes = new ArrayList<Node>();
      final List<Way> ways = new ArrayList<Way>();
      final List<Relation> relations = new ArrayList<Relation>();

      Bounds bounds = null;

      System.out.println("tileid: " + key.get());

      if (key.get() == 3022887)
      {
        System.out.println("found it");
      }
      for (final Text text : values)
      {
        
        // get the bytes of the buffer without copying...
        final ByteBuffer buffer = ByteBuffer.wrap(text.getBytes(), 0, text.getLength());
        final byte type = buffer.get();
        final byte[] bytes = new byte[text.getLength() - 1];
        buffer.get(bytes);

        if (type == NODE)
        {
          final Node.Builder nb = Node.newBuilder();
          nb.mergeFrom(bytes);

          final Node node = nb.build();

          bounds = expandBounds(bounds, node, granularity, latOffset, lonOffset);

          nodes.add(node);
        }
        else if (type == WAY)
        {
          final Way.Builder wb = Way.newBuilder();
          wb.mergeFrom(bytes);
          final Way way = wb.build();

          bounds = expandBounds(way, bounds, granularity, latOffset, lonOffset);
          ways.add(way);

        }
        else if (type == RELATION)
        {
          final Relation.Builder rb = Relation.newBuilder();
          rb.mergeFrom(bytes);

          final Relation r = rb.build();
          
          dump(r, strings, System.out);
          if (r.getId() == key.get())
          {
            if (relation != null)
            {
              System.out.println("ERROR!  There are two relations with the same id!!!");
            }
            relationCount.increment(1);
            relation = r;
          }
          else
          {
            relations.add(r);
          }

        }
        else if (type == BOUNDS)
        {
          final HeaderBBox.Builder bb = HeaderBBox.newBuilder();
          bb.mergeFrom(bytes);
          
          final HeaderBBox bbox = bb.build();

          final double s = fromNanoDegrees(bbox.getBottom(), granularity, latOffset);
          final double n = fromNanoDegrees(bbox.getTop(), granularity, latOffset);
          final double w = fromNanoDegrees(bbox.getLeft(), granularity, lonOffset);
          final double e = fromNanoDegrees(bbox.getRight(), granularity, lonOffset);

          if (bounds == null)
          {
            bounds = new Bounds(w, s, e, n);
          }
          else
          {
            bounds.expand(w, s, e, n);
          }
        }
      }

      if (bounds != null)
      {
        final TMSUtils.TileBounds tb = TMSUtils.boundsToTile(TMSUtils.Bounds
          .convertOldToNewBounds(bounds), zoomlevel, tilesize);

        for (long ty = tb.s; ty <= tb.n; ty++)
        {
          for (long tx = tb.w; tx <= tb.e; tx++)
          {
            final long tileid = TMSUtils.tileid(tx, ty, zoomlevel);

            // write the way to the final tmpDir
            context.write(new TileIdWritable(tileid), new Text(toBytes(relation)));

            // If the node is outside the tileid for the relation, we need to emit it into
            // that tile as well (so we can create the geometries in its entirety)
            for (final Node node : nodes)
            {
              TMSUtils.Tile t = calculateTileId(node, granularity, latOffset, lonOffset, zoomlevel, tilesize);
              final long ntid = TMSUtils.tileid(t.tx, t.ty, zoomlevel);

              if (tileid != ntid)
              {
                nodeCount.increment(1);
                context.write(new TileIdWritable(ntid), new Text(toBytes(node)));
              }
            }

            // If the way is outside the tileid for the relation, we need to emit it into
            // that tile as well (so we can create the geometries in its entirety)
            for (final Way way : ways)
            {
              final HeaderBBox bbox = way.getBbox();

              final double s = fromNanoDegrees(bbox.getBottom(), granularity, latOffset);
              final double n = fromNanoDegrees(bbox.getTop(), granularity, latOffset);
              final double w = fromNanoDegrees(bbox.getLeft(), granularity, lonOffset);
              final double e = fromNanoDegrees(bbox.getRight(), granularity, lonOffset);

              final Bounds b = new Bounds(w, s, e, n);

              final TMSUtils.TileBounds wb = TMSUtils.boundsToTile(TMSUtils.Bounds
                .convertOldToNewBounds(b), zoomlevel, tilesize);

              for (long wy = wb.s; wy <= wb.n; wy++)
              {
                for (long wx = wb.w; wx <= wb.e; wx++)
                {
                  final long wtid = TMSUtils.tileid(wx, wy, zoomlevel);

                  if (tileid != wtid)
                  {
                    wayCount.increment(1);
                    context.write(new TileIdWritable(wtid), new Text(toBytes(way)));
                  }
                }
              }
            }
          }
        }
      }

      if (relation != null)
      {
        if (relations.size() > 0)
        {
          if (relationWriter == null)
          {
            final Configuration conf = context.getConfiguration();
            final Path output = new Path(conf.get(OUTPUT, ""));

            final int relationRun = conf.getInt(RELATION_RUN, 0);
            relationWriter = openWriter(conf, output, RELATIONS + "_" + relationRun);
          }

          for (final Relation r : relations)
          {
            long rid = r.getId();
            write(relationWriter, relation, rid);
            write(relationWriter, r, rid);

            if (bounds != null)
            {
              final HeaderBBox.Builder hb = HeaderBBox.newBuilder();

              hb.setLeft(toNanoDegrees(bounds.getMinX(), granularity, lonOffset));
              hb.setRight(toNanoDegrees(bounds.getMaxX(), granularity, lonOffset));
              hb.setBottom(toNanoDegrees(bounds.getMinY(), granularity, lonOffset));
              hb.setTop(toNanoDegrees(bounds.getMaxY(), granularity, lonOffset));

              write(relationWriter, hb.build(), rid);
            }
            nestedRelationCount.increment(1);
          }
        }
      }
      else
      {
        // orphan node in a way/relation
        orphanCount.increment(1);
      }


        }

    public static void dump(final Relation relation, final String[] strings, final PrintStream stream)
    {
      final StringBuilder builder = new StringBuilder();

      builder.append(relation.getId() + ":");

      long id = 0;
      for (int j = 0; j < relation.getMemidsCount(); j++)
      {
        final MemberType t = relation.getTypes(j);
        switch (t.ordinal())
        {
        case MemberType.NODE_VALUE:
          builder.append('n');
          break;
        case MemberType.WAY_VALUE:
          builder.append('w');
          break;
        case MemberType.RELATION_VALUE:
          builder.append('r');
          break;
        }
        id += relation.getMemids(j);
        builder.append(id);
        final int role = relation.getRolesSid(j);
        if (role != 0)
        {
          builder.append("(r:" + strings[role] + ")");
        }
        builder.append(' ');
      }

      for (int j = 0; j < relation.getKeysCount(); j++)
      {
        if (j == 0)
        {
          builder.append(" ");
        }
        builder.append(strings[relation.getKeys(j)] + "|" + strings[relation.getVals(j)] + " ");
      }
      stream.println(builder.toString());
    }

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException
    {
      final Configuration conf = context.getConfiguration();

      zoomlevel = conf.getInt(ZOOMLEVEL, 18);
      tilesize = conf.getInt(TILESIZE, 512);

      granularity = conf.getInt(GRANULATIRY, 100);
      latOffset = conf.getLong(LATOFFSET, 0);
      lonOffset = conf.getLong(LONOFFSET, 0);

      nodeCount = context.getCounter("OSM Ingest", "Nodes outside their natural tile");
      orphanCount = context.getCounter("OSM Ingest", "Orphan Relations");
      wayCount = context.getCounter("OSM Ingest", "Ways outside their natural tile(s)");
      relationCount = context.getCounter("OSM Ingest", "Relations");
      nestedRelationCount = context.getCounter("OSM Ingest", "Nested Relations");


      final Map<Long, String> stringMap = new TreeMap<Long, String>();

      final Path output = new Path(conf.get(OUTPUT, ""));
      final Path p = new Path(output, STRINGS);
      final FileSystem fs = HadoopFileUtils.getFileSystem(conf, p);

      if (fs.exists(p))
      {
        final FileStatus status = fs.getFileStatus(p);

        if (status.isDir())
        {
          final FileStatus[] files = fs.listStatus(p);
          for (final FileStatus file : files)
          {
            final SequenceFile.Reader reader = new SequenceFile.Reader(HadoopFileUtils
              .getFileSystem(conf), file.getPath(), conf);

            try
            {
              final LongWritable key = new LongWritable();
              final Text value = new Text();

              while (reader.next(key, value))
              {
                stringMap.put(key.get(), value.toString());
              }
            }
            finally
            {
              reader.close();
            }

          }
        }
      }
      strings = (new ArrayList<String>(stringMap.values())).toArray(new String[0]);
    }
  }

  private static class ProcessTilesReducer extends
  Reducer<TileIdWritable, Text, TileIdWritable, VectorTileWritable>
  {

    private Counter tileCount = null;
    private Counter blankTileCount = null;
    private Counter geometryCount = null;
    private Counter invalidGeometryCount = null;


    List<String> strings;
    int zoomlevel;
    int tilesize;

    int granularity;
    long latOffset;
    long lonOffset;

    @Override
    protected void cleanup(final Context context) throws IOException, InterruptedException
    {
      super.cleanup(context);
    }

    @Override
    protected void reduce(final TileIdWritable key, final Iterable<Text> values,
      final Context context) throws IOException, InterruptedException
      {
      final long tileid = key.get();

      final List<Node> nodes = new ArrayList<Node>();
      final List<Way> ways = new ArrayList<Way>();
      final List<Relation> relations = new ArrayList<Relation>();

      for (final Text text : values)
      {
        final GeneratedMessage msg = fromBytes(getBytes(text));

        if (msg instanceof Node)
        {
          nodes.add((Node) msg);
        }
        else if (msg instanceof Way)
        {
          ways.add((Way) msg);
        }
        else if (msg instanceof Relation)
        {
          relations.add((Relation) msg);
        }
      }

      final VectorTileCleaner rawTile = new VectorTileCleaner();

      rawTile.setGranularity(granularity);
      rawTile.setLatOffset(latOffset);
      rawTile.setLonOffset(lonOffset);

      rawTile.addStrings(strings);
      rawTile.addNodes(nodes);
      rawTile.addWays(ways);
      rawTile.addRelations(relations);

      rawTile.organize();

      final TMSUtils.Tile t = TMSUtils.tileid(tileid, zoomlevel);
      final Bounds tb = TMSUtils.tileBounds(t.tx, t.ty, zoomlevel, tilesize).asBounds();

      final WritableVectorTile tile = new WritableVectorTile();
      tile.beginWriting();

      boolean added = false;
      for (final Geometry geometry : rawTile.geometries())
      {
        final Bounds gb = geometry.getBounds();

        // is the geometry fully contained in the tile...
        if (tb.contains(gb))
        {
          added = true;
          tile.add(geometry);
          geometryCount.increment(1);
        }
        else
        {
          // clip the geometry to the bounds...
          try
          {
            final Geometry clipped = geometry.clip(tb);
            if (clipped != null)
            {
              added = true;
              tile.add(clipped);
              geometryCount.increment(1);
            }
          }
          catch (final TopologyException e)
          {
            System.out.println("ERROR clipping geometry (is it self-intersecting?): " + geometry);
            invalidGeometryCount.increment(1);
          }
          catch (final IllegalArgumentException e)
          {
            System.out
            .println("ERROR clipping geometry: " + geometry + " (" + e.getMessage() + ")");
            invalidGeometryCount.increment(1);
          }
        }
      }

      if (added)
      {
        tileCount.increment(1);
        tile.endWriting();
        context.write(key, VectorTileWritable.toWritable(tile));
      }
      else
      {
        blankTileCount.increment(1);
      }
      }

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException
    {
      super.setup(context);

      tileCount = context.getCounter("OSM Ingest", "Tiles");
      blankTileCount = context.getCounter("OSM Ingest", "Blank Tiles");
      geometryCount = context.getCounter("OSM Ingest", "Geometries");
      invalidGeometryCount = context.getCounter("OSM Ingest", "Invalid Geometries");

      final Configuration conf = context.getConfiguration();

      granularity = conf.getInt(GRANULATIRY, 100);
      latOffset = conf.getLong(LATOFFSET, 0);
      lonOffset = conf.getLong(LONOFFSET, 0);

      zoomlevel = conf.getInt(ZOOMLEVEL, 18);
      tilesize = conf.getInt(TILESIZE, 512);
      final Path output = new Path(conf.get(OUTPUT, ""));

      final Map<Long, String> stringMap = new TreeMap<Long, String>();

      final Path p = new Path(output, STRINGS);
      final FileSystem fs = HadoopFileUtils.getFileSystem(conf, p);

      if (fs.exists(p))
      {
        final FileStatus status = fs.getFileStatus(p);

        if (status.isDir())
        {
          final FileStatus[] files = fs.listStatus(p);
          for (final FileStatus file : files)
          {
            final SequenceFile.Reader reader = new SequenceFile.Reader(HadoopFileUtils
              .getFileSystem(conf), file.getPath(), conf);

            try
            {
              final LongWritable key = new LongWritable();
              final Text value = new Text();

              while (reader.next(key, value))
              {
                stringMap.put(key.get(), value.toString());
              }
            }
            finally
            {
              reader.close();
            }

          }
        }
      }
      strings = new ArrayList<String>(stringMap.values());
    }
  }

  private static class ProcessWaysReducer extends Reducer<LongWritable, Text, TileIdWritable, Text>
  {
    int tilesize;
    int zoomlevel;

    int granularity;
    long latOffset;
    long lonOffset;

    private Writer relationWriter = null;

    private Counter nodeCount = null;
    private Counter orphanCount = null;
    private Counter orphanNoBoundsCount = null;
    private Counter wayCount = null;
    private Counter relationCount = null;

    @Override
    protected void cleanup(final Context context) throws IOException, InterruptedException
    {
      if (relationWriter != null)
      {
        relationWriter.close();
      }
    }

    @Override
    protected void reduce(final LongWritable key, final Iterable<Text> values, final Context context)
        throws IOException, InterruptedException
        {
      Way way = null;
      final List<Node> nodes = new ArrayList<Node>();
      final List<Relation> relations = new ArrayList<Relation>();

      Bounds bounds = null;

      for (final Text text : values)
      {
        // get the bytes of the buffer without copying...
        final ByteBuffer buffer = ByteBuffer.wrap(text.getBytes(), 0, text.getLength());
        final byte type = buffer.get();
        final byte[] bytes = new byte[text.getLength() - 1];
        buffer.get(bytes);

        if (type == NODE)
        {
          final Node.Builder nb = Node.newBuilder();
          nb.mergeFrom(bytes);

          final Node node = nb.build();

          bounds = expandBounds(bounds, node, granularity, latOffset, lonOffset);

          nodes.add(node);
        }
        else if (type == WAY)
        {
          if (way != null)
          {
            System.out.println("ERROR!  There are two ways with the same id!!!");
          }

          wayCount.increment(1);

          final Way.Builder wb = Way.newBuilder();
          wb.mergeFrom(bytes);
          way = wb.build();

          bounds = expandBounds(way, bounds, granularity, latOffset, lonOffset);

        }
        else if (type == RELATION)
        {
          final Relation.Builder rb = Relation.newBuilder();
          rb.mergeFrom(bytes);
          relations.add(rb.build());
        }
      }

      // if bounds is null it means a way is orphaned, it had no nodes associated with it.
      if (way != null && bounds != null)
      {
        final Way.Builder wb = Way.newBuilder(way);

        final HeaderBBox.Builder bbox = HeaderBBox.newBuilder();

        bbox.setLeft(toNanoDegrees(bounds.getMinX(), granularity, lonOffset));
        bbox.setRight(toNanoDegrees(bounds.getMaxX(), granularity, lonOffset));
        bbox.setBottom(toNanoDegrees(bounds.getMinY(), granularity, latOffset));
        bbox.setTop(toNanoDegrees(bounds.getMaxY(), granularity, latOffset));

        wb.setBbox(bbox.build());
        way = wb.build();

        final TMSUtils.TileBounds tb = TMSUtils.boundsToTile(TMSUtils.Bounds
          .convertOldToNewBounds(bounds), zoomlevel, tilesize);

        for (long ty = tb.s; ty <= tb.n; ty++)
        {
          for (long tx = tb.w; tx <= tb.e; tx++)
          {
            final long tileid = TMSUtils.tileid(tx, ty, zoomlevel);

            // write the way to the final tmpDir
            context.write(new TileIdWritable(tileid), new Text(toBytes(way)));

            // If the way is outside the tileid for the node, we need to emit the node into
            // that tile as well (so we can create the geometries in its entirety)
            for (final Node node : nodes)
            {
              TMSUtils.Tile t = calculateTileId(node, granularity, latOffset, lonOffset, zoomlevel, tilesize);
              final long ntid = TMSUtils.tileid(t.tx, t.ty, zoomlevel);

              if (tileid != ntid)
              {
                nodeCount.increment(1);
                context.write(new TileIdWritable(ntid), new Text(toBytes(node)));
              }
            }
          }
        }

        for (final Relation r : relations)
        {
          write(relationWriter, way, r.getId());
          relationCount.increment(1);
        }
      }
      else
      {
        // orphan node in a way/relation
        if (way == null)
        {
          orphanCount.increment(1);
        }
        else
        {
          orphanNoBoundsCount.increment(1);
        }
      }
        }



    @Override
    protected void setup(final Context context) throws IOException, InterruptedException
    {
      final Configuration conf = context.getConfiguration();

      zoomlevel = conf.getInt(ZOOMLEVEL, 18);
      tilesize = conf.getInt(TILESIZE, 512);
      final Path output = new Path(conf.get(OUTPUT, ""));

      granularity = conf.getInt(GRANULATIRY, 100);
      latOffset = conf.getLong(LATOFFSET, 0);
      lonOffset = conf.getLong(LONOFFSET, 0);

      relationWriter = openWriter(conf, output, RELATIONS);

      nodeCount = context.getCounter("OSM Ingest", "Nodes outside their natural tile");
      orphanCount = context.getCounter("OSM Ingest", "Orphan Ways");
      orphanNoBoundsCount = context.getCounter("OSM Ingest", "Orphan Ways - No Bounds");
      wayCount = context.getCounter("OSM Ingest", "Valid Ways");
      relationCount = context.getCounter("OSM Ingest", "Relations");

    }
  }

  //  private DataInputStream inputStream;
  private Writer stringWriter;
  private Writer nodeWriter;
  private Writer wayWriter;
  private Writer relationWriter;

  private int zoomlevel;
  private Properties providerProperties;

  // this is stolen from VectorTileCleaner...
  int[] chunkstringmap = null; // maps the old stringtable key (offset) to the new stringtable
  // entry; // maps the old stringtable offset to new stringtable
  // offset

  ObjectIntOpenHashMap<String> stringmap = new ObjectIntOpenHashMap<String>(10000);
  private static final String NODES = "nodes";
  private static final String WAYS = "ways";
  private static final String RELATIONS = "relations";
  private static final String STRINGS = "strings";
  private static final String TILEIDS = "tiles";
  private static final String VECTORTILES = "mrsvector";

  private static final byte NODE = 1;
  private static final byte WAY = 2;
  private static final byte RELATION = 4;
  private static final byte BOUNDS = 8;

  static final private String base = OSMTileIngester.class.getSimpleName();
  static final public String ZOOMLEVEL = base + ".zoom";
  static final public String TILESIZE = base + ".tilesize";
  static final public String OUTPUT = base + ".output";
  static final public String GRANULATIRY = base + ".granularity";
  static final public String LATOFFSET = base + ".lat.offset";
  static final public String LONOFFSET = base + ".lon.offset";
  static final public String RELATION_RUN = base + ".relation.run";

  private Configuration config;

  private Path tmpDir;
  private Bounds datasetBounds;

  long nodeCount = 0;
  long wayCount = 0;
  long relationCount = 0;

  public OSMTileIngester()
  {
  }

  public static void main(final String[] args)
  {
    try
    {
      // String input = "/data/osm/osm-extract.pbf";
      String[] inputs = new String[] { "/data/osm/fredy.pbf" };
      //String[] inputs = new String[] { "/data/osm/rappahannock.pbf" };
      // String input = "/data/osm/sydney.osm.pbf";
      // String input = "/data/osm/central-america-latest.osm.pbf";
      // String input = "/data/osm/north-america-latest.osm.pbf";
      // String input = "/data/osm/planet-latest.osm.pbf";
      // String input = "/data/osm/dc-baltimore.osm.pbf";

      //      DataInputStream dis = new DataInputStream(new FileInputStream(new File(inputs[0])));
      //       final HeaderBlock header = readOSMHeader(dis);
      //       final VectorTile tile = new VectorTile();
      //       tile.readOSMData(dis);
      //       tile.loadKeys();
      //      
      //       File dump = new File("/data/osm/dump.txt");
      //       tile.dump(new PrintStream(dump));
      //       
      //       System.exit(1);

      final Configuration conf = HadoopUtils.createConfiguration();

      HadoopUtils.setupLocalRunner(conf);
      // final Path unique = HadoopFileUtils.createUniqueTmpPath();
      final String output = "osm-ingest";

      Properties providerProperties = null;
      ingestOsm(inputs, output, conf, 11, providerProperties);
    }
    catch (final Exception ex)
    {
      ex.printStackTrace();
    }
  }

  public static void ingestOsm(final String[] inputs, final String output,
    Configuration conf, int zoomLevel, Properties providerProperties) throws IOException
    {
    long start = System.currentTimeMillis();

    // final HeaderBlock header = readOSMHeader(dis);
    // final VectorTile tile = new VectorTile();
    // tile.readOSMData(dis);
    // tile.loadKeys();
    //
    // File dump = new File("/data/osm/dump.txt");
    // tile.dump(new PrintStream(dump));
    //
    // System.exit1);

    final Path unique = HadoopFileUtils.createUniqueTmpPath();

    HadoopFileUtils.delete(output);
    HadoopFileUtils.delete(unique);
    // HadoopFileUtils.delete(unique + "/" + TILEIDS);
    final OSMTileIngester osm = new OSMTileIngester();
    osm.ingest(inputs, conf, output, zoomLevel, unique, providerProperties);

    System.out.println("OSM ingest time: " + (System.currentTimeMillis() - start));
    }

  private static void write(final Writer writer, final Node n)
  {
    write(writer, n, n.getId());

  }

  private static void write(final Writer writer, final Relation r)
  {
    write(writer, r, r.getId());
  }

  private static void write(final Writer writer, final Way w)
  {
    write(writer, w, w.getId());
  }

  static GeneratedMessage fromBytes(final byte[] bytes)
  {
    try
    {
      final ByteBuffer buffer = ByteBuffer.wrap(bytes, 0, bytes.length);
      final byte type = buffer.get();

      final byte[] msg = new byte[bytes.length - 1];
      buffer.get(msg);

      if (type == NODE)
      {
        final Node.Builder nb = Node.newBuilder();
        nb.mergeFrom(msg);

        return nb.build();
      }
      else if (type == WAY)
      {
        final Way.Builder wb = Way.newBuilder();
        wb.mergeFrom(msg);

        return wb.build();
      }
      else if (type == RELATION)
      {
        final Relation.Builder rb = Relation.newBuilder();
        rb.mergeFrom(msg);

        return rb.build();
      }
      else if (type == BOUNDS)
      {
        final HeaderBBox.Builder hb = HeaderBBox.newBuilder();
        hb.mergeFrom(msg);

        return hb.build();
      }
    }
    catch (final IOException e)
    {
      e.printStackTrace();
    }

    return null;
  }

  static Writer openWriter(final Configuration conf, final Path path, final String subdir)
      throws IOException
      {
    return openWriter(conf, path, subdir, LongWritable.class, Text.class);
      }

  static Writer openWriter(final Configuration conf, final Path path, final String subdir,
    final Class<?> key, final Class<?> value) throws IOException
    {
    final FileSystem fs = HadoopFileUtils.getFileSystem(path);

    final String name = HadoopUtils.createRandomString(10);
    final Path stringsPath = new Path(path, subdir + "/" + name);
    return SequenceFile.createWriter(fs, conf, stringsPath, key, value,
      SequenceFile.CompressionType.RECORD);
    }

  static byte[] toBytes(final GeneratedMessage m)
  {
    final byte[] bytes = m.toByteArray();
    final ByteBuffer buffer = ByteBuffer.allocate(bytes.length + 1);

    if (m instanceof Node)
    {
      buffer.put(NODE);
    }
    else if (m instanceof Way)
    {
      buffer.put(WAY);
    }
    else if (m instanceof Relation)
    {
      buffer.put(RELATION);
    }
    else if (m instanceof HeaderBBox)
    {
      buffer.put(BOUNDS);
    }

    buffer.put(bytes);
    return buffer.array();
  }

  static void write(final Writer writer, final GeneratedMessage m, final long id)
  {
    try
    {
      writer.append(new LongWritable(id), new Text(toBytes(m)));
    }
    catch (final IOException e)
    {
      e.printStackTrace();
    }

  }

  static byte[] getBytes(final Text text)
  {
    byte[] bytes = text.getBytes();
    if (text.getLength() == bytes.length)
    {
      return bytes;
    }

    return Arrays.copyOf(bytes, text.getLength());
  }

  static Bounds expandBounds(Way way, Bounds bounds, long granularity, long latOffset, long lonOffset)
  {
    Bounds b =  extractBounds(way, granularity, latOffset, lonOffset);

    if (bounds == null)
    {
      return b;
    }

    if (b != null)
    {
      bounds.expand(b);
    }
    
    return bounds;
  }

  static Bounds extractBounds(Way way, long granularity, long latOffset, long lonOffset)
  {
    if (way.hasBbox())
    {
      final HeaderBBox bbox = way.getBbox();

      final double s = fromNanoDegrees(bbox.getBottom(), granularity, latOffset);
      final double n = fromNanoDegrees(bbox.getTop(), granularity, latOffset);
      final double w = fromNanoDegrees(bbox.getLeft(), granularity, lonOffset);
      final double e = fromNanoDegrees(bbox.getRight(), granularity, lonOffset);

      return new Bounds(w, s, e, n);
    }

    return null;
  }
  static Bounds extractBounds(Node node, long granularity, long latOffset, long lonOffset)
  {
    final double lat = fromNanoDegrees(node.getLat(), granularity, latOffset);
    final double lon = fromNanoDegrees(node.getLon(), granularity, lonOffset);

    return new Bounds(lon, lat, lon, lat);
  }
  
  static Bounds expandBounds(Bounds bounds, final Node node, long granularity, long latOffset, long lonOffset)
  {
    Bounds b =  extractBounds(node, granularity, latOffset, lonOffset);

    if (bounds == null)
    {
      return b;
    }

    bounds.expand(b);
    return bounds;
  }

  static TMSUtils.Tile calculateTileId(Node node, long granularity, long latOffset, long lonOffset, int zoom, int tilesize)
  {
    final double lat = fromNanoDegrees(node.getLat(), granularity, latOffset);
    final double lon = fromNanoDegrees(node.getLon(), granularity, lonOffset);

    return TMSUtils.latLonToTile(lat, lon, zoom, tilesize);
  }

  public void ingest(final String[] inputs, final Configuration conf, final String output,
    final int zoomlevel, final Path tmpDir,
    final Properties providerProperties) throws IOException
    {
    this.config = conf;
    this.providerProperties = providerProperties;
    this.tmpDir = tmpDir;
    this.zoomlevel = zoomlevel;

    for (String input : inputs)
    {
      FileInputStream fis = null;
      DataInputStream dis;
      try
      {
        fis = new FileInputStream(input);
        dis = new DataInputStream(fis);

        ingestRawOSM(dis);
      }
      finally
      {
        if (fis != null)
        {
          fis.close();
        }
      }
    }


    processNodes();
    processWays();
    processRelations();

    if (buildTiles())
    {
      //HadoopFileUtils.create(output);

      HadoopFileUtils.move(conf, new Path(tmpDir, VECTORTILES), new Path(output));
      HadoopFileUtils.cleanDirectory(output);
      HadoopFileUtils.delete(tmpDir);
    }
    }

  @Override
  protected void parseDenseNodes(final DenseNodes dense)
  {
    final int count = dense.getIdCount();

    final Node.Builder nb = Node.newBuilder();

    // kv offset is the running offset within the key/value list for the nodes.
    // in dense nodes, the k/v are stored as consecutive values (k, v, k, v, ...) with a "0" key
    // value denoting the end of the k/v for a dense nodemap.
    int kvoffset = 0;

    long lon = 0;
    long lat = 0;

    long id = 0;

    for (int i = 0; i < count; i++)
    {
      // these are DELTA coded, so we need to keep a running count...
      id += dense.getId(i);
      lon += dense.getLon(i);
      lat += dense.getLat(i);
      
      nb.clear();

      nb.setId(id);
      nb.setLat(lat);
      nb.setLon(lon);

      if (dense.getKeysValsCount() > 0)
      {
        int k = dense.getKeysVals(kvoffset++);
        while (k != 0)
        {
          final int v = dense.getKeysVals(kvoffset++);

          nb.addKeys(chunkstringmap[k]);
          nb.addVals(chunkstringmap[v]);

          k = dense.getKeysVals(kvoffset++);
        }
      }
      nb.setUnknownFields(dense.getUnknownFields());

      final Node node = nb.build();
      datasetBounds = expandBounds(datasetBounds, node, granularity, latOffset, lonOffset);
      
      nodeCount++;
      write(nodeWriter, node);
    }
  }

  @Override
  protected void parseNodes(final List<Node> nodesList)
  {
    final Node.Builder nb = Node.newBuilder();

    for (final Node node : nodesList)
    {
      nb.clear();
      nb.mergeFrom(node);

      nb.clearKeys();
      nb.clearVals();
      for (final int key : node.getKeysList())
      {
        nb.addKeys(chunkstringmap[key]);
      }
      for (final int val : node.getValsList())
      {
        nb.addVals(chunkstringmap[val]);
      }

      final Node n = nb.build();
      datasetBounds = expandBounds(datasetBounds, n, granularity, latOffset, lonOffset);

      nodeCount++;
      write(nodeWriter, n);
    }
  }

  @Override
  protected void parseRelations(final List<Relation> relationsList)
  {
    Relation.Builder rb = Relation.newBuilder();
    for (final Relation r : relationsList)
    {
      //      dump(r, System.out);
      rb.clear();
      rb.mergeFrom(r);

      rb.clearKeys();
      rb.clearVals();

      for (final int key : r.getKeysList())
      {
        rb.addKeys(chunkstringmap[key]);
      }
      for (final int val : r.getValsList())
      {
        rb.addVals(chunkstringmap[val]);
      }

      rb.clearRolesSid();
      for (final int role: r.getRolesSidList())
      {
        rb.addRolesSid(chunkstringmap[role]);
      }

      final Relation relation = rb.build();

      relationCount++;
      write(relationWriter, relation);


      long id = 0;
      for (int k = 0; k < relation.getMemidsCount(); k++)
      {
        id += relation.getMemids(k); // DELTA encoding

        if (relation.getTypes(k) == Relation.MemberType.NODE)
        {
          write(nodeWriter, relation, id);
        }
        else if (relation.getTypes(k) == Relation.MemberType.WAY)
        {
          write(wayWriter, relation, id);
        }
        else if (relation.getTypes(k) == Relation.MemberType.RELATION)
        {
          write(relationWriter, relation, id);
        }
      }

    }
  }

  @Override
  protected void parseStrings(final StringTable stringtable)
  {
    chunkstringmap = new int[stringtable.getSCount()];

    int offset = stringmap.size();
    for (int i = 0; i < stringtable.getSCount(); i++)
    {
      final String str = stringtable.getS(i).toStringUtf8();
      if (!stringmap.containsKey(str))
      {
        stringmap.put(str, offset);
        chunkstringmap[i] = offset;

        try
        {
          stringWriter.append(new LongWritable(offset), new Text(str));
        }
        catch (final IOException e)
        {
          e.printStackTrace();
        }
        offset++;
      }
      else
      {
        chunkstringmap[i] = stringmap.get(str);
      }
    }
  }

  @Override
  protected void parseWays(final List<Way> waysList)
  {

    final Way.Builder wb = Way.newBuilder();
    for (final Way way : waysList)
    {
      wb.clear();
      wb.mergeFrom(way);

      wb.clearKeys();
      wb.clearVals();

      for (final int key : way.getKeysList())
      {
        wb.addKeys(chunkstringmap[key]);
      }
      for (final int val : way.getValsList())
      {
        wb.addVals(chunkstringmap[val]);
      }

      final Way w = wb.build();

      wayCount++;
      write(wayWriter, w);

      long id = 0;

      for (int i = 0; i < way.getRefsCount(); i++)
      {
        id += way.getRefs(i); // DELTA encoding

        write(nodeWriter, w, id);
      }

    }
  }

  private boolean buildTiles()
  {
    try
    {
      final Job job = new Job(config);
      HadoopUtils.setJar(job, this.getClass());

      final String now = new SimpleDateFormat("yyyy-MM-dd'T'HH-mm-ss").format(new Date());

      final String jobName = "BuildTiles_" + now + "_" + UUID.randomUUID().toString();
      job.setJobName(jobName);

      final Configuration conf = job.getConfiguration();

      conf.setInt(ZOOMLEVEL, zoomlevel);
      final int tilesize = Integer.parseInt(MrGeoProperties.getInstance().getProperty(
        "mrsimage.tilesize", "512"));
      conf.setInt(TILESIZE, tilesize);
      conf.set(OUTPUT, tmpDir.toString());

      conf.setInt(GRANULATIRY, granularity);
      conf.setLong(LATOFFSET, latOffset);
      conf.setLong(LONOFFSET, lonOffset);

      job.setInputFormatClass(SequenceFileInputFormat.class);

      final Path tilesPath = new Path(tmpDir, TILEIDS + "/*/part*");
      HadoopVectorUtils.addInputPath(job, tilesPath);

      job.setReducerClass(ProcessTilesReducer.class);

      final Path output = new Path(tmpDir, VECTORTILES);

      HadoopFileUtils.delete(output);

      MrsImageOutputFormatProvider ofProvider = MrsImageDataProvider.setupMrsPyramidOutputFormat(
          job, output.toString(), datasetBounds, zoomlevel, tilesize, providerProperties);
      //FileOutputFormat.setOutputPath(job, outputWithZoom);

      job.setMapOutputKeyClass(TileIdWritable.class);
      job.setMapOutputValueClass(Text.class);

      job.setOutputKeyClass(TileIdWritable.class);
      job.setOutputValueClass(VectorTileWritable.class);

      try
      {
        job.submit();
        final boolean success = job.waitForCompletion(true);

        if (success)
        {
          ofProvider.teardown(job);
          MrsVectorPyramid.calculateMetadata(output.toString(), zoomlevel, tilesize, datasetBounds);
          return true;
        }

      }
      catch (final InterruptedException e)
      {
        e.printStackTrace();
      }
      catch (final ClassNotFoundException e)
      {
        e.printStackTrace();
      }
    }
    catch (final IOException e)
    {
      e.printStackTrace();
    }
    return false;

  }

  private void ingestRawOSM(final DataInputStream dis) throws IOException
  {
    //    inputStream = dis;

    // set the packet size way up...
    config.set("dfs.client.write-packet-size", "786500");

    stringWriter = openWriter(config, tmpDir, STRINGS, LongWritable.class, Text.class);
    nodeWriter = openWriter(config, tmpDir, NODES);
    wayWriter = openWriter(config, tmpDir, WAYS);
    relationWriter = openWriter(config, tmpDir, RELATIONS);

    datasetBounds = null;
    readOSMHeader(dis);
    readOSMData(dis);

    if (stringWriter != null)
    {
      stringWriter.close();
    }
    if (nodeWriter != null)
    {
      nodeWriter.close();
    }
    if (wayWriter != null)
    {
      wayWriter.close();
    }
    if (relationWriter != null)
    {
      relationWriter.close();
    }

    System.out.println("nodes read: " + nodeCount);
    System.out.println("ways read: " + wayCount);
    System.out.println("relations read: " + relationCount);
  }

  private boolean processNodes()
  {
    try
    {
      final Job job = new Job(config);
      HadoopUtils.setJar(job, this.getClass());

      final String now = new SimpleDateFormat("yyyy-MM-dd'T'HH-mm-ss").format(new Date());

      final String jobName = "ProcesNodes_" + now + "_" + UUID.randomUUID().toString();
      job.setJobName(jobName);

      final Configuration conf = job.getConfiguration();

      conf.setInt(ZOOMLEVEL, zoomlevel);
      conf.setInt(TILESIZE, Integer.parseInt(MrGeoProperties.getInstance().getProperty(
        "mrsimage.tilesize", "512")));
      conf.set(OUTPUT, tmpDir.toString());

      conf.setInt(GRANULATIRY, granularity);
      conf.setLong(LATOFFSET, latOffset);
      conf.setLong(LONOFFSET, lonOffset);

      job.setInputFormatClass(SequenceFileInputFormat.class);

      final Path nodesPath = new Path(tmpDir, NODES);
      HadoopVectorUtils.addInputPath(job, nodesPath);

      job.setReducerClass(ProcessNodesReducer.class);

      job.setOutputFormatClass(SequenceFileOutputFormat.class);

      final Path output = new Path(tmpDir, TILEIDS + "/" + NODES);
      HadoopFileUtils.delete(output);
      FileOutputFormat.setOutputPath(job, output);

      job.setMapOutputKeyClass(LongWritable.class);
      job.setMapOutputValueClass(Text.class);

      job.setOutputKeyClass(TileIdWritable.class);
      job.setOutputValueClass(Text.class);

      try
      {
        job.submit();
        final boolean success = job.waitForCompletion(true);

        if (success)
        {
          return true;
        }

      }
      catch (final InterruptedException e)
      {
        e.printStackTrace();
      }
      catch (final ClassNotFoundException e)
      {
        e.printStackTrace();
      }
    }
    catch (final IOException e)
    {
      e.printStackTrace();
    }
    return false;
  }

  private boolean processRelations()
  {
    try
    {
      int runCnt = 1;

      while (true)
      {
        final Job job = new Job(config);
        HadoopUtils.setJar(job, this.getClass());

        final String now = new SimpleDateFormat("yyyy-MM-dd'T'HH-mm-ss").format(new Date());

        final String jobName = "ProcesRelations_" + runCnt + "_" + now + "_" +
            UUID.randomUUID().toString();
        job.setJobName(jobName);

        final Configuration conf = job.getConfiguration();

        conf.setInt(ZOOMLEVEL, zoomlevel);
        conf.setInt(TILESIZE, Integer.parseInt(MrGeoProperties.getInstance().getProperty(
          "mrsimage.tilesize", "512")));
        conf.set(OUTPUT, tmpDir.toString());

        conf.setInt(GRANULATIRY, granularity);
        conf.setLong(LATOFFSET, latOffset);
        conf.setLong(LONOFFSET, lonOffset);
        conf.setInt(RELATION_RUN, runCnt);

        job.setInputFormatClass(SequenceFileInputFormat.class);

        final Path relationsPath;
        if (runCnt <= 1)
        {
          relationsPath = new Path(tmpDir, RELATIONS);
        }
        else
        {
          relationsPath = new Path(tmpDir, RELATIONS + "_" + (runCnt - 1));
        }
        HadoopVectorUtils.addInputPath(job, relationsPath);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        final Path output = new Path(tmpDir, TILEIDS + "/" + RELATIONS + "_" + runCnt);
        HadoopFileUtils.delete(output);
        FileOutputFormat.setOutputPath(job, output);

        job.setReducerClass(ProcessRelationsReducer.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(TileIdWritable.class);
        job.setOutputValueClass(Text.class);


        boolean success = false;
        try
        {
          job.submit();
          success = job.waitForCompletion(true);
        }
        catch (final InterruptedException e)
        {
          e.printStackTrace();
        }
        catch (final ClassNotFoundException e)
        {
          e.printStackTrace();
        }

        if (success)
        {

          final Path rp = new Path(tmpDir, RELATIONS + "_" + runCnt);

          // did we make a relations file?
          if (!HadoopFileUtils.exists(rp))
          {
            return true;
          }
        }
        runCnt++;

                if (runCnt > 5)
                {
                  return true;
                }
      }
    }
    catch (final IOException e)
    {
      e.printStackTrace();
    }
    return false;
  }

  private boolean processWays()
  {
    try
    {
      final Job job = new Job(config);
      HadoopUtils.setJar(job, this.getClass());

      final String now = new SimpleDateFormat("yyyy-MM-dd'T'HH-mm-ss").format(new Date());

      final String jobName = "ProcesWays_" + now + "_" + UUID.randomUUID().toString();
      job.setJobName(jobName);

      final Configuration conf = job.getConfiguration();

      conf.setInt(ZOOMLEVEL, zoomlevel);
      conf.setInt(TILESIZE, Integer.parseInt(MrGeoProperties.getInstance().getProperty(
        "mrsimage.tilesize", "512")));
      conf.set(OUTPUT, tmpDir.toString());

      conf.setInt(GRANULATIRY, granularity);
      conf.setLong(LATOFFSET, latOffset);
      conf.setLong(LONOFFSET, lonOffset);

      job.setInputFormatClass(SequenceFileInputFormat.class);

      final Path waysPath = new Path(tmpDir, WAYS);
      HadoopVectorUtils.addInputPath(job, waysPath);

      job.setReducerClass(ProcessWaysReducer.class);

      job.setOutputFormatClass(SequenceFileOutputFormat.class);

      final Path output = new Path(tmpDir, TILEIDS + "/" + WAYS);
      HadoopFileUtils.delete(output);
      FileOutputFormat.setOutputPath(job, output);

      job.setMapOutputKeyClass(LongWritable.class);
      job.setMapOutputValueClass(Text.class);

      job.setOutputKeyClass(TileIdWritable.class);
      job.setOutputValueClass(Text.class);

      try
      {
        job.submit();
        final boolean success = job.waitForCompletion(true);

        if (success)
        {
          return true;
        }
      }
      catch (final InterruptedException e)
      {
        e.printStackTrace();
      }
      catch (final ClassNotFoundException e)
      {
        e.printStackTrace();
      }
    }
    catch (final IOException e)
    {
      e.printStackTrace();
    }
    return false;
  }

}
