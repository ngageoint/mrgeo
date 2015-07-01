/*
 * Copyright 2009-2015 DigitalGlobe, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.mrgeo.vector.mrsvector;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Message;
import org.apache.commons.lang.ArrayUtils;
import org.mrgeo.geometry.*;
import org.mrgeo.vector.mrsvector.pbf.FileFormat.Blob;
import org.mrgeo.vector.mrsvector.pbf.FileFormat.BlobHeader;
import org.mrgeo.vector.mrsvector.pbf.OsmFormat.*;
import org.mrgeo.vector.mrsvector.pbf.OsmFormat.Relation.MemberType;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.LongRectangle;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.zip.InflaterInputStream;

public class VectorTile
{
  public static class MrsVectorException extends RuntimeException
  {
    private static final long serialVersionUID = 1L;
    private final Exception origException;

    public MrsVectorException(final Exception e)
    {
      this.origException = e;
      printStackTrace();
    }

    public MrsVectorException(final String msg)
    {
      final Exception e = new Exception(msg);
      this.origException = e;
    }

    @Override
    public void printStackTrace()
    {
      origException.printStackTrace();
    }
  }

  private class GeometryIterable implements Iterable<Geometry>
  {

    public GeometryIterable()
    {
    }

    @Override
    public Iterator<Geometry> iterator()
    {
      return new GeometryIterator();
    }
  }

  private class GeometryIterator implements Iterator<Geometry>
  {

    int currentnode = 0;
    int currentway = Integer.MAX_VALUE;
    int currentrelation = Integer.MAX_VALUE;

    public GeometryIterator()
    {
    }

    @Override
    public boolean hasNext()
    {
      // MAX_VALUE is used here as a shortcut for testing we've looped through all the elements
      if (currentnode < Integer.MAX_VALUE && nodes != null)
      {
        while (currentnode < nodes.length)
        {
          if (hasKeyValue(nodes[currentnode++], singlePointKey, yesKey))
          {
            return true;
          }
        }
        // finished nodes, now go on to ways
        currentnode = Integer.MAX_VALUE;
        currentway = 0;
      }
      if (currentway < Integer.MAX_VALUE && ways != null)
      {
        while (currentway < ways.length)
        {
          if (!hasKeyValue(ways[currentway++], inRelationKey, yesKey))
          {
            return true;
          }
        }

        // finished ways, no on to relations
        currentway = Integer.MAX_VALUE;
        currentrelation = 0;
      }

      if (currentrelation < Integer.MAX_VALUE && relations != null)
      {
        if (currentrelation < relations.length)
        {
          if (!hasKeyValue(relations[currentrelation++], inRelationKey, yesKey))
          {
            return true;
          }
        }

        // finished relations, all done!
        currentrelation = Integer.MAX_VALUE;
      }
      return false;
    }

    @Override
    public Geometry next()
    {
      // remember, all the current... values are ONE MORE than the actual value to return!
      if (currentnode < Integer.MAX_VALUE)
      {
        return toGeometry(nodes[currentnode - 1]);
      }
      if (currentway < Integer.MAX_VALUE)
      {
        return toGeometry(ways[currentway - 1]);
      }
      if (currentrelation < Integer.MAX_VALUE)
      {
        return toGeometry(relations[currentrelation - 1]);
      }
      return null;
    }

    @Override
    public void remove()
    {
    }
  }

  protected final static String OSMHEADER = "OSMHeader";
  protected final static String OSMDATA = "OSMData";
  protected final static String PBF_OSM_SCHEMA_V06 = "OsmSchema-V0.6";
  protected final static String PBF_DENSE_NODES = "DenseNodes";
  protected final static String MRSVECTOR_TILE = "MRGEO.tile";

  protected final static String MRGEO = "mrgeo";
  // Don't use a raw size greater than 30MB. This should make the compressed block size in the
  // 16MB range.
  // private final static int MAX_RAW_SIZE = (30 * 1024 * 1024);

  // max bytes for a stream. This prevents the OSM pbf from complaining about messages too big.
  protected final static int MAX_STREAM_SIZE = (500 * 1024 * 1024);

  protected String[] strings = null;

  protected Node[] nodes = null;
  protected Way[] ways = null;
  protected Relation[] relations = null;

  protected int granularity = Integer.MIN_VALUE;
  protected int dateGranularity = Integer.MIN_VALUE;
  protected long lonOffset = Long.MIN_VALUE;
  protected long latOffset = Long.MIN_VALUE;

  // we'll use a LongRectangle here so we don't have to convert when calculating or reading/writing
  protected LongRectangle bounds = null;
  protected boolean mrsvectortile = false; // is this tile a cleaned MrsVectorTile before writing

  static final String[] TRUE_KEYS = { "yes", "true", "1", "on" };
  static final String[] FALSE_KEYS = { "no", "false", "0", "off" };

  private int[] trues = null;
  private int[] falses = null;

  static final String AREA = "area";
  protected int arealKey = -1;

  //  static final String[] AREA_KEYS = {"aeroway", "building", "landuse", "leisure", "natural"};
  //  private int[] areas = null;
  //
  //  static final String AEROWAYKEY = "aeroway";
  //  private int aerowayKey = -1;
  //
  //  static final String TAXIWAY = "taxiway";
  //  private int taxiwayKey = -1;

  static final String SINGLE_POINT = "single_point";
  protected int singlePointKey = -1;

  static final String NO = "no";
  static final String YES = "yes";
  protected int yesKey = -1;

  static final String INRELATION = "in_relation";
  protected int inRelationKey = -1;

  static final String INNER_RING = "inner";
  protected int innerKey = -1;
  static final String OUTER_RING = "outer";
  protected int outerKey = -1;

  static final String TYPE = "type";
  protected int typeKey = -1;

  static final String MULTIPOLYGON = "multipolygon";
  protected int multipolygonKey = -1;

  static final String UNKNOWN = "unknown";

  // protected so no one except us and WritableVectorTile can create a blank VectorTile
  protected VectorTile()
  {
  }

  protected VectorTile(final VectorTile copy)
  {
    this.dateGranularity = copy.dateGranularity;
    this.granularity = copy.granularity;
    this.latOffset = copy.latOffset;
    this.lonOffset = copy.lonOffset;
    this.mrsvectortile = copy.mrsvectortile;
    this.nodes = copy.nodes;
    this.relations = copy.relations;
    this.strings = copy.strings;
    this.ways = copy.ways;
  }

  public static VectorTile fromProtobuf(final byte[] buffer,
      int offset, int length) throws IOException
  {
    final ByteArrayInputStream stream = new ByteArrayInputStream(buffer, offset, length);
    try
    {
      return fromProtobuf(stream);
    }
    finally
    {
      stream.close();
    }
  }

  public static VectorTile fromProtobuf(final InputStream stream) throws IOException
  {
    final HeaderBlock header = readOSMHeader(stream);

    final boolean mrsvectortile = header.getOptionalFeaturesList().contains(MRSVECTOR_TILE);

    if (mrsvectortile)
    {
      final VectorTile vector = new VectorTile();
      vector.readOSMData(stream);
      vector.loadKeys();
      return vector;
    }

    final VectorTileCleaner vector = new VectorTileCleaner();
    vector.read(stream);
    vector.loadKeys();
    return vector;
  }

//  public static VectorTile fromProtobuf(final Value value) throws IOException
//  {
//    return fromProtobuf(value.get());
//  }

  protected static double fromNanoDegrees(final long value, final long granularity, final long offset)
  {
    // we do the divide (instead of a multiply by a small number)
    return ((value / 1000000000.0) + offset) * granularity;
    //    return (offset + (granularity * (double) value)) / 1000000000.0;
  }

  protected static long toNanoDegrees(final double value, final long granularity, final long offset)
  {
    return (long) (((value / granularity) - offset) * 1000000000.0);
  }

  protected static String human(final long bytes)
  {
    final int unit = 1024;
    if (bytes < unit)
    {
      return bytes + "B";
    }
    final int exp = (int) (Math.log(bytes) / Math.log(unit));
    final char pre = new String("KMGTPE").charAt(exp - 1);

    return String.format("%.1f%sB", bytes / Math.pow(unit, exp), pre);
  }

  protected static InputStream parseBlob(final DataInputStream dis, final BlobHeader header)
      throws IOException
      {
    final byte[] blob = new byte[header.getDatasize()];
    dis.read(blob);

    final Blob b = Blob.parseFrom(blob);

    InputStream blobData;
    if (b.hasZlibData())
    {
      blobData = new InflaterInputStream(b.getZlibData().newInput());
    }
    else
    {
      blobData = b.getRaw().newInput();
    }

    return blobData;
      }

  protected static BlobHeader parseHeader(final DataInputStream dis) throws IOException
  {
    final int len = dis.readInt();
    final byte[] blobHeader = new byte[len];
    dis.read(blobHeader);

    return BlobHeader.parseFrom(blobHeader);
  }

  protected static HeaderBlock readOSMHeader(final InputStream stream) throws IOException
  {
    DataInputStream dis;
    if (stream instanceof DataInputStream)
    {
      dis = (DataInputStream) stream;
    }
    else
    {
      dis = new DataInputStream(stream);
    }

    while (dis.available() > 0)
    {
      final BlobHeader header = parseHeader(dis);
      if (header.getType().equals(OSMHEADER))
      {
        final CodedInputStream blob = CodedInputStream.newInstance(parseBlob(dis, header));
        final HeaderBlock hb = HeaderBlock.parseFrom(blob);

        return hb;
      }
    }

    return null;
  }

  private static void writeBlob(final OutputStream stream, final Message msg, final String type)
      throws IOException
      {
    final BlobHeader.Builder headerbuilder = BlobHeader.newBuilder();
    final Blob.Builder blobbuilder = Blob.newBuilder();

    final ByteArrayOutputStream baos = new ByteArrayOutputStream();

    // uncomment to write uncompressed (and comment out zip section to before the builder.build())
    msg.writeTo(baos);

    // add the uncompressed data to the blob
    blobbuilder.setRaw(ByteString.copyFrom(baos.toByteArray()));

    // // set up the zip stream
    // final DeflaterOutputStream zip = new DeflaterOutputStream(baos, new
    // Deflater(Deflater.BEST_SPEED, false));
    // // write to the zipper
    // msg.writeTo(zip);
    //
    // // make sure we've finished processing
    // zip.close();
    //
    // // set the raw size of the data
    // blobbuilder.setRawSize(msg.getSerializedSize());
    //
    // // add the zipped data to the blob
    // blobbuilder.setZlibData(ByteString.copyFrom(baos.toByteArray()));

    final Blob blob = blobbuilder.build();

    // set the size
    headerbuilder.setDatasize(blob.getSerializedSize());
    headerbuilder.setType(type);

    final BlobHeader bh = headerbuilder.build();

    // need to write length of the header...
    final DataOutputStream daos = new DataOutputStream(stream);
    daos.writeInt(bh.getSerializedSize());

    // write the header
    bh.writeTo(stream);

    headerbuilder.clear();

    // now write the data blob
    blob.writeTo(stream);
    blobbuilder.clear();
      }

  static int findKey(final int key, final List<Integer> keys)
  {
    return keys.indexOf(key);
  }

  static int findValue(final int keyNdx, final List<Integer> values)
  {
    return values.get(keyNdx);
  }

  static boolean hasKey(final int key, final List<Integer> keys)
  {
    return findKey(key, keys) >= 0;
  }

  public void dump(final Node node, final PrintStream stream)
  {
    final StringBuilder builder = new StringBuilder();

    builder.append(node.getId() + ":");
    builder.append(fromNanoDegrees(node.getLon(), granularity, lonOffset) + " ");
    builder.append(fromNanoDegrees(node.getLat(), granularity, latOffset) + " ");
    for (int j = 0; j < node.getKeysCount(); j++)
    {
      builder.append(strings[node.getKeys(j)] + "|" + strings[node.getVals(j)] + " ");
    }
    stream.println(builder.toString());
  }

  public void dump(final PrintStream stream)
  {

    StringBuilder builder = new StringBuilder();

    stream.println("strings");
    builder.append("  ");
    for (int i = 0; i < strings.length; i++)
    {
      builder.append(i + ":" + strings[i] + " ");
      if (builder.length() > 80)
      {
        stream.println(builder.toString());
        builder = new StringBuilder("  ");
      }
    }
    stream.println(builder.toString());

    stream.println("nodes");
    if (nodes != null)
    {
      builder = new StringBuilder("  ");
      for (final Node n : nodes)
      {
        builder.append(n.getId() + ":");
        builder.append(fromNanoDegrees(n.getLon(), granularity, lonOffset) + " ");
        builder.append(fromNanoDegrees(n.getLat(), granularity, latOffset) + " ");
        for (int j = 0; j < n.getKeysCount(); j++)
        {
          builder.append(strings[n.getKeys(j)] + "|" + strings[n.getVals(j)] + " ");

          if (builder.length() > 80)
          {
            stream.println(builder.toString());
            builder = new StringBuilder("  ");
          }
        }
        if (builder.length() > 80)
        {
          stream.println(builder.toString());
          builder = new StringBuilder("  ");
        }
      }
      stream.println(builder.toString());
    }

    stream.println("ways");
    if (ways != null)
    {
      builder = new StringBuilder("  ");
      for (final Way w : ways)
      {
        builder.append(w.getId() + ":");

        long id = 0;
        for (int j = 0; j < w.getRefsCount(); j++)
        {
          id += w.getRefs(j);
          builder.append(id + " ");

          if (builder.length() > 80)
          {
            stream.println(builder.toString());
            builder = new StringBuilder("  ");
          }
        }
        for (int j = 0; j < w.getKeysCount(); j++)
        {
          builder.append(strings[w.getKeys(j)] + "|" + strings[w.getVals(j)] + " ");

          if (builder.length() > 80)
          {
            stream.println(builder.toString());
            builder = new StringBuilder("  ");
          }
        }
        if (builder.length() > 80)
        {
          stream.println(builder.toString());
          builder = new StringBuilder("  ");
        }
      }
      stream.println(builder.toString());
    }

    stream.println("relations");
    if (relations != null)
    {
      builder = new StringBuilder("  ");
      for (final Relation r : relations)
      {
        builder.append(r.getId() + ":");

        long id = 0;
        for (int j = 0; j < r.getMemidsCount(); j++)
        {
          final MemberType t = r.getTypes(j);
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

          id += r.getMemids(j);
          builder.append(id);
          final int role = r.getRolesSid(j);
          if (role != 0)
          {
            builder.append("(r:" + strings[role] + ")");
          }
          builder.append(' ');

          if (builder.length() > 80)
          {
            stream.println(builder.toString());
            builder = new StringBuilder("  ");
          }
        }

        for (int j = 0; j < r.getKeysCount(); j++)
        {
          if (j == 0)
          {
            builder.append(" ");
          }
          builder.append(strings[r.getKeys(j)] + "|" + strings[r.getVals(j)] + " ");

          if (builder.length() > 80)
          {
            stream.println(builder.toString());
            builder = new StringBuilder("  ");
          }
        }
        if (builder.length() > 80)
        {
          stream.println(builder.toString());
          builder = new StringBuilder("  ");
        }
      }
      stream.println(builder.toString());
    }

  }

  public void dump(final Relation relation, final PrintStream stream)
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

  public void dump(final Way way, final PrintStream stream)
  {
    final StringBuilder builder = new StringBuilder();

    builder.append(way.getId() + ":");
    for (int j = 0; j < way.getRefsCount(); j++)
    {
      builder.append(way.getRefs(j) + " ");
    }
    for (int j = 0; j < way.getKeysCount(); j++)
    {
      builder.append(strings[way.getKeys(j)] + "|" + strings[way.getVals(j)] + " ");
    }
    stream.println(builder.toString());
  }

  @SuppressWarnings("static-method")
  public int findKey(final Node node, final int key)
  {
    return findKey(key, node.getKeysList());
  }

  public int findKey(final Node node, final String key)
  {
    final int ndx = findString(key);
    if (ndx >= 0)
    {
      return findKey(ndx, node.getKeysList());
    }

    return ndx;
  }

  @SuppressWarnings("static-method")
  public int findKey(final Relation relation, final int key)
  {
    return findKey(key, relation.getKeysList());
  }

  public int findKey(final Relation relation, final String key)
  {
    final int ndx = findString(key);
    if (ndx >= 0)
    {
      return findKey(ndx, relation.getKeysList());
    }
    return ndx;
  }

  @SuppressWarnings("static-method")
  public int findKey(final Way way, final int key)
  {
    return findKey(key, way.getKeysList());
  }

  public int findKey(final Way way, final String key)
  {
    final int ndx = findString(key);
    if (ndx >= 0)
    {
      return findKey(ndx, way.getKeysList());
    }

    return ndx;
  }

  public String findKeyString(final Node node, final int key)
  {
    final int ndx = findKey(key, node.getKeysList());
    if (ndx >= 0)
    {
      return strings[findKey(ndx, node.getKeysList())];
    }

    throw new MrsVectorException("Key not found: " + key);
  }

  public String findKeyString(final Node node, final String key)
  {
    final int ndx = findString(key);
    if (ndx >= 0)
    {
      return strings[findKey(ndx, node.getKeysList())];
    }

    throw new MrsVectorException("Key not found: " + key);
  }

  public String findKeyString(final Relation relation, final int key)
  {
    final int ndx = findKey(key, relation.getKeysList());
    if (ndx >= 0)
    {
      return strings[findKey(ndx, relation.getKeysList())];
    }

    throw new MrsVectorException("Key not found: " + key);
  }

  public String findKeyString(final Relation relation, final String key)
  {
    final int ndx = findString(key);
    if (ndx >= 0)
    {
      return strings[findKey(ndx, relation.getKeysList())];
    }
    throw new MrsVectorException("Key not found: " + key);
  }

  public String findKeyString(final Way way, final int key)
  {
    final int ndx = findKey(key, way.getKeysList());
    if (ndx >= 0)
    {
      return strings[findKey(ndx, way.getKeysList())];
    }

    throw new MrsVectorException("Key not found: " + key);
  }

  public String findKeyString(final Way way, final String key)
  {
    final int ndx = findString(key);
    if (ndx >= 0)
    {
      return strings[findKey(ndx, way.getKeysList())];
    }

    throw new MrsVectorException("Key not found: " + key);
  }

  public Iterable<Geometry> geometries()
  {
    return new GeometryIterable();
  }

  public Bounds getBounds()
  {
    if (bounds == null)
    {
      calculateBounds();
    }

    final double l = fromNanoDegrees(bounds.getMinX(), granularity, lonOffset);
    final double b = fromNanoDegrees(bounds.getMinY(), granularity, latOffset);
    final double r = fromNanoDegrees(bounds.getMaxX(), granularity, lonOffset);
    final double t = fromNanoDegrees(bounds.getMaxY(), granularity, latOffset);

    return new Bounds(l, b, r, t);
  }

  @SuppressWarnings("static-method")
  public int getValue(final Node node, final int key)
  {
    final int ndx = findKey(key, node.getKeysList());
    if (ndx >= 0)
    {
      return findValue(ndx, node.getValsList());
    }

    throw new MrsVectorException("Key not found: " + key);
  }

  public int getValue(final Node node, final String key)
  {
    final int ndx = findKey(node, key);
    if (ndx >= 0)
    {
      return findValue(ndx, node.getValsList());
    }

    throw new MrsVectorException("Key not found: " + key);
  }

  @SuppressWarnings("static-method")
  public int getValue(final Relation relation, final int key)
  {
    final int ndx = findKey(key, relation.getKeysList());
    if (ndx >= 0)
    {
      return findValue(ndx, relation.getValsList());
    }

    throw new MrsVectorException("Key not found: " + key);
  }

  public int getValue(final Relation relation, final String key)
  {
    final int ndx = findKey(relation, key);
    if (ndx >= 0)
    {
      return findValue(ndx, relation.getValsList());
    }
    throw new MrsVectorException("Key not found: " + key);
  }

  @SuppressWarnings("static-method")
  public int getValue(final Way way, final int key)
  {
    final int ndx = findKey(key, way.getKeysList());
    if (ndx >= 0)
    {
      return findValue(ndx, way.getValsList());
    }

    throw new MrsVectorException("Key not found: " + key);

  }

  public int getValue(final Way way, final String key)
  {
    final int ndx = findKey(way, key);
    if (ndx >= 0)
    {
      return findValue(ndx, way.getValsList());
    }

    throw new MrsVectorException("Key not found: " + key);
  }

  public String getValueString(final Node node, final int key)
  {
    final int ndx = findKey(key, node.getKeysList());
    if (ndx >= 0)
    {
      return strings[findValue(ndx, node.getValsList())];
    }

    throw new MrsVectorException("Key not found: " + key);
  }

  public String getValueString(final Node node, final String key)
  {
    final int ndx = findString(key);
    if (ndx >= 0)
    {
      return strings[findKey(ndx, node.getKeysList())];
    }

    throw new MrsVectorException("Key not found: " + key);
  }

  public String getValueString(final Relation relation, final int key)
  {
    final int ndx = findKey(key, relation.getKeysList());
    if (ndx >= 0)
    {
      return strings[findValue(ndx, relation.getValsList())];
    }
    throw new MrsVectorException("Key not found: " + key);
  }

  public String getValueString(final Relation relation, final String key)
  {
    final int ndx = findString(key);
    if (ndx >= 0)
    {
      return strings[findValue(ndx, relation.getValsList())];
    }
    throw new MrsVectorException("Key not found: " + key);
  }

  public String getValueString(final Way way, final int key)
  {
    final int ndx = findKey(key, way.getKeysList());
    if (ndx >= 0)
    {
      return strings[findValue(ndx, way.getValsList())];
    }

    throw new MrsVectorException("Key not found: " + key);
  }

  public String getValueString(final Way way, final String key)
  {
    final int ndx = findString(key);
    if (ndx >= 0)
    {
      return strings[findValue(ndx, way.getValsList())];
    }

    throw new MrsVectorException("Key not found: " + key);
  }

  @SuppressWarnings("static-method")
  public boolean hasKey(final Node node, final int key)
  {
    return hasKey(key, node.getKeysList());
  }

  @SuppressWarnings("static-method")
  public boolean hasKey(final Relation relation, final int key)
  {
    return hasKey(key, relation.getKeysList());
  }

  @SuppressWarnings("static-method")
  public boolean hasKey(final Way way, final int key)
  {
    return hasKey(key, way.getKeysList());
  }

  @SuppressWarnings("static-method")
  public boolean hasKeyValue(final Node node, final int key, final int value)
  {
    final int ndx = findKey(key, node.getKeysList());
    if (ndx >= 0)
    {
      return value == findValue(ndx, node.getValsList());
    }

    return false;
  }

  @SuppressWarnings("static-method")
  public boolean hasKeyValue(final Relation relation, final int key, final int value)
  {
    final int ndx = findKey(key, relation.getKeysList());
    if (ndx >= 0)
    {
      return value == findValue(ndx, relation.getValsList());
    }

    return false;
  }

  @SuppressWarnings("static-method")
  public boolean hasKeyValue(final Way way, final int key, final int value)
  {
    final int ndx = findKey(key, way.getKeysList());
    if (ndx >= 0)
    {
      return value == findValue(ndx, way.getValsList());
    }

    return false;
  }

  public Iterable<Node> nodes()
  {
    return Arrays.asList(nodes);
  }

  public Iterable<Relation> relations()
  {
    return Arrays.asList(relations);
  }

  public Geometry toGeometry(final Node node)
  {
    final double lat = fromNanoDegrees(node.getLat(), granularity, latOffset);
    final double lon = fromNanoDegrees(node.getLon(), granularity, lonOffset);

    final WritablePoint point = GeometryFactory.createPoint(lon, lat);

    addAttributes(point, node.getKeysList(), node.getValsList());

    return point;
  }

  public Geometry toGeometry(final Relation relation)
  {
    //    if (isArea(relation))
    //    {
    //      // need to see if there are any other nodes in this collection other than a multipolyon
    //      boolean collection = false;
    //      final List<Integer> roles = relation.getRolesSidList();
    //      final List<MemberType> types = relation.getTypesList();
    //      for (int i = 0; i < relation.getMemidsCount(); i++)
    //      {
    //        switch (types.get(i))
    //        {
    //        case NODE:
    //          collection = true;
    //          break;
    //        case WAY:
    //          final int role = roles.get(i);
    //          if (!isInnerRole(role) && !isOuterRole(role))
    //          {
    //            collection = true;
    //          }
    //          break;
    //        case RELATION:
    //          collection = true;
    //          break;
    //        default:
    //          break;
    //        }
    //
    //        if (collection)
    //        {
    //          break;
    //        }
    //      }
    //
    //      if (!collection)
    //      {
    //        return createPolygon(relation);
    //      }
    //
    //    }

    return createCollection(relation);
  }

  public Geometry toGeometry(final Way way)
  {
    if (isArea(way))
    {
      return createPolygon(way);
    }

    //    final List<Long> refs = way.getRefsList();
    //    final long a = refs.get(0);
    //    final long b = refs.get(refs.size() - 1);
    //
    //    if (a == b)
    //    {
    //      return createRing(way);
    //    }
    LineString ls = createLine(way);
    if (ls.getNumPoints() > 2 && ls.getPoint(0).equals(ls.getPoint(ls.getNumPoints() - 1)))
    {
      return GeometryFactory.createLinearRing(ls);
    }
    return ls;
  }

  public byte[] toProtobuf() throws IOException
  {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try
    {
      toProtobuf(baos);

      // make sure we've finished
      baos.flush();

      return baos.toByteArray();
    }
    finally
    {
      baos.close();
    }
  }

  public void toProtobuf(final File file) throws IOException
  {
    final FileOutputStream stream = new FileOutputStream(file);
    try
    {
      toProtobuf(stream);
    }
    finally
    {
      stream.close();
    }
  }

  public void toProtobuf(final OutputStream stream) throws IOException
  {
    writeHeader(stream);

    boolean finished = false;
    while (!finished)
    {
      finished = writeOSM(stream);
    }
  }

  public Iterable<Way> ways()
  {
    return Arrays.asList(ways);
  }

  protected void calculateBounds()
  {
    // a LongRectangle initializes to "unset", so we don't need to do any initialization here
    bounds = new LongRectangle();
    for (final Node n : nodes)
    {
      bounds.add(n.getLon(), n.getLat());
    }
  }

  protected void loadAreal()
  {
    if (mrsvectortile)
    {
      arealKey = findString(AREA);

      //      final ArrayList<Integer> list = new ArrayList<Integer>();
      //      for (final String str : AREA_KEYS)
      //      {
      //        final int ndx;
      //        ndx = findString(str);
      //
      //        if (ndx >= 0)
      //        {
      //          list.add(ndx);
      //        }
      //      }
      //
      //      areas = ArrayUtils.toPrimitive(list.toArray(new Integer[0]));
      //
      //      aerowayKey = findString(AEROWAYKEY);
      //      taxiwayKey = findString(TAXIWAY);

    }
    else
    {
      arealKey = findStringUnsorted(AREA);

      //      final ArrayList<Integer> list = new ArrayList<Integer>();
      //      for (final String str : AREA_KEYS)
      //      {
      //        final int ndx;
      //        ndx = findStringUnsorted(str);
      //
      //        if (ndx >= 0)
      //        {
      //          list.add(ndx);
      //        }
      //      }
      //
      //      areas = ArrayUtils.toPrimitive(list.toArray(new Integer[0]));
      //
      //      aerowayKey = findStringUnsorted(AEROWAYKEY);
      //      taxiwayKey = findStringUnsorted(TAXIWAY);
    }
  }

  protected void loadFalses()
  {
    final ArrayList<Integer> list = new ArrayList<Integer>();
    for (final String str : FALSE_KEYS)
    {
      final int ndx;
      if (mrsvectortile)
      {
        ndx = findString(str);
      }
      else
      {
        ndx = findStringUnsorted(str);
      }

      if (ndx >= 0)
      {
        list.add(ndx);
      }
    }

    falses = ArrayUtils.toPrimitive(list.toArray(new Integer[0]));
  }

  protected void loadKeys()
  {
    loadAreal();
    loadFalses();
    loadInRelation();
    loadMultipolygon();
    loadRoles();
    loadSinglePoints();
    loadTrues();
  }

  private void loadInRelation()
  {
    if (mrsvectortile)
    {
      inRelationKey = findString(INRELATION);
    }
    else
    {
      inRelationKey = findStringUnsorted(INRELATION);
    }

  }

  private void loadMultipolygon()
  {
    if (mrsvectortile)
    {
      multipolygonKey = findString(MULTIPOLYGON);
      typeKey = findString(TYPE);
    }
    else
    {
      multipolygonKey = findStringUnsorted(MULTIPOLYGON);
      typeKey = findStringUnsorted(TYPE);
    }

  }

  protected void loadRoles()
  {

    if (mrsvectortile)
    {
      innerKey = findString(INNER_RING);
      outerKey = findString(OUTER_RING);
    }
    else
    {
      innerKey = findStringUnsorted(INNER_RING);
      outerKey = findStringUnsorted(OUTER_RING);
    }
  }

  protected void loadSinglePoints()
  {

    if (mrsvectortile)
    {
      singlePointKey = findString(SINGLE_POINT);
      yesKey = findString(YES);
    }
    else
    {
      singlePointKey = findStringUnsorted(SINGLE_POINT);
      yesKey = findStringUnsorted(YES);
    }

  }

  protected void loadTrues()
  {    
    final ArrayList<Integer> list = new ArrayList<Integer>();
    for (final String str : TRUE_KEYS)
    {
      final int ndx;
      if (mrsvectortile)
      {
        ndx = findString(str);
      }
      else
      {
        ndx = findStringUnsorted(str);
      }

      if (ndx >= 0)
      {
        list.add(ndx);
      }
    }

    trues = ArrayUtils.toPrimitive(list.toArray(new Integer[0]));

  }

  protected void parseDenseNodes(final DenseNodes dense)
  {
    final int count = dense.getIdCount();

    final Node[] denseNodes = new Node[count];
    final Node.Builder nb = Node.newBuilder();

    // kv offset is the running offset within the key/value list for the nodes.
    // in dense nodes, the k/v are stored as consecutive values (k, v, k, v, ...) with a "0" key
    // value denoting the end of the k/v for a dense nodemap.
    int kvoffset = 0;

    long lon = 0;
    long lat = 0;

    long offset = nodes == null ? 0 : nodes.length;
    for (int i = 0; i < count; i++)
    {
      nb.clear();

      // these are DELTA coded, so we need to keep a running count...
      lon += dense.getLon(i);
      lat += dense.getLat(i);

      // build the nodemap with the nodemap builder
      nb.clear();

      // TODO: Add the original id as a k/v? (that will balloon the string table immensely)
      nb.setId(offset);
      nb.setLat(lat);
      nb.setLon(lon);

      if (dense.getKeysValsCount() > 0)
      {
        int k = dense.getKeysVals(kvoffset++);
        while (k != 0)
        {
          final int v = dense.getKeysVals(kvoffset++);

          nb.addKeys(k);
          nb.addVals(v);

          k = dense.getKeysVals(kvoffset++);
        }
      }

      nb.setUnknownFields(dense.getUnknownFields());
      denseNodes[i] = nb.build();

      offset++;
    }

    if (nodes == null)
    {
      nodes = denseNodes;
    }
    else
    {
      nodes = (Node[]) ArrayUtils.addAll(nodes, denseNodes);
    }
  }

  protected void parseNodes(final List<Node> nodesList)
  {
    if (nodes == null)
    {
      nodes = nodesList.toArray(new Node[0]);
    }
    else
    {
      nodes = (Node[]) ArrayUtils.addAll(nodes, nodesList.toArray(new Node[0]));
    }
  }

  protected void parseOSM(final CodedInputStream blob) throws IOException
  {
    final PrimitiveBlock pb = PrimitiveBlock.parseFrom(blob);
    // System.out.println("pb: ");

    // load some constants
    granularity = pb.getGranularity();
    lonOffset = pb.getLonOffset();
    latOffset = pb.getLatOffset();
    dateGranularity = pb.getDateGranularity();

    // load strings
    if (pb.getStringtable().getSCount() > 0)
    {
      parseStrings(pb.getStringtable());
    }

    //    System.out.println("s: " + pb.getStringtable().getSCount());

    for (int i = 0; i < pb.getPrimitivegroupCount(); i++)
    {
      final PrimitiveGroup pg = pb.getPrimitivegroup(i);

      //      System.out.println("n: " + pg.getNodesCount());
      //      System.out.println("d: " + pg.getDense().getIdCount());
      //      System.out.println("w: " + pg.getWaysCount());
      //      System.out.println("r: " + pg.getRelationsCount());

      // parse nodes
      if (pg.getNodesCount() > 0)
      {
        parseNodes(pg.getNodesList());
      }

      // parse dense nodes
      if (pg.getDense().getIdCount() > 0)
      {
        parseDenseNodes(pg.getDense());
      }

      // parse ways
      if (pg.getWaysCount() > 0)
      {
        parseWays(pg.getWaysList());
      }

      // parse relations
      if (pg.getRelationsCount() > 0)
      {
        parseRelations(pg.getRelationsList());
      }
    }
  }

  protected void parseRelations(final List<Relation> relationsList)
  {
    if (relations == null)
    {
      relations = relationsList.toArray(new Relation[0]);
    }
    else
    {
      relations = (Relation[]) ArrayUtils.addAll(relations, relationsList.toArray(new Relation[0]));
    }
  }

  protected void parseStrings(final StringTable stringtable)
  {
    // System.out.println("  strings (" + st.getSCount() + ")");

    final String[] newStrings = new String[stringtable.getSCount()];
    for (int i = 0; i < stringtable.getSCount(); i++)
    {
      newStrings[i] = stringtable.getS(i).toStringUtf8();
    }

    if (strings == null)
    {
      strings = newStrings;
    }
    else
    {
      strings = (String[]) ArrayUtils.addAll(strings, newStrings);
    }
  }

  protected void parseWays(final List<Way> waysList)
  {
    if (ways == null)
    {
      ways = waysList.toArray(new Way[0]);
    }
    else
    {
      ways = (Way[]) ArrayUtils.addAll(ways, waysList.toArray(new Way[0]));
    }
  }

  private void addAttributes(final WritableGeometry p, final List<Integer> keys,
    final List<Integer> values)
  {
    for (int i = 0; i < keys.size(); i++)
    {
      p.setAttribute(strings[keys.get(i)], strings[values.get(i)]);
    }
  }

  private DenseNodes buildDenseNodes()
  {
    final DenseNodes.Builder db = DenseNodes.newBuilder();

    long id = 0;
    long lat = 0;
    long lon = 0;

    long lastid = 0;
    long lastlon = 0;
    long lastlat = 0;

    for (final Node node : nodes)
    {
      id = node.getId() - lastid;
      lat = node.getLat() - lastlat;
      lon = node.getLon() - lastlon;

      lastid = node.getId();
      lastlat = node.getLat();
      lastlon = node.getLon();

      db.addId(id);
      db.addLat(lat);
      db.addLon(lon);

      for (int j = 0; j < node.getKeysCount(); j++)
      {
        db.addKeysVals(node.getKeys(j));
        db.addKeysVals(node.getVals(j));
      }
      db.addKeysVals(0);
    }

    return db.build();
  }

  private StringTable buildStringTable()
  {
    final StringTable.Builder sb = StringTable.newBuilder();
    for (final String str : strings)
    {
      sb.addS(ByteString.copyFromUtf8(str));
    }
    return sb.build();
  }

  private Geometry createCollection(final Relation relation)
  {
    final WritableGeometryCollection collection = GeometryFactory.createGeometryCollection();

    if (isArea(relation))
    {
      List<WritablePolygon> polys = new ArrayList<WritablePolygon>();

      WritablePolygon poly = poly = GeometryFactory.createPolygon();;

      WritableLinearRing outer = null;
      WritableLinearRing inner = null;

      final List<Long> ids = relation.getMemidsList();
      final List<Integer> roles = relation.getRolesSidList();
      final List<MemberType> types = relation.getTypesList();
      int id = 0;
      for (int i = 0; i < relation.getMemidsCount(); i++)
      {
        id += ids.get(i).intValue();  // DELTA encoding

        if (types.get(i) == MemberType.WAY)
        {
          if (id < ways.length)
          {
            final Way way = ways[id];
            final int role = roles.get(i);

            if (isOuterRole(role))
            {
              if (outer == null)
              {
                outer = GeometryFactory.createLinearRing();
              }
              else
              {

              }

              addWayToRing(outer, way);

              if (outer.getNumPoints() >= 4)
              {
                // if the 1st and last points equal, the polygon is closed
                if (outer.getPoint(0).getGeohashBits() == outer.getPoint(outer.getNumPoints() - 1).getGeohashBits())
                {
                  poly.setExteriorRing(outer);

                  if (!outer.isEmpty())
                  {
                    polys.add(poly);
                  }
                  outer = null;
                }
              }
            }
            else if (isInnerRole(role))
            {            
              if (inner == null)
              {
                inner = GeometryFactory.createLinearRing();
              }

              addWayToRing(inner, way);

              // if the 1st and last points equal, the polygon is closed
              if (inner.getNumPoints() >= 4)
              {
                if (inner.getPoint(0).getGeohashBits() == inner.getPoint(inner.getNumPoints() - 1).getGeohashBits())
                {
                  poly.addInteriorRing(inner);
                  inner = null;
                }
              }
            }
          }
        }
      }

      if (outer != null)
      {
        outer.closeRing();
        poly.setExteriorRing(outer);
      }

      if (inner != null)
      {
        inner.closeRing();
        poly.addInteriorRing(inner);
      }

      // only 1 polygon, return it as a polygon.
      if (polys.size() == 1)
      {
        return polys.get(0);
      }

      for (WritablePolygon p: polys)
      {
        collection.addGeometry(p);
      }
    }
    else
    {
      final List<Long> ids = relation.getMemidsList();
      final List<Integer> roles = relation.getRolesSidList();
      final List<MemberType> types = relation.getTypesList();

      int id = 0;
      for (int i = 0; i < relation.getMemidsCount(); i++)
      {
        id += ids.get(i).intValue();  // DELTA encoding

        final int role = roles.get(i);
        switch (types.get(i))
        {
        case NODE:
          if (id < nodes.length)
          {
            collection.addGeometry((WritableGeometry) toGeometry(nodes[id]));
          }
          break;
        case WAY:
          if (!isInnerRole(role) && !isOuterRole(role))
          {
            if (id < ways.length)
            {
              collection.addGeometry((WritableGeometry) toGeometry(ways[id]));
            }
          }
          break;
        case RELATION:
          if (id < relations.length)
          {
            collection.addGeometry((WritableGeometry) toGeometry(relations[id]));
          }
          break;
        default:
          break;
        }

        collection.setRole(i, strings[role]);
        addAttributes(collection, relation.getKeysList(), relation.getValsList());
      }
    }
    return collection;
  }

  private void addWayToRing(WritableLinearRing ring, final Way way)
  {
    int wid = 0;
    for (final long ref : way.getRefsList())
    {
      wid += ref; // DELTA encoding

      final Node node = nodes[wid];
      final double lat = fromNanoDegrees(node.getLat(), granularity, latOffset);
      final double lon = fromNanoDegrees(node.getLon(), granularity, lonOffset);

      ring.addPoint(GeometryFactory.createPoint(lon, lat));
    }
  }

  private LineString createLine(final Way way)
  {
    final WritableLineString line = GeometryFactory.createLineString();
    int id = 0;
    for (final long ref : way.getRefsList())
    {
      id += ref; // DELTA encoding
      final Node node = nodes[id];
      final double lat = fromNanoDegrees(node.getLat(), granularity, latOffset);
      final double lon = fromNanoDegrees(node.getLon(), granularity, lonOffset);

      line.addPoint(GeometryFactory.createPoint(lon, lat));
    }
    addAttributes(line, way.getKeysList(), way.getValsList());
    return line;
  }

  @SuppressWarnings("unused")
  private Geometry createPolygon(final Relation relation)
  {    
    // polygons can be tricky...
    // See http://wiki.openstreetmap.org/wiki/Relation:multipolygon
    final WritablePolygon polygon = GeometryFactory.createPolygon();
    final WritableLinearRing outer = GeometryFactory.createLinearRing();

    final List<Long> ids = relation.getMemidsList();
    final List<Integer> roles = relation.getRolesSidList();
    final List<MemberType> types = relation.getTypesList();

    int id = 0;
    for (int i = 0; i < relation.getMemidsCount(); i++)
    {
      id += ids.get(i).intValue();  // DELTA encoding
      if (types.get(i) == MemberType.WAY)
      {
        final Way way = ways[id];
        final int role = roles.get(i);
        // assume if the role is missing (role = 0), it's an outer ring
        if (isOuterRole(role) || role == 0)
        {
          int wid = 0;
          for (final long ref : way.getRefsList())
          {
            wid += ref; // DELTA encoding
            final Node node = nodes[wid];
            final double lat = fromNanoDegrees(node.getLat(), granularity, latOffset);
            final double lon = fromNanoDegrees(node.getLon(), granularity, lonOffset);

            outer.addPoint(GeometryFactory.createPoint(lon, lat));
          }
        }
        else if (isInnerRole(role))
        {
          polygon.addInteriorRing(createRing(way));
        }
      }
    }

    if (outer.getNumPoints() == 0)
    {
      // hmmm no outer rings, but have inner rings. That must be a mistake. We'll assume they should
      // be outer rings.
      if (polygon.getNumInteriorRings() > 0)
      {
        final WritableLinearRing o = GeometryFactory.createLinearRing();
        for (int i = 0; i < polygon.getNumInteriorRings(); i++)
        {
          final LinearRing ring = polygon.getInteriorRing(i);
          for (final Point p : ring.getPoints())
          {
            o.addPoint(p);
          }
        }
        o.closeRing();

        final WritablePolygon p = GeometryFactory.createPolygon();
        p.setExteriorRing(o);
        return p;
      }
    }
    outer.closeRing();
    polygon.setExteriorRing(outer);

    return polygon;
  }

  private Polygon createPolygon(final Way way)
  {
    final WritablePolygon polygon = GeometryFactory.createPolygon();
    polygon.setExteriorRing(createRing(way));

    addAttributes(polygon, way.getKeysList(), way.getValsList());
    return polygon;
  }

  private LinearRing createRing(final Way way)
  {
    final WritableLinearRing ring = GeometryFactory.createLinearRing();

    int id = 0;
    for (final long ref : way.getRefsList())
    {
      id += ref; // DELTA encoding
      final Node node = nodes[id];
      final double lat = fromNanoDegrees(node.getLat(), granularity, latOffset);
      final double lon = fromNanoDegrees(node.getLon(), granularity, lonOffset);

      ring.addPoint(GeometryFactory.createPoint(lon, lat));
    }
    ring.closeRing();
    addAttributes(ring, way.getKeysList(), way.getValsList());
    return ring;
  }

  protected void readOSMData(final InputStream stream) throws IOException
  {
    DataInputStream dis;
    if (stream instanceof DataInputStream)
    {
      dis = (DataInputStream) stream;
    }
    else
    {
      dis = new DataInputStream(stream);
    }

    while (dis.available() > 0)
    {
      final BlobHeader header = parseHeader(dis);

      if (header.getType().equals(OSMDATA))
      {
        final CodedInputStream blob = CodedInputStream.newInstance(parseBlob(dis, header));

        // for speed, we make _large_ tiles, we don't want protobuf to complain...
        blob.setSizeLimit(MAX_STREAM_SIZE);

        parseOSM(blob);
      }
    }
  }

  private void writeHeader(final OutputStream stream) throws IOException
  {

    final HeaderBlock.Builder headerbuilder = HeaderBlock.newBuilder();
    headerbuilder.clear();

    headerbuilder.setWritingprogram(MRGEO);

    headerbuilder.addRequiredFeatures(PBF_OSM_SCHEMA_V06);
    headerbuilder.addRequiredFeatures(PBF_DENSE_NODES);
    // headerbuilder.addOptionalFeatures(PBF_SORT_TYPE_THEN_ID);
    headerbuilder.addOptionalFeatures(MRSVECTOR_TILE);

    if (bounds == null)
    {
      calculateBounds();
    }

    final HeaderBBox.Builder bboxbuilder = HeaderBBox.newBuilder();

    bboxbuilder.setLeft(bounds.getMinX());
    bboxbuilder.setBottom(bounds.getMinY());
    bboxbuilder.setRight(bounds.getMaxX());
    bboxbuilder.setTop(bounds.getMaxY());
    headerbuilder.setBbox(bboxbuilder.build());

    writeBlob(stream, headerbuilder.build(), OSMHEADER);
    headerbuilder.clear();
  }

  private boolean writeOSM(final OutputStream stream) throws IOException
  {
    final PrimitiveBlock.Builder pb = PrimitiveBlock.newBuilder();
    final PrimitiveGroup.Builder gb = PrimitiveGroup.newBuilder();

    pb.setDateGranularity(dateGranularity);
    pb.setGranularity(granularity);
    pb.setLatOffset(latOffset);
    pb.setLonOffset(lonOffset);

    pb.setStringtable(buildStringTable());

    // add nodes
    // gb.addAllNodes(Arrays.asList(nodes));

    // add dense nodes
    if (nodes != null)
    {
      gb.setDense(buildDenseNodes());
    }

    // add ways
    if (ways != null)
    {
      gb.addAllWays(Arrays.asList(ways));
    }

    // add relations
    if (relations != null)
    {
      gb.addAllRelations(Arrays.asList(relations));
    }

    pb.addPrimitivegroup(gb.build());

    writeBlob(stream, pb.build(), OSMDATA);

    // only clear the primitive group. Don't want to clear the initialized values.
    pb.clearPrimitivegroup();

    return true;
  }

  int findString(final String str)
  {
    final int ndx = Arrays.binarySearch(strings, str);
    if (ndx < 0)
    {
      return -1;
    }
    return ndx;
  }

  int findStringUnsorted(final String str)
  {
    for (int i = 0; i < strings.length; i++)
    {
      if (str.equals(strings[i]))
      {
        return i;
      }
    }

    return -1;
  }

  @SuppressWarnings("static-method")
  int getValue(final int key, final List<Integer> keys, final List<Integer> values)
  {
    final int ndx = findKey(key, keys);
    if (ndx >= 0)
    {
      return findValue(ndx, values);
    }

    throw new MrsVectorException("Key not found: " + key);
  }

  boolean isArea(final List<Integer> keys, final List<Integer> values)
  {
    if (arealKey < 0)
    {
      loadAreal();
    }
    final int ndx = findKey(arealKey, keys);
    if (ndx >= 0)
    {
      if (!isFalse(findValue(ndx, values)))
      {
        return true;
      }
    }

    //    for (int key: areas)
    //    {
    //      // if we have any of these keys, we're a closed way (polygon)
    //      if (hasKey(key, keys))
    //      {
    //        // special case if aeroway == taxiway
    //        if (key == aerowayKey)
    //        {
    //          int val = findValue(key, values);
    //          if (val != taxiwayKey)
    //          {
    //            return true;
    //          }
    //        }
    //        return true;
    //      }
    //    }

    return false;
  }

  boolean isArea(final Relation relation)
  {
    return isArea(relation.getKeysList(), relation.getValsList());
  }

  boolean isArea(final Way way)
  {
    return isArea(way.getKeysList(), way.getValsList());
  }

  boolean isFalse(final int v)
  {
    if (falses == null)
    {
      loadFalses();
    }

    for (final int t : falses)
    {
      if (v == t)
      {
        return true;
      }
    }

    return false;
  }

  boolean isInnerRole(final int role)
  {
    if (innerKey < 0)
    {
      loadRoles();
    }
    return role == innerKey;
  }

  boolean isOuterRole(final int role)
  {
    if (outerKey < 0)
    {
      loadRoles();
    }
    return role == outerKey;
  }

  boolean isTrue(final int v)
  {
    if (trues == null)
    {
      loadTrues();
    }

    for (final int t : trues)
    {
      if (v == t)
      {
        return true;
      }
    }
    return false;
  }

  public void printstats()
  {
    int totalsize = 0;
    int size = 0;

    if (strings != null)
    {
      for (final String s : strings)
      {
        size += s.length();
      }
      totalsize += size;

      System.out.println("strings: " + strings.length + " (" + human(size) + ")");

    }

    {
      try
      {

        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        final ObjectOutputStream out = new ObjectOutputStream(bytes);
        out.writeObject(nodes);

        size = bytes.size();
        totalsize += size;
        System.out.println("nodes: " + nodes.length + " (" + human(size) + ")");
      }
      catch (final IOException e)
      {
        e.printStackTrace();
      }

    }

    try
    {
      final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      final ObjectOutputStream out = new ObjectOutputStream(bytes);
      out.writeObject(ways);

      size = bytes.size();
      totalsize += size;
      System.out.println("ways: " + ways.length + " (" + human(size) + ")");
    }
    catch (final IOException e)
    {
      e.printStackTrace();
    }

    try
    {
      final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      final ObjectOutputStream out = new ObjectOutputStream(bytes);
      out.writeObject(relations);

      size = bytes.size();
      totalsize += size;
      System.out.println("relations: " + relations.length + " (" + human(size) + ")");
    }
    catch (final IOException e)
    {
      e.printStackTrace();
    }

    System.out.println("total size: " + human(totalsize));
  }

  public static void main(String[] args)
  {
    if (args.length != 3)
    {
      System.err.println("Usage: VectorTile <vector> <tx> <ty>");
    }
    else
    {
      String vectorName = args[0];
      MrsVector vector = null;
      try
      {
        int tx = Integer.parseInt(args[1]);
        int ty = Integer.parseInt(args[2]);
        MrsVectorPyramid vectorPyramid = MrsVectorPyramid.open(vectorName);
        vector = vectorPyramid.getHighestResVector();
        VectorTile tile = vector.getTile(tx, ty);
        if (tile != null)
        {
          tile.dump(System.out);
          System.out.println("WKT Geometries:");
          for (Geometry geom : tile.geometries())
          {
            System.out.println("  " + WktConverter.toWkt(geom));
          }
        }
        else
        {
          System.err.println("Invalid tile, tile bounds are: " + vectorPyramid.getTileBounds(vectorPyramid.getMaximumLevel()));
        }
      }
      catch(NumberFormatException nfe)
      {
        System.err.println("Invalid tile coordinates given");
      }
      catch (IOException e)
      {
        System.err.println("IOException: " + e);
      }
      finally
      {
        if (vector != null)
        {
          vector.close();
        }
      }
    }
  }

}
