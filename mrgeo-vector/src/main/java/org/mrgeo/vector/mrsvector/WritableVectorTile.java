/*
 * Copyright 2009-2014 DigitalGlobe, Inc.
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

import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.carrotsearch.hppc.ObjectArrayList;
import com.carrotsearch.hppc.ObjectIntOpenHashMap;
import com.carrotsearch.hppc.ObjectOpenHashSet;
import com.carrotsearch.hppc.cursors.LongObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.mrgeo.geometry.*;
import org.mrgeo.geometry.Geometry.Type;
import org.mrgeo.vector.mrsvector.pbf.OsmFormat.Node;
import org.mrgeo.vector.mrsvector.pbf.OsmFormat.Relation;
import org.mrgeo.vector.mrsvector.pbf.OsmFormat.Relation.MemberType;
import org.mrgeo.vector.mrsvector.pbf.OsmFormat.Way;

import java.util.*;
import java.util.Map.Entry;

public class WritableVectorTile extends VectorTile
{
  ObjectOpenHashSet<Geometry> geometries;

  boolean writing = false;

  public WritableVectorTile()
  {
    super();
    init();
  }

  private WritableVectorTile(final VectorTile vector)
  {
    super(vector);
    init();
  }

  public static WritableVectorTile createWritableMrsVector(final VectorTile vector)
  {
    final WritableVectorTile writable = new WritableVectorTile(vector);
    return writable;
  }

//  public static void main(final String[] args)
//  {
//
//    try
//    {
//      FileInputStream fis;
//      DataInputStream dis;
//      VectorTile tile;
//      long start;
//      File file;
//
//       fis = new FileInputStream("/data/osm/sydney.osm.pbf");
//      // fis = new FileInputStream("/data/osm/central-america-latest.osm.pbf");
//      // fis = new FileInputStream("/data/osm/north-america-latest.osm.pbf");
//      // fis = new FileInputStream("/data/osm/planet-latest.osm.pbf");
//      // fis = new FileInputStream("/data/osm/dc-baltimore.osm.pbf");
//
//      dis = new DataInputStream(fis);
//
//      start = System.currentTimeMillis();
//      tile = VectorTile.fromProtobuf(dis);
//      fis.close();
//
//      System.out.println("read time: " + (System.currentTimeMillis() - start));
//
//      tile.printstats();
//
//      file = new File("/data/osm/block.osm.pbf");
//      if (file.exists())
//      {
//        file.delete();
//      }
//
//      start = System.currentTimeMillis();
//      tile.toProtobuf(file);
//
//      System.out.println("write time: " + (System.currentTimeMillis() - start));
//
//      final File out = new File("/data/osm/cleaned.txt");
//      final PrintStream stream = new PrintStream(out);
//      tile.dump(stream);
//      stream.close();
//
//      // fis = new FileInputStream("/data/osm/block.osm.pbf");
//      // dis = new DataInputStream(fis);
//      //
//      // start = System.currentTimeMillis();
//      // tile = VectorTile.fromProtobuf(dis);
//      // fis.close();
//      //
//      // System.out.println("read time: " + (System.currentTimeMillis() - start));
//      // tile.printstats();
//      //
//      // out = new File("/data/osm/read.txt");
//      // stream = new PrintStream(out);
//      // tile.dump(stream);
//      // stream.close();
//      //
//      // file = new File("/data/osm/rewritten.osm.pbf");
//      // if (file.exists())
//      // {
//      // file.delete();
//      // }
//      //
//      // start = System.currentTimeMillis();
//      // tile.toProtobuf(file);
//      //
//      // System.out.println("write time: " + (System.currentTimeMillis() - start));
//
//      fis = new FileInputStream("/data/osm/block.osm.pbf");
//      dis = new DataInputStream(fis);
//
//      start = System.currentTimeMillis();
//      tile = VectorTile.fromProtobuf(dis);
//      fis.close();
//
//      System.out.println("read time: " + (System.currentTimeMillis() - start));
//      tile.printstats();
//      System.out.println();
//      // // out = new File("/data/osm/read2.txt");
//      // // stream = new PrintStream(out);
//      // // tile.dump(stream);
//      // // stream.close();
//      //
//      // file = new File("/data/osm/rewritten2.osm.pbf");
//      // if (file.exists())
//      // {
//      // file.delete();
//      // }
//      //
//      // start = System.currentTimeMillis();
//      // tile.toProtobuf(file);
//      // fis.close();
//
//      final WritableVectorTile writable = new WritableVectorTile();
//
//      writable.beginWriting();
//      start = System.currentTimeMillis();
//      final Iterator<Geometry> iter = tile.geometries().iterator();
//      while (iter.hasNext())
//      {
//        final Geometry geometry = iter.next();
//
//        writable.add(geometry);
//        System.out.println(geometry.toString());
//      }
//      System.out.println("loop time: " + (System.currentTimeMillis() - start));
//
//      start = System.currentTimeMillis();
//
//      writable.endWriting();
//
//      System.out.println("clean time: " + (System.currentTimeMillis() - start));
//      start = System.currentTimeMillis();
//
//      writable.printstats();
//      System.out.println();
//
//      // out = new File("/data/osm/read2.txt");
//      // stream = new PrintStream(out);
//      // tile.dump(stream);
//      // stream.close();
//
//      file = new File("/data/osm/writable.osm.pbf");
//      if (file.exists())
//      {
//        file.delete();
//      }
//
//      start = System.currentTimeMillis();
//      writable.toProtobuf(file);
//      fis.close();
//      System.out.println("write time: " + (System.currentTimeMillis() - start));
//
//    }
//    catch (final Exception ex)
//    {
//      ex.printStackTrace();
//    }
//
//  }

  private static void buildNodes(final VectorTileCleaner tile,
    final LongObjectOpenHashMap<Node> nodeset,
    final ObjectArrayList<Node> duplicateNodeset)
  {
    List<Node> nodeList = new ArrayList<Node>(nodeset.size() + duplicateNodeset.size());
    // Add the nodes from the nodeset to a list of nodes
    {
      Iterator<LongObjectCursor<Node>> iter = nodeset.iterator();
      while (iter.hasNext())
      {
        LongObjectCursor<Node> item = iter.next();
        nodeList.add(item.value);
      }
    }
    // Add the nodes from the duplicateNodeset to the list of nodes
    {
      Iterator<ObjectCursor<Node>> iter = duplicateNodeset.iterator();
      while (iter.hasNext())
      {
        ObjectCursor<Node> item = iter.next();
        nodeList.add(item.value);
      }
    }
    // Add all of the nodes to the tile
    tile.addNodes(nodeList);
  }

  private static void buildRelations(final VectorTileCleaner tile,
    final LongObjectOpenHashMap<Relation> relationset)
  {
    tile.addRelations(Arrays.asList(relationset.values().toArray(Relation.class)));
  }

  //  private static ObjectIntOpenHashMap<String> buildStringList(final TreeSet<String> stringset)
  //  {
  //    final ObjectIntOpenHashMap<String> stringList = new ObjectIntOpenHashMap<>(stringset
  //        .size());
  //
  //    int offset = 0;
  //    for (final String key : stringset)
  //    {
  //      stringList.put(key, offset++);
  //    }
  //
  //    return stringList;
  //  }

  private static ObjectIntOpenHashMap<String> buildStringList(final String[] stringarray)
  {
    final ObjectIntOpenHashMap<String> stringList = new ObjectIntOpenHashMap<String>(stringarray.length);

    for (int i = 0; i < stringarray.length; i++)
    {
      stringList.put(stringarray[i], i);
    }
    return stringList;
  }

  private static void buildStrings(final VectorTileCleaner tile, final TreeSet<String> stringset)
  {
    tile.addStrings(new ArrayList<String>(stringset));
  }

  private static void
  buildWays(final VectorTileCleaner tile, final LongObjectOpenHashMap<Way> wayset)
  {
    tile.addWays(Arrays.asList(wayset.values().toArray(Way.class)));
  }

  public void add(final Geometry geometry)
  {
    if (!writing)
    {
      throw new MrsVectorException("Must call beginWriting() before calling add()");
    }

    geometries.add(geometry);
  }

  public void beginWriting()
  {
    if (writing)
    {
      throw new MrsVectorException("Can not call beginWriting() more than once");
    }
    writing = true;

    geometries = new ObjectOpenHashSet<Geometry>();
  }

  public void endWriting()
  {
    if (!writing)
    {
      throw new MrsVectorException("Must call beginWriting() before calling endWriting()");
    }

    writing = false;



    final TreeSet<String> stringset = new TreeSet<String>();
    final LongObjectOpenHashMap<Node> nodeset = new LongObjectOpenHashMap<Node>();
    final ObjectArrayList<Node> duplicateNodeset = new ObjectArrayList<Node>();
    final LongObjectOpenHashMap<Way> wayset = new LongObjectOpenHashMap<Way>();
    final LongObjectOpenHashMap<Relation> relationset = new LongObjectOpenHashMap<Relation>();

    // need to build the entire string table first... yuck!

    // get any existing strings from the cleaner tile

    final Object[] keys = geometries.keys;
    final boolean[] allocated = geometries.allocated;
    for (int i = 0; i < allocated.length; i++)
    {
      if (allocated[i])
      {
        final Geometry geometry = (Geometry) (keys[i]);
        for (final Entry<String, String> kv : geometry.getAllAttributes().entrySet())
        {
          stringset.add(kv.getKey());
          stringset.add(kv.getValue());
        }
      }
    }
    // get the fixed initial set
    for (String s: VectorTileCleaner.initialStrings)
    {
      stringset.add(s);
    }

    strings = stringset.toArray(new String[stringset.size()]);

    //ObjectIntOpenHashMap<String> stringlist = buildStringList(stringset);

    final VectorTileCleaner tile = new VectorTileCleaner(strings, nodes, ways, relations);

    //    buildStrings(tile, stringset);
    //    try
    //    {
    //      PrintStream ps = new PrintStream("build-strings.txt");
    //      tile.dump(ps);
    //      ps.close();
    //    }
    //    catch (FileNotFoundException e)
    //    {
    //    }
    //
    //    //tile.organize();
    //
    //    // need to load keys...
    //    strings = tile.strings;
    loadKeys();

    // build the stringlist...
    ObjectIntOpenHashMap<String> stringlist = buildStringList(strings);

    // now we can make nodes, ways, and relations...
    for (int i = 0; i < allocated.length; i++)
    {
      if (allocated[i])
      {
        final Geometry geometry = (Geometry) keys[i];

        switch (geometry.type())
        {
        case COLLECTION:
          addCollection((GeometryCollection) geometry, relationset, wayset,
              nodeset, duplicateNodeset, stringlist);
          break;
        case LINEARRING:
        case LINESTRING:
          addLine((LineString) geometry, wayset, nodeset, duplicateNodeset, stringlist);
          break;
        case POINT:
          addStandalonePoint((Point) geometry, nodeset, duplicateNodeset, stringlist);
          break;
        case POLYGON:
          addPolygon((Polygon) geometry, relationset, wayset, nodeset,
              duplicateNodeset, stringlist);
          break;
        }
      }
    }

    // clear out the geometries
    geometries = new ObjectOpenHashSet<Geometry>();

    buildStrings(tile, stringset);
    buildNodes(tile, nodeset, duplicateNodeset);
    buildWays(tile, wayset);
    buildRelations(tile, relationset);

    tile.organize();

    //     tile.printstats();
    //     tile.dump(System.out);

    strings = tile.strings;
    nodes = tile.nodes;
    ways = tile.ways;
    relations = tile.relations;

    // reload the keys, they may have moved during tile.organize()
    loadKeys();
  }

  private long addCollection(final GeometryCollection collection,
    final LongObjectOpenHashMap<Relation> relationset, final LongObjectOpenHashMap<Way> wayset,
    final LongObjectOpenHashMap<Node> nodeset, final ObjectArrayList<Node> duplicateNodeset,
    final ObjectIntOpenHashMap<String> stringlist)
  {
    return addCollection(collection, collection.getAllAttributes(), relationset,
        wayset, nodeset, duplicateNodeset, stringlist);
  }
  private long addCollection(final GeometryCollection collection, final Map<String, String> attrs,
    final LongObjectOpenHashMap<Relation> relationset, final LongObjectOpenHashMap<Way> wayset,
    final LongObjectOpenHashMap<Node> nodeset, final ObjectArrayList<Node> duplicateNodeset,
    final ObjectIntOpenHashMap<String> stringlist)
  {
    // make it a relation
    final Relation.Builder rb = Relation.newBuilder();

    rb.setId(relationset.size());

    long lastid = 0;
    for (int i = 0; i < collection.getNumGeometries(); i++)
    {
      final Geometry geometry = collection.getGeometry(i);
      if (geometry.type() == Type.POLYGON)
      {
        attrs.put(TYPE, MULTIPOLYGON);
        lastid = addPolygon((Polygon) geometry, rb, lastid, wayset, nodeset,
            duplicateNodeset, stringlist);
      }
      else
      {
        String role = collection.getRole(i);
        if (role == null)
        {
          role = UNKNOWN;
        }
        long id = 0;
        switch (geometry.type())
        {
        case COLLECTION:
          id = addCollection((GeometryCollection) geometry, relationset, wayset,
              nodeset, duplicateNodeset, stringlist);
          rb.addTypes(MemberType.RELATION);
          break;
        case LINEARRING:
        case LINESTRING:
          id = addLine((LineString) geometry, wayset, nodeset, duplicateNodeset,
              stringlist);
          rb.addTypes(MemberType.WAY);
          break;
        case POINT:
          id = addPoint((Point) geometry, nodeset, duplicateNodeset, stringlist);
          rb.addTypes(MemberType.NODE);
          break;
        case POLYGON:
          break;
        }

        rb.addRolesSid(stringlist.get(role));

        rb.addMemids(id - lastid); // DELTA encoded
        lastid = id;
      }
    }

    for (final Entry<String, String> kv : attrs.entrySet())
    {
      rb.addKeys(stringlist.get(kv.getKey()));
      rb.addVals(stringlist.get(kv.getValue()));
    }

    relationset.put(rb.getId(), rb.build());

    return rb.getId();
  }

  private long addLine(final LineString line, final Map<String, String> attrs, final LongObjectOpenHashMap<Way> wayset,
    final LongObjectOpenHashMap<Node> nodeset, final ObjectArrayList<Node> duplicateNodeset,
    final ObjectIntOpenHashMap<String> stringlist)
  {
    final Way.Builder wb = Way.newBuilder();
    wb.setId(wayset.size());

    long lastid = 0;
    for (final Point point : line.getPoints())
    {
      final long hash = point.getGeohashBits();

      final long nodeid;
      if (!nodeset.containsKey(hash))
      {
        nodeid = addPoint(point, nodeset, duplicateNodeset, stringlist);
        // System.out.println("a");
      }
      else
      {
        // System.out.println("e");
        nodeid = nodeset.lget().getId();
      }

      wb.addRefs(nodeid - lastid);
      lastid = nodeid;
    }

    for (final Entry<String, String> kv : attrs.entrySet())
    {
      wb.addKeys(stringlist.get(kv.getKey()));
      wb.addVals(stringlist.get(kv.getValue()));
    }

    wayset.put(wb.getId(), wb.build());

    // System.out.println(wb.getId() + ": " + wayset.size());
    return wb.getId();
  }

  private long addLine(final LineString line, final LongObjectOpenHashMap<Way> wayset,
    final LongObjectOpenHashMap<Node> nodeset, final ObjectArrayList<Node> duplicateNodeset,
    final ObjectIntOpenHashMap<String> stringlist)
  {
    return addLine(line, line.getAllAttributes(), wayset, nodeset,
        duplicateNodeset, stringlist);
  }

  private long addPoint(final Point point, final LongObjectOpenHashMap<Node> nodeset,
      final ObjectArrayList<Node> duplicateNodeset,
    final ObjectIntOpenHashMap<String> stringlist)
  {
    return addPoint(point, point.getAllAttributes(), nodeset, duplicateNodeset, stringlist);
  }

  private Node createPointNode(final long id, final Point point,
      final Map<String, String> attrs, final ObjectIntOpenHashMap<String> stringlist)
  {
    final Node.Builder nb = Node.newBuilder();

    nb.setId(id);

    nb.setLat(toNanoDegrees(point.getY(), granularity, latOffset));
    nb.setLon(toNanoDegrees(point.getX(), granularity, lonOffset));

    if (attrs != null)
    {
      for (final Entry<String, String> kv : attrs.entrySet())
      {
        nb.addKeys(stringlist.get(kv.getKey()));
        nb.addVals(stringlist.get(kv.getValue()));
      }
    }
    return nb.build();
  }

  private long addPoint(final Point point, Map<String, String> attrs,
      final LongObjectOpenHashMap<Node> nodeset,
      final ObjectArrayList<Node> duplicateNodeset,
      final ObjectIntOpenHashMap<String> stringlist)
  {
    final long hash = point.getGeohashBits();

    boolean newNodeHasAttributes = (attrs != null && attrs.size() > 0);
    if (newNodeHasAttributes)
    {
      // Since the new node has attributes, we need to store it.
      Node n = null;
      if (nodeset.containsKey(hash))
      {
        n = nodeset.get(hash);
      }
      if (n != null)
      {
        if (n.getKeysCount() == 0)
        {
          // Existing node has no attributes. Assign the
          // new node's attributes to the existing node.
          Node replaceNode = createPointNode(n.getId(), point, attrs, stringlist);
          nodeset.remove(hash);
          nodeset.put(hash, replaceNode);
          return replaceNode.getId();
        }
        else
        {
          // Existing node already has attributes. Create a duplicate node at
          // the same lat/lon.
          Node newNode = createPointNode(nodeset.size() + duplicateNodeset.size(),
              point, attrs, stringlist);
          duplicateNodeset.add(newNode);
          return newNode.getId();
        }
      }
      else
      {
        // There is no node yet at the same lat/lon, so store it
        Node newNode = createPointNode(nodeset.size() + duplicateNodeset.size(),
            point, attrs, stringlist);
        nodeset.put(hash, newNode);
        return newNode.getId();
      }
    }
    else
    {
      // There are no attributes for the new node.
      if (!nodeset.containsKey(hash))
      {
        Node newNode = createPointNode(nodeset.size() + duplicateNodeset.size(),
            point, null, null);
        nodeset.put(hash, newNode);
        return newNode.getId();
      }
      return nodeset.get(hash).getId();
    }
  }

  private long addPolygon(final LineString line, final Map<String, String> attrs, final LongObjectOpenHashMap<Way> wayset,
    final LongObjectOpenHashMap<Node> nodeset, final ObjectArrayList<Node> duplicateNodeset,
    final ObjectIntOpenHashMap<String> stringlist)
  {
    attrs.put(AREA, YES);
    return addLine(line, attrs, wayset, nodeset, duplicateNodeset, stringlist);
  }

  private long addPolygon(final Polygon polygon, final LongObjectOpenHashMap<Relation> relationset,
    final LongObjectOpenHashMap<Way> wayset, final LongObjectOpenHashMap<Node> nodeset,
    final ObjectArrayList<Node> duplicateNodeset, final ObjectIntOpenHashMap<String> stringlist)
  {
    return addPolygon(polygon, polygon.getAllAttributes(), relationset, wayset,
        nodeset, duplicateNodeset, stringlist);
  }

  private long addPolygon(final Polygon polygon, final Map<String, String> attrs, final LongObjectOpenHashMap<Relation> relationset,
    final LongObjectOpenHashMap<Way> wayset, final LongObjectOpenHashMap<Node> nodeset,
    final ObjectArrayList<Node> duplicateNodeset, final ObjectIntOpenHashMap<String> stringlist)
  {
    if (polygon.getNumInteriorRings() > 0)
    {
      // make it a relation
      final Relation.Builder rb = Relation.newBuilder();

      rb.setId(relationset.size());

      attrs.put(AREA, YES);
      for (final Entry<String, String> kv : attrs.entrySet())
      {
        rb.addKeys(stringlist.get(kv.getKey()));
        rb.addVals(stringlist.get(kv.getValue()));
      }

      // outer ring (don't want it to be a polygon!)
      long id = addLine(polygon.getExteriorRing(), wayset, nodeset,
          duplicateNodeset, stringlist);
      rb.addMemids(id);
      rb.addRolesSid(outerKey); // outer ring
      rb.addTypes(MemberType.WAY);

      long lastid = id;
      for (int i = 0; i < polygon.getNumInteriorRings(); i++)
      {
        id = addLine(polygon.getInteriorRing(i), wayset, nodeset, duplicateNodeset, stringlist);
        rb.addMemids(id - lastid);
        lastid = id;
        rb.addRolesSid(innerKey); // inner ring
        rb.addTypes(MemberType.WAY);
      }
      relationset.put(rb.getId(), rb.build());
      return rb.getId();
    }

    // make it a closed way
    return addPolygon(polygon.getExteriorRing(), attrs, wayset, nodeset,
        duplicateNodeset, stringlist);
  }

  private long addPolygon(final Polygon polygon, Relation.Builder builder, long lastid,
    final LongObjectOpenHashMap<Way> wayset, final LongObjectOpenHashMap<Node> nodeset,
    final ObjectArrayList<Node> duplicateNodeset, final ObjectIntOpenHashMap<String> stringlist)
  {
    // outer ring (don't want it to be a polygon!)
    long id = addLine(polygon.getExteriorRing(), wayset, nodeset,
        duplicateNodeset, stringlist);
    builder.addMemids(id - lastid);
    lastid = id;
    builder.addRolesSid(outerKey); // outer ring
    builder.addTypes(MemberType.WAY);

    for (int i = 0; i < polygon.getNumInteriorRings(); i++)
    {
      id = addLine(polygon.getInteriorRing(i), wayset, nodeset,
          duplicateNodeset, stringlist);
      builder.addMemids(id - lastid);
      lastid = id;
      builder.addRolesSid(innerKey); // inner ring
      builder.addTypes(MemberType.WAY);
    }

    return lastid;
  }

  private long addStandalonePoint(final Point point, final LongObjectOpenHashMap<Node> nodeset,
      final ObjectArrayList<Node> duplicateNodeset, final ObjectIntOpenHashMap<String> stringlist)
  {
    final Map<String, String> attrs = point.getAllAttributes();
    attrs.put(SINGLE_POINT, YES);

    return addPoint(point, nodeset, duplicateNodeset, stringlist);
  }

  private void init()
  {
    granularity = 100;
    lonOffset = 0;
    latOffset = 0;
    dateGranularity = 1000;
  }
  
  void setGranularity(int granularity)
 {
   this.granularity = granularity;
 }

  void setDateGranularity(int dateGranularity)
 {
   this.dateGranularity = dateGranularity;
 }

  void setLonOffset(long lonOffset)
 {
   this.lonOffset = lonOffset;
 }

  void setLatOffset(long latOffset)
 {
   this.latOffset = latOffset;
 }

}
