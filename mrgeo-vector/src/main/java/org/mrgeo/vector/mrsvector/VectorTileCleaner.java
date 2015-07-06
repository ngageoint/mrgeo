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

import com.carrotsearch.hppc.LongLongOpenHashMap;
import com.carrotsearch.hppc.ObjectIntOpenHashMap;
import com.carrotsearch.hppc.cursors.ObjectIntCursor;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import org.apache.commons.lang.ArrayUtils;
import org.mrgeo.vector.mrsvector.pbf.FileFormat.BlobHeader;
import org.mrgeo.vector.mrsvector.pbf.OsmFormat.*;
import org.mrgeo.vector.mrsvector.pbf.OsmFormat.Relation.MemberType;
import org.mrgeo.data.raster.RasterUtils;

import java.io.*;
import java.util.*;

class VectorTileCleaner extends VectorTile
{

  // keys for defining what is a polygon. The format is
  // "<key>|<inclusion1>,<inclusion2>...|<exclusion1>,<exclusion2>,...
  // Values were taken from: http://wiki.openstreetmap.org/wiki/Overpass_turbo/Polygon_Features
  //  static final String[] POLYGONS = { "building||", "highway|services,rest_area,escape|",
  //    "natural||coastline,ridge,arete,tree_row", "landuse||",
  //    "waterway|riverbank,dock,boatyard,dam|", "amenity||", "leisure||",
  //    "barrier|city_wall,ditch,hedge,retaining_wall,wall,spikes|",
  //    "railway|station,turntable,roundhouse,platform|", "boundary||",
  //    "man_made||cutline,embankment,pipeline", "power|generator,station,sub_station,transformer|",
  //    "place||", "shop||", "aeroway||taxiway", "tourism||", "historic||", "public_transport||",
  //    "office||", "building:part||", "ruins||", "area:highway||", "craft||", };

  static final String[] POLYGONS = { "building||", "highway|services,rest_area,escape|",
    "natural||coastline,ridge,arete,tree_row", "landuse||",
    "waterway|riverbank,dock,boatyard,dam|", "amenity||", "leisure||",
    "barrier|city_wall,ditch,hedge,retaining_wall,wall,spikes|",
    "railway|station,turntable,roundhouse,platform|",
    "man_made||cutline,embankment,pipeline", "power|generator,station,sub_station,transformer|",
    "place||", "shop||", "aeroway||taxiway", "tourism||", "historic||", "public_transport||",
    "office||", "building:part||", "ruins||", "area:highway||", "craft||", };

  private int[] polygons = null;
  private int[][] inclusions = null;
  private int[][] exclusions = null;

  int[] chunkstringmap = null; // maps the old stringtable key (offset) to the new stringtable entry
  ObjectIntOpenHashMap<String> stringmap = null; // maps the old stringtable offset to new stringtable offset

  LongLongOpenHashMap nodemap = null; // maps old node id to new node id
  LongLongOpenHashMap waymap = null; // maps old way id to new way id
  LongLongOpenHashMap relationmap = null; // maps old relation id to new relation id
  Way[] orphanWays = null;

  Relation[] orphanRelations = null;

  // this is kinda hacky, but we need to make sure these values are in the string table...
  // make sure "" is the 1st entry
  final static String[] initialStrings = new String[] {"", SINGLE_POINT, AREA, INNER_RING, INRELATION, 
    OUTER_RING, YES, NO, TYPE, MULTIPOLYGON, UNKNOWN};

  VectorTileCleaner()
  {
    strings = (String[]) ArrayUtils.clone(initialStrings);
    init();
  }

  private void init()
  {
    stringmap = new ObjectIntOpenHashMap<String>(10000);

    for (int i = 0; i < strings.length; i++)
    {
      stringmap.put(strings[i], i);
    }

    nodemap = new LongLongOpenHashMap(10000);
    waymap = new LongLongOpenHashMap(10000);
    relationmap = new LongLongOpenHashMap(10000);
    orphanRelations = new Relation[0];
    orphanWays = new Way[0];
  }

  VectorTileCleaner( String[] strings)
  {
    this(strings, null, null, null);
  }

  VectorTileCleaner( String[] strings, Node[] nodes, Way[] ways,  Relation[] relations)
  {
    this.strings = strings;
    this.nodes = nodes;
    this.ways = ways;
    this.relations = relations;

    init();
  }

  @Override
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
    long id = 0;

    long offset = nodes == null ? 0 : nodes.length;
    for (int i = 0; i < count; i++)
    {
      nb.clear();
      // these are DELTA coded, so we need to keep a running count...
      id += dense.getId(i);
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

          nb.addKeys(chunkstringmap[k]);
          nb.addVals(chunkstringmap[v]);

          k = dense.getKeysVals(kvoffset++);
        }
      }

      nodemap.put(id, offset);

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

  @Override
  protected void parseNodes(final List<Node> nodesList)
  {
    // we're dirty...
    final ArrayList<Node> newNodes = new ArrayList<Node>(nodesList.size());
    final Node.Builder nb = Node.newBuilder();

    long offset = nodes == null ? 0 : nodes.length;
    for (final Node node : nodesList)
    {
      nb.clear();
      nb.setId(offset);
      nb.setInfo(node.getInfo());
      nb.setLat(node.getLat());
      nb.setLon(node.getLon());
      nb.setUnknownFields(node.getUnknownFields());

      nodemap.put(node.getId(), offset);

      // TODO: Add the original id as a k/v? (that will balloon the string table immensely)
      for (final int key : node.getKeysList())
      {
        nb.addKeys(chunkstringmap[key]);
      }
      for (final int val : node.getValsList())
      {
        nb.addVals(chunkstringmap[val]);
      }

      offset++;

      newNodes.add(nb.build());
    }
    nodes = (Node[]) ArrayUtils.addAll(nodes, newNodes.toArray(new Node[0]));
  }

  @Override
  protected void parseRelations(final List<Relation> relationsList)
  {
    final ArrayList<Relation> newRelations = new ArrayList<Relation>();
    final Relation.Builder rb = Relation.newBuilder();

    long offset = relations == null ? 0 : relations.length;

    for (final Relation relation : relationsList)
    {

      rb.clear();
      rb.setId(offset);
      rb.setInfo(relation.getInfo());

      long lastid = 0;

      long id = 0;
      for (int k = 0; k < relation.getMemidsCount(); k++)
      {
        id += relation.getMemids(k);  // DELTA encoding

        MemberType type = relation.getTypes(k);
        long memid = 0;
        switch (type)
        {
        case NODE:
          if (nodemap.containsKey(id))
          {
            memid = nodemap.lget();
          }
          break;
        case RELATION:
          if (relationmap.containsKey(id))
          {
            memid = relationmap.lget();
          }
          break;
        case WAY:
          if (waymap.containsKey(id))
          {
            memid = waymap.lget();
          }
          break;
        default:
          break;

        }

        if (memid > 0)
        {
          rb.addTypes(type);
          rb.addRolesSid(chunkstringmap[relation.getRolesSid(k)]);
          rb.addMemids(memid - lastid);
          lastid = memid;
        }
      }

      if (rb.getMemidsCount() > 0)
      {
        // TODO: Add the original id as a k/v? (that will balloon the string table immensely)
        for (final int key : relation.getKeysList())
        {
          rb.addKeys(chunkstringmap[key]);
        }
        for (final int val : relation.getValsList())
        {
          rb.addVals(chunkstringmap[val]);
        }

        rb.setUnknownFields(relation.getUnknownFields());
        newRelations.add(rb.build());

        relationmap.put(relation.getId(), offset);

        offset++;
      }
      else
      {

      }
    }
    relations = (Relation[]) ArrayUtils.addAll(relations, newRelations.toArray(new Relation[0]));
  }

  @Override
  protected void parseStrings(final StringTable stringtable)
  {
    // System.out.println("  strings (" + st.getSCount() + ")");

    chunkstringmap = new int[stringtable.getSCount()];

    final ArrayList<String> newStrings = new ArrayList<String>();

    int offset = strings == null ? 0 : strings.length;
    for (int i = 0; i < stringtable.getSCount(); i++)
    {
      final String str = stringtable.getS(i).toStringUtf8();
      if (!stringmap.containsKey(str))
      {
        stringmap.put(str, offset);
        chunkstringmap[i] = offset;

        offset++;

        newStrings.add(str);
      }
      else
      {
        chunkstringmap[i] = stringmap.get(str);
      }
    }
    strings = (String[]) ArrayUtils.addAll(strings, newStrings.toArray(new String[0]));
  }

  @Override
  protected void parseWays(final List<Way> waysList)
  {
    final ArrayList<Way> newWays = new ArrayList<Way>();
    final Way.Builder wb = Way.newBuilder();

    long offset = ways == null ? 0 : ways.length;
    boolean orphan;

    for (final Way way : waysList)
    {
      orphan = false;

      initWay(wb, offset, way);

      long id = 0;
      long newid = 0;
      long lastid = 0;
      for (final long ref : way.getRefsList())
      {
        id += ref; // DELTA encoding

        if (!nodemap.containsKey(id))
        {
          //          orphanWays = (Way[]) ArrayUtils.add(orphanWays, way);
          //
          //          if (wb.getRefsCount() >= 2)
          //          {
          //            buildWay(newWays, wb, way);
          //            offset++;
          //
          //            initWay(wb, offset, way);
          //          }
          //          else if (wb.getRefsCount() > 0)
          //          {
          //            initWay(wb, offset, way);
          //          }
          //          lastid = 0;
          //          newid = 0;
          //orphan = true;
          //break;
        }
        else
        {
          // get the new node is and set it in the way
          newid = nodemap.lget();
          wb.addRefs(newid - lastid);
          lastid = newid;
        }
      }

      if (orphan)
      {
        continue;
      }

      if (wb.getRefsCount() >= 2)
      {
        buildWay(newWays, wb, way);
      }

      offset++;
    }
    ways = (Way[]) ArrayUtils.addAll(ways, newWays.toArray(new Way[0]));

  }

  private void initWay(final Way.Builder wb, long offset, final Way way)
  {
    wb.clear();
    wb.setId(offset);

    if (way.hasBbox())
    {
      wb.setBbox(way.getBbox());
    }

    wb.setInfo(way.getInfo());
  }

  private void buildWay(final ArrayList<Way> newWays, final Way.Builder wb, final Way way)
  {
    // TODO: Add the original id as a k/v? (that will balloon the string table immensely)
    for (final int key : way.getKeysList())
    {
      wb.addKeys(chunkstringmap[key]);
    }
    for (final int val : way.getValsList())
    {
      wb.addVals(chunkstringmap[val]);
    }

    wb.setUnknownFields(way.getUnknownFields());

    newWays.add(wb.build());

    waymap.put(way.getId(), wb.getId());
  }

  private boolean checkForPolygon(final List<Integer> keys, final List<Integer> values)
  {
    if (polygons != null)
    {
      for (int i = 0; i < polygons.length; i++)
      {
        final int key = polygons[i];
        // do we have a polygon key?
        final int ndx = findKey(key, keys);
        if (ndx >= 0)
        {
          final int val = findValue(ndx, values);

          // if the value "no" (not a polygon)
          if (isFalse(val))
          {
            return false;
          }
          // is the value in the inclusion list (polygon if true)
          if (inclusions[i] != null)
          {
            for (final int incl : inclusions[i])
            {
              if (val == incl)
              {
                // in the inclusion list (polygon!)
                return true;
              }
            }
            // not in the inclusion list (not a polygon)
            return false;
          }
          if (exclusions[i] != null)
          {
            // is the value in the exclusion list (not a polygon)
            for (final int excl : exclusions[i])
            {
              if (val == excl)
              {
                // in the exclusion list (not a polygon)
                return false;
              }
            }
            // not in the exclusion list (polygon!)
            return true;
          }

          return true;
        }
      }
    }
    // not in any list, not a polygon
    return false;
  }

  private void checkforStandalonePoints()
  {
    if (nodes != null)
    {
      // this is kinda messy, but we need to loop through all the points, then see if they are part
      // a way or relation...
      final boolean[] referenced = new boolean[nodes.length];
      Arrays.fill(referenced, true);

      if (ways != null)
      {
        // first loop through the ways set refs to false
        for (final Way way : ways)
        {
          long id = 0;
          for (final long ref : way.getRefsList())
          {
            id += ref;   // DELTA encoding
            referenced[(int) id] = false;
          }
        }
      }

      if (relations != null)
      {
        // now look at relations (only type NODE) and set them to false
        for (final Relation relation : relations)
        {
          final List<MemberType> types = relation.getTypesList();
          final List<Long> memids = relation.getMemidsList();

          long id = 0;
          for (int i = 0; i < relation.getTypesCount(); i++)
          {
            id += memids.get(i);  // DELTA encoding
            if (types.get(i) == MemberType.NODE)
            {
              referenced[(int) id] = false;
            }
          }
        }
      }

      // anything true is a stand-alone point
      for (int i = 0; i < referenced.length; i++)
      {
        if (referenced[i])
        {
          final Node node = nodes[i];
          if (!hasKeyValue(node, singlePointKey, yesKey))
          {
            final Node.Builder nb = Node.newBuilder(nodes[i]);
            nb.addKeys(singlePointKey);
            nb.addVals(yesKey);

            nodes[i] = nb.build();
          }
        }
      }
    }
  }

  private Relation checkRelationForArea(final Relation relation)
  {
    // 1st check if the area key is already set
    if (!isArea(relation))
    {
      // simply check for the "type=multipolygon" key/value
      if (hasKeyValue(relation, typeKey, multipolygonKey))
      {
        // passed the area test, set the key/val to "area=yes"
        final Relation.Builder rb = Relation.newBuilder(relation);
        rb.addKeys(arealKey);
        rb.addVals(yesKey);

        return rb.build();
      }
    }
    return relation;
  }

  private Way checkWayForArea(final Way way)
  {
    // 1st check if the area key is already set
    if (way.getRefsCount() >= 3 && !isArea(way))
    {
      if (checkForPolygon(way.getKeysList(), way.getValsList()))
      {
        // passed the area test, set the key/val to "area=yes"
        final Way.Builder wb = Way.newBuilder(way);
        wb.addKeys(arealKey);
        wb.addVals(yesKey);

        // check to make sure the way is closed
        final List<Long> refs = way.getRefsList();
        final long a = refs.get(0);
        long id = 0;
        for (long ref: refs)
        {
          id += ref;
        }
        final long b = id;

        if (a != b)
        {
          //          id = 0;
          //          for (long ref: refs)
          //          {
          //            id += ref;
          //            System.out.print(id + " ");
          //          }
          //          System.out.println();
          //          for (int i = 0; i < wb.getKeysCount(); i++)
          //          {
          //            int key = wb.getKeys(i);
          //            int val = wb.getVals(i);
          //            System.out.print(strings[key] + ":" + strings[val] + " ");
          //          }
          //          System.out.println();

          wb.addRefs(a - b);
        }

        if (way.getRefsCount() >= 4)
        {
          return wb.build();
        }
      }
    }
    return way;
  }

  private void loadPolygons()
  {
    final ArrayList<Integer> polys = new ArrayList<Integer>();
    final ArrayList<ArrayList<Integer>> incs = new ArrayList<ArrayList<Integer>>();
    final ArrayList<ArrayList<Integer>> excl = new ArrayList<ArrayList<Integer>>();
    for (final String str : POLYGONS)
    {
      final String[] split = str.split("\\|", 3);
      final String key = split[0];
      final String[] in = split[1].split(",");
      final String[] ex = split[2].split(",");

      int ndx = findString(key);
      if (ndx >= 0)
      {
        polys.add(findString(key));

        ArrayList<Integer> list = new ArrayList<Integer>();
        boolean haveinclusion = false;
        for (final String s : in)
        {
          if (s.length() > 0)
          {
            haveinclusion = true;
            ndx = findString(s);
            if (ndx >= 0)
            {
              list.add(findString(s));
            }
          }
        }
        // if we didn't find any keys matching the inclusion, we need to add a "dummy", so the 
        // searches still work
        if (haveinclusion && list.size() == 0)
        {
          list.add(Integer.MIN_VALUE);
        }
        incs.add(list);

        list = new ArrayList<Integer>();
        boolean haveexclusion = false;

        for (final String s : ex)
        {
          if (s.length() > 0)
          {
            haveexclusion = true;
            ndx = findString(s);
            if (ndx >= 0)
            {
              list.add(findString(s));
            }
          }
        }

        // if we didn't find any keys matching the exclusion, we need to add a "dummy", so the 
        // searches still work
        if (haveexclusion && list.size() == 0)
        {
          list.add(Integer.MIN_VALUE);
        }

        excl.add(list);
      }
    }

    polygons = ArrayUtils.toPrimitive(polys.toArray(new Integer[0]));
    inclusions = new int[polygons.length][];
    exclusions = new int[polygons.length][];

    for (int i = 0; i < polygons.length; i++)
    {
      ArrayList<Integer> list = incs.get(i);
      if (list.size() > 0)
      {
        inclusions[i] = ArrayUtils.toPrimitive(list.toArray(new Integer[0]));
      }
      else
      {
        inclusions[i] = null;
      }
      list = excl.get(i);

      if (list.size() > 0)
      {
        exclusions[i] = ArrayUtils.toPrimitive(list.toArray(new Integer[0]));
      }
      else
      {
        exclusions[i] = null;
      }
    }
  }

  private void sortNodes()
  {
    if (nodes != null)
    {
      // update the nodes with the new string values...
      final Node[] newNodes = new Node[nodes.length];
      int offset = 0;
      final Node.Builder nb = Node.newBuilder();
      for (final Node node : nodes)
      {
        nb.clear();

        nb.setId(node.getId());
        nb.setInfo(node.getInfo());
        nb.setLat(node.getLat());
        nb.setLon(node.getLon());

        final Map<Integer, Integer> sortedKeys = new TreeMap<Integer, Integer>();
        for (int i = 0; i < node.getKeysCount(); i++)
        {
          sortedKeys.put(chunkstringmap[node.getKeys(i)], chunkstringmap[node.getVals(i)]);
        }
        nb.addAllKeys(sortedKeys.keySet());
        nb.addAllVals(sortedKeys.values());

        nb.setUnknownFields(node.getUnknownFields());
        newNodes[offset++] = nb.build();
      }
      nodes = newNodes;
    }
  }

  private void sortRelations()
  {
    if (relations != null)
    {
      final Relation[] newRelations = new Relation[relations.length];
      final Relation.Builder rb = Relation.newBuilder();

      int offset = 0;
      for (final Relation relation : relations)
      {
        rb.clear();

        rb.setId(relation.getId());
        rb.setInfo(relation.getInfo());
        rb.addAllMemids(relation.getMemidsList());
        rb.addAllTypes(relation.getTypesList());

        for (final int i : relation.getRolesSidList())
        {
          rb.addRolesSid(chunkstringmap[i]);
        }

        final Map<Integer, Integer> sortedKeys = new TreeMap<Integer, Integer>();
        for (int i = 0; i < relation.getKeysCount(); i++)
        {
          sortedKeys.put(chunkstringmap[relation.getKeys(i)], chunkstringmap[relation.getVals(i)]);
        }
        rb.addAllKeys(sortedKeys.keySet());
        rb.addAllVals(sortedKeys.values());

        rb.setUnknownFields(relation.getUnknownFields());

        newRelations[offset++] = rb.build();
      }

      relations = newRelations;
    }
  }

  private void sortStrings()
  {
    // sort the strings using a treemap, so we can keep the old index around for mapping...
    final Map<String, Integer> sortedStrings = new TreeMap<String, Integer>();
    for (int i = 0; i < strings.length; i++)
    {
      sortedStrings.put(strings[i], i);
    }

    strings = sortedStrings.keySet().toArray(new String[0]);

    chunkstringmap = new int[strings.length];
    int newndx = 0;
    for (final Map.Entry<String, Integer> entry : sortedStrings.entrySet())
    {
      chunkstringmap[entry.getValue()] = newndx++;
    }

    sortNodes();
    sortWays();
    sortRelations();
  }

  private void sortWays()
  {
    if (ways != null)
    {
      final Way[] newWays = new Way[ways.length];
      int offset = 0;
      final Way.Builder wb = Way.newBuilder();
      for (final Way way : ways)
      {
        wb.clear();

        wb.setId(way.getId());
        wb.setInfo(way.getInfo());

        if (way.hasBbox())
        {
          wb.setBbox(way.getBbox());
        }

        wb.addAllRefs(way.getRefsList());
        wb.setUnknownFields(way.getUnknownFields());

        final Map<Integer, Integer> sortedKeys = new TreeMap<Integer, Integer>();
        for (int i = 0; i < way.getKeysCount(); i++)
        {
          sortedKeys.put(chunkstringmap[way.getKeys(i)], chunkstringmap[way.getVals(i)]);
        }
        wb.addAllKeys(sortedKeys.keySet());
        wb.addAllVals(sortedKeys.values());

        newWays[offset++] = wb.build();
      }

      ways = newWays;
    }
  }

  void addNodes(final List<Node> nodesList)
  {
    parseNodes(nodesList);
  }

  void addRelations(final List<Relation> relationsList)
  {
    parseRelations(relationsList);
  }

  void addStrings(final List<String> stringList)
  {
    final StringTable.Builder sb = StringTable.newBuilder();
    for (final String str : stringList)
    {
      sb.addS(ByteString.copyFromUtf8(str));
    }
    parseStrings(sb.build());
  }

  void addWays(final List<Way> waysList)
  {
    parseWays(waysList);
  }

  void organize()
  {
    sortStrings();

    // now all the data is in order, we can check and set specific area features
    loadKeys();
    loadPolygons();

    if (ways != null)
    {
      for (int i = 0; i < ways.length; i++)
      {
        ways[i] = checkWayForArea(ways[i]);
      }
    }

    if (relations != null)
    {
      typeKey = findString("type");
      multipolygonKey = findString("multipolygon");
      for (int i = 0; i < relations.length; i++)
      {
        relations[i] = checkRelationForArea(relations[i]);

        Relation r = relations[i];
        int id = 0;
        for (int j = 0; j < r.getMemidsCount(); j++)
        {
          id += r.getMemids(j);  // DELTA encoding

          if (r.getTypes(j) == MemberType.NODE)
          {
            if (id < nodes.length)
            {
              nodes[id] = setIsInRelation(nodes[id]);
            }
          }
          else if (r.getTypes(j) == MemberType.WAY)
          {
            if (id < ways.length)
            {
              ways[id] = setIsInRelation(ways[id]);
            }
          }
          else if (r.getTypes(j) == MemberType.RELATION)
          {
            if (id < relations.length)
            {
              relations[id] = setIsInRelation(relations[id]);
            }
          }
        }
      }
    }

    checkforStandalonePoints();
  }

  private Relation setIsInRelation(Relation relation)
  {
    if (!hasKey(inRelationKey, relation.getKeysList()))
    {
      final Relation.Builder rb = Relation.newBuilder(relation);
      rb.addKeys(inRelationKey);
      rb.addVals(yesKey);

      return rb.build();
    }

    return relation;
  }

  private Node setIsInRelation(Node node)
  {
    if (!hasKey(inRelationKey, node.getKeysList()))
    {
      final Node.Builder nb = Node.newBuilder(node);
      nb.addKeys(inRelationKey);
      nb.addVals(yesKey);

      return nb.build();
    }

    return node;
  }

  private Way setIsInRelation(Way way)
  {
    if (!hasKey(inRelationKey, way.getKeysList()))
    {
      // passed the area test, set the key/val to "area=yes"
      final Way.Builder wb = Way.newBuilder(way);
      wb.addKeys(inRelationKey);
      wb.addVals(yesKey);

      return wb.build();
    }

    return way;
  }

  @Override
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

    System.out.println("cleaner: ");
    {
      size = 0;
      for (final ObjectIntCursor<String> c : stringmap)
      {
        size += c.key.length() + RasterUtils.INT_BYTES;
      }

      totalsize += size;
      System.out.println("  stringtable: " + stringmap.size() + " (" + human(size) + ")");
    }
    {
      try
      {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        final ObjectOutputStream out = new ObjectOutputStream(bytes);
        out.writeObject(chunkstringmap);

        size = bytes.size();
        totalsize += size;
        System.out.println("  stringmap: " + chunkstringmap.length + " (" + human(size) + ")");
      }
      catch (final IOException e)
      {
        e.printStackTrace();
      }
    }

    {
      size = nodemap.size() * 2 * RasterUtils.LONG_BYTES;
      totalsize += size;
      System.out.println("  nodemap: " + nodemap.size() + " (" + human(size) + ")");
    }

    {
      size = waymap.size() * 2 * RasterUtils.LONG_BYTES;
      totalsize += size;
      System.out.println("  waymap: " + waymap.size() + " (" + human(size) + ")");
    }
    {
      size = relationmap.size() * 2 * RasterUtils.LONG_BYTES;
      totalsize += size;
      System.out.println("  relationmap: " + relationmap.size() + " (" + human(size) + ")");
    }
    {
      try
      {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        final ObjectOutputStream out = new ObjectOutputStream(bytes);
        out.writeObject(orphanWays);

        size = bytes.size();
        totalsize += size;
        System.out.println("  orphaned ways: " + orphanWays.length + " (" + human(size) + ")");
      }
      catch (final IOException e)
      {
        e.printStackTrace();
      }
    }
    {
      try
      {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        final ObjectOutputStream out = new ObjectOutputStream(bytes);
        out.writeObject(orphanRelations);

        size = bytes.size();
        totalsize += size;
        System.out.println("  orphaned relations: " + orphanRelations.length + " (" + human(size) +
            ")");
      }
      catch (final IOException e)
      {
        e.printStackTrace();
      }
    }
    System.out.println("total size: " + human(totalsize));
  }

  void read(final InputStream stream) throws IOException
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
      // final InputStream blob =;
      final CodedInputStream blob = CodedInputStream.newInstance(parseBlob(dis, header));

      // for speed, we make _large_ tiles, we don't want protobuf to complain...
      blob.setSizeLimit(MAX_STREAM_SIZE);

      if (header.getType().equals(OSMDATA))
      {
        parseOSM(blob);
      }
    }

    organize();
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
