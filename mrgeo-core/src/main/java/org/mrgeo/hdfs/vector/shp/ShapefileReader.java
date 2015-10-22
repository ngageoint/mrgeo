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

package org.mrgeo.hdfs.vector.shp;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mrgeo.geometry.*;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.hdfs.vector.GeometryInputStream;
import org.mrgeo.hdfs.vector.ShapefileGeometryCollection;
import org.mrgeo.hdfs.vector.shp.dbase.DbaseException;
import org.mrgeo.hdfs.vector.shp.esri.ESRILayer;
import org.mrgeo.hdfs.vector.shp.esri.FormatException;
import org.mrgeo.hdfs.vector.shp.esri.geom.*;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Iterator;
import java.util.List;

/**
 * Reads a Shapefile as an InputStream (no seeking). This implementation does
 * not read the DBF at this time.
 * 
 * Limitations: The Shapefile may not contain "skipped" records as a result of
 * editing. The Shapefile may not have polygons with holes. There are probably 
 * other scenarios that this Shapefile reader will not handle properly.
 * 
 * @author jason.surratt
 * 
 */
public class ShapefileReader implements GeometryInputStream, ShapefileGeometryCollection
{
  static class LocalIterator implements Iterator<WritableGeometry>
  {
    private int currentIndex = 0;
    private ShapefileReader parent;

    public LocalIterator(ShapefileReader parent)
    {
      this.parent = parent;
    }

    @Override
    public boolean hasNext()
    {
      return currentIndex < parent.size();
    }

    @Override
    public WritableGeometry next()
    {
      return parent.get(currentIndex++);
    }

    @Override
    public void remove()
    {
      throw new UnsupportedOperationException();
    }
  }

  static class ReadOnlyLocalIterator implements Iterator<Geometry>
  {
    private int currentIndex = 0;
    private ShapefileReader parent;

    public ReadOnlyLocalIterator(ShapefileReader parent)
    {
      this.parent = parent;
    }

    @Override
    public boolean hasNext()
    {
      return currentIndex < parent.size();
    }

    @Override
    public Geometry next()
    {
      return parent.get(currentIndex++);
    }

    @Override
    public void remove()
    {
      throw new UnsupportedOperationException();
    }
  }

  private enum Source {
    FILE, HDFS, INVALID
  }

  private static final long serialVersionUID = 1L;

  private static WritableGeometry convertToGeometry(JShape shape)
  {
    WritableGeometry result;

    switch (shape.getType())
    {
    case JShape.POINT:
      JPoint p = (JPoint) shape;
      result = GeometryFactory.createPoint(p.getX(), p.getY());
      break;
    case JShape.POINTZ:
      JPointZ pz = (JPointZ) shape;
      result = GeometryFactory.createPoint(pz.getX(), pz.getY(), pz.getZ());
      break;
    case JShape.POLYGON:
      JPolygon poly = (JPolygon) shape;
      result = convertToPolygon(poly);
      break;
    case JShape.POLYGONZ:
      JPolygonZ polyZ = (JPolygonZ) shape;
      result = convertToPolygon(polyZ);
      break;
    case JShape.POLYLINE:
      JPolyLine line = (JPolyLine) shape;
      result = convertToLineString(line);
      break;
    case JShape.POLYLINEZ:
      JPolyLineZ lineZ = (JPolyLineZ) shape;
      result = convertToLineString(lineZ);
      break;
    default:
      throw new IllegalArgumentException("Unsupported geometry type.");
    }

    return result;
  }

  private static WritableLineString convertToLineString(JPolyLine line)
  {
    WritableLineString result = GeometryFactory.createLineString();
    for (int i = 0; i < line.getPointCount(); i++)
    {
      Coord c = line.getPoint(i);
      result.addPoint(GeometryFactory.createPoint(c.x, c.y));
    }

    return result;
  }

  private static WritableLineString convertToLineString(JPolyLineZ line)
  {
    WritableLineString result = GeometryFactory.createLineString();
    for (int i = 0; i < line.getPointCount(); i++)
    {
      Coord c = line.getPoint(i);
      result.addPoint(GeometryFactory.createPoint(c.x, c.y));
    }

    return result;
  }

  private static WritableGeometry convertToPolygon(JPolygon poly)
  {
    WritableGeometry result = null;
    WritableGeometryCollection gc = null;
    if (poly.getPartCount() > 1)
    {
      gc = GeometryFactory.createGeometryCollection();
      result = gc;
    }
    for (int p = 0; p < poly.getPartCount(); p++)
    {
      WritablePolygon r = GeometryFactory.createPolygon();
      int part = poly.getPart(p);
      int end;
      if (p == poly.getPartCount() - 1)
      {
        end = poly.getPointCount();
      }
      else
      {
        end = poly.getPart(p + 1);
      }
      WritableLinearRing exteriorRing = GeometryFactory.createLinearRing();
      for (int i = part; i < end; i++)
      {
        Coord c = poly.getPoint(i);
        exteriorRing.addPoint(GeometryFactory.createPoint(c.x, c.y));
      }
      r.setExteriorRing(exteriorRing);

      // if there is only one exterior ring
      if (gc == null)
      {
        result = r;
      }
      // else if there is more than one exterior ring
      else
      {
        gc.addGeometry(r);
      }
    }

    return result;
  }

  private static WritablePolygon convertToPolygon(JPolygonZ poly)
  {
    if (poly.getPartCount() == 0)
    {
      return null;
    }
    WritablePolygon result = GeometryFactory.createPolygon();
    for (int part=0; part < poly.getPartCount(); part++)
    {
      WritableLinearRing ring = GeometryFactory.createLinearRing();
      int maxPointIndex = (part == poly.getPartCount() - 1) ? poly.getPointCount() : poly.getPart(part + 1);
      for (int i = poly.getPart(part); i < maxPointIndex; i++)
      {
        Coord c = poly.getPoint(i);
        ring.addPoint(GeometryFactory.createPoint(c.x, c.y, poly.getZ(i)));
      }
      if (part == 0)
      {
        result.setExteriorRing(ring);
      }
      else
      {
        result.addInteriorRing(ring);
      }
    }

    return result;
  }

  transient int currentIndex = 0;

  // only one of these should be set at any time. If path is set, use Hadoop,
  // otherwise use
  // standard files.
  String fileName = null;

  transient ESRILayer shpFile;

  Source source = Source.INVALID;

  public ShapefileReader(Path path) throws IOException
  {
    load(path);
  }

  /**
   * Unfortunately Shapefiles require random access to multiple files. At this
   * point we will assume that the files are on the local filesystem, not HDFS.
   * 
   * @param shpFilename
   * @throws IOException
   */
  public ShapefileReader(String shpFilename) throws IOException
  {
    load(shpFilename);
  }

  @Override
  public WritableGeometry get(int index)
  {
    if (index >= shpFile.getNumRecords() || index < 0)
    {
      throw new IllegalArgumentException(String.format("Index out of range. (%d, %d)", index,
          shpFile.getNumRecords()));
    }
    WritableGeometry g = convertToGeometry(shpFile.getShape(index));
    List<?> attributes;
    try
    {
      attributes = shpFile.getTable().getRow(index);
    }
    catch (Exception e)
    {
      e.printStackTrace();
      return null;
    }
    String[] columns = shpFile.getDataColumns();
    for (int i = 0; i < attributes.size(); i++)
    {
      Object a = attributes.get(i);
      if (a != null) {
        g.setAttribute(columns[i], a.toString());
      }
      else {
        g.setAttribute(columns[i], null);
      }
    }
    return g;
  }



  @Override
  public String getProjection()
  {
    return shpFile.getProjection();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.spadac.Geometry.GeometryInputStream#hasNext()
   */
  @Override
  public boolean hasNext()
  {
    return (currentIndex < shpFile.getNumRecords());
  }

  @Override
  public Iterator<WritableGeometry> iterator()
  {
    return new LocalIterator(this);
  }

  public Iterator<Geometry> readOnlyIterator()
  {
    return new ReadOnlyLocalIterator(this);
  }

  private void load(Path path) throws IOException
  {
    fileName = path.toString();
    source = Source.HDFS;
    currentIndex = 0;
    try
    {
      String baseName = ESRILayer.getBaseName(path.toString());
      FileSystem fs = HadoopFileUtils.getFileSystem(path);
      SeekableHdfsInput shp = new SeekableHdfsInput(path);
      SeekableHdfsInput shx = new SeekableHdfsInput(new Path(baseName + ".shx"));
      SeekableHdfsInput dbf = new SeekableHdfsInput(new Path(baseName + ".dbf"));
      FSDataInputStream prj = fs.open(new Path(baseName + ".prj"));
      try
      {
        shpFile = ESRILayer.open(shp, shx, dbf, prj);
      }
      finally
      {
        prj.close();
      }
    }
    catch (FormatException e)
    {
      throw new IOException("Shapefile format error", e);
    }
    catch (DbaseException e)
    {
      throw new IOException("Error reading shapefile", e);
    }

    reset();
  }

  private void load(String shpFilename) throws IOException
  {
    fileName = shpFilename;
    source = Source.FILE;
    currentIndex = 0;
    try
    {
      shpFile = ESRILayer.open(shpFilename);
    }
    catch (FormatException e)
    {
      throw new IOException("Shapefile format error", e);
    }
    catch (DbaseException e)
    {
      throw new IOException("Error reading shapefile", e);
    }

    reset();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.spadac.Geometry.GeometryInputStream#next()
   */
  @Override
  public WritableGeometry next()
  {
    if (currentIndex < shpFile.getNumRecords())
    {
      return get(currentIndex++);
    }
      
    return null;
  }

  private void readObject(ObjectInputStream is) throws ClassNotFoundException, IOException
  {
    // always perform the default de-serialization first
    is.defaultReadObject();

    if (source == Source.FILE)
    {
      load(fileName);
    }
    else
    {
      load(new Path(fileName));
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.util.Iterator#remove()
   */
  @Override
  public void remove()
  {
    throw new UnsupportedOperationException();
  }

  public void reset()
  {
    currentIndex = 0;
  }

  @Override
  public int size()
  {
    return shpFile.getCount();
  }
  
  @Override
  public void close()
  {
    if (shpFile != null)
    {
      try
      {
        shpFile.close();
      }
      catch (IOException e)
      {
        e.printStackTrace();
      }
      
      shpFile = null;
    }
  }
  
  @Override
  protected void finalize()
  {
    if (shpFile != null)
    {
      try
      {
        shpFile.close();
        shpFile = null;
      }
      catch (IOException e)
      {
      }
    }
  }
}
