package org.mrgeo.featurefilter;

import org.geotools.geometry.DirectPosition2D;
import org.geotools.referencing.CRS;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.geometry.GeometryCollection;
import org.mrgeo.geometry.GeometryFactory;
import org.mrgeo.geometry.LineString;
import org.mrgeo.geometry.Point;
import org.mrgeo.geometry.Polygon;
import org.mrgeo.geometry.WritableGeometryCollection;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BufferFeatureFilter extends BaseFeatureFilter
{
  private static final Logger log = LoggerFactory.getLogger(BufferFeatureFilter.class);

  private static final long serialVersionUID = 1L;
  private double _distance = -1;
  private double _distanceRange = -1;
  private String _unit = "degree";

  private CoordinateReferenceSystem sourceCrs;
  private CoordinateReferenceSystem destCrs;

  public BufferFeatureFilter()
  {
    try
    {
      sourceCrs = CRS.decode("EPSG:4326");
      destCrs = CRS.decode("EPSG:3857"); // Spherical Mercator projection
    }
    catch (final NoSuchAuthorityCodeException e)
    {
      e.printStackTrace();
    }
    catch (final FactoryException e)
    {
      e.printStackTrace();
    }

  }

  @Override
  public Geometry filter(final Geometry f)
  {
    // this filter in place creates a new feature, so we're doing this for
    // performance.
    return filterInPlace(f);
  }

  @Override
  public Geometry filterInPlace(final Geometry f)
  {
    return bufferGeometry(f);
  }

  public double getDistance()
  {
    return _distance;
  }

  public void setDistance(final double d)
  {
    _distance = d;
  }

  public void setDistanceRange(final double d)
  {
    _distanceRange = d;
  }

  public void setUnit(final String unit)
  {
    _unit = unit;
  }

  private double calculateDistance(final Geometry g, final double distance)
  {

    if (g instanceof Polygon)
    {
      final Polygon polygon = (Polygon) g;
      final LineString ring = polygon.getExteriorRing();

      final Point c = ring.getPoint(0);
      return convertDistance(c.getX(), c.getY(), distance);
    }
    else if (g instanceof Point)
    {
      final Point point = (Point) g;
      return convertDistance(point.getX(), point.getY(), distance);
    }
    else if (g instanceof LineString)
    {
      final LineString ring = (LineString) g;

      final Point c = ring.getPoint(0);
      return convertDistance(c.getX(), c.getY(), distance);
    }
    else if (g instanceof GeometryCollection)
    {
      final  GeometryCollection collection  = (GeometryCollection)g;
      return calculateDistance(collection.getGeometry(0), distance);
    }
    else
    {
      throw new IllegalArgumentException("Geometry type not implemented " + g.getClass().toString());
    }
  }

  private double convertDistance(final double lon, final double lat, final double distance)
  {
    double dist = 0.0;

    if (lat >= 90.0 || lat <= -90.0)
    {
      throw new IllegalArgumentException("Ellipses at or above the poles are not supported.");
    }

    MathTransform transform = null;
    try
    {
      // Remember, lat & lon are swapped for transform 
      final DirectPosition2D sourcePoint = new DirectPosition2D(lat, lon);

      final DirectPosition2D destPoint = new DirectPosition2D();
      final DirectPosition2D destInversePoint = new DirectPosition2D();
      // create a lenient transform. This means that if the
      // "Bursa-Wolf parameters" are not
      // available a less accurate transform will be performed.
      transform = CRS.findMathTransform(sourceCrs, destCrs, true);

      log.debug("org.geotools.referencing.forceXY: " + System.getProperty("org.geotools.referencing.forceXY", "<not set>"));

      log.debug("src: lon/lat: " + lon + " " + lat);
      transform.transform(sourcePoint, destPoint);
      log.debug("dest: x/y: " + destPoint.x + " " + destPoint.y);

      double x = destPoint.x;
      final double y = destPoint.y;

      x = x + distance;

      destPoint.setLocation(x, y);// reset point after add the distance
      log.debug("dest(2): x/y: " + destPoint.x + " " + destPoint.y);

      final MathTransform transformInverse = transform.inverse();
      transformInverse.transform(destPoint, destInversePoint);

      log.debug("dest inverse: x/y: " + destInversePoint.x + " " + destInversePoint.y);

      // unswap the x & y
      final double deltaX = destInversePoint.y - lon;
      final double deltaY = destInversePoint.x - lat;

      dist = Math.sqrt(deltaX * deltaX + deltaY * deltaY);
    }
    catch (final FactoryException e)
    {
      throw new IllegalArgumentException(e);
    }
    catch (final MismatchedDimensionException e)
    {
      throw new MismatchedDimensionException();
    }
    catch (final TransformException e)
    {
      e.printStackTrace();
    }
    return dist;
  }

  private Geometry bufferGeometry(final Geometry geometry)
  {
    com.vividsolutions.jts.geom.Geometry jtsGeom = geometry.toJTS();

    double distance = _distance;

    if (_unit.equalsIgnoreCase("meter"))
    {
      distance = calculateDistance(geometry, distance);
    }

    if (_distanceRange > -1)
    {
      double distanceRange = _distanceRange;
      if (_unit.equalsIgnoreCase("meter"))
      {
        distanceRange = calculateDistance(geometry, distanceRange);
      }

      WritableGeometryCollection collection = GeometryFactory.createGeometryCollection();
      // Make sure to add the outer ring first, then the inner ring. If they are
      // backwards, the output will be blank.
      collection.addGeometry(GeometryFactory.fromJTS(jtsGeom.buffer(distanceRange)));
      collection.addGeometry(GeometryFactory.fromJTS(jtsGeom.buffer(distance)));

      return collection;
    }

    return GeometryFactory.fromJTS(jtsGeom.buffer(distance));
  }
}
