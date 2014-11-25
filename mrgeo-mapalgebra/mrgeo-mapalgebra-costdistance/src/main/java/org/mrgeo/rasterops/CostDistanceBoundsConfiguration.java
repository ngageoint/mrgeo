package org.mrgeo.rasterops;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import org.apache.hadoop.conf.Configuration;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.utils.TMSUtils.Bounds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class CostDistanceBoundsConfiguration
{
  private static final Logger LOG = LoggerFactory.getLogger(CostDistanceBoundsConfiguration.class);

  public static Bounds getBounds(Bounds rvBounds, 
    float maxCost, double minFriction,
    Configuration conf)
  {
    /*
     * We have two ways to infer bounds - the algorithm is as follows: 
     * 1. If a hard-coded bounds buffer (as per giraph.boundsBuffer) is provided, use that one
     * 2. Otherwise, unless explicitly forbidden by user (as per giraph.disableAutoBounds), 
     *    calculate a bounds buffer based on maxCost and minFriction 
     * 3. If neither case 1 or 2, return the passed in bounds. This will be cropped to image bounds later 
     *    in BoundsCropper anyway
     */
    int schemeUsedForBoundsCalculation = 3; // set to 1,2,3 depending on above algorithm
    double buffer = 0;
    final Properties properties = MrGeoProperties.getInstance();
    if (!properties.getProperty("giraph.boundsBuffer", "-1").equals("-1"))
    {
      buffer = Double.valueOf(properties.getProperty("giraph.boundsBuffer"));
      schemeUsedForBoundsCalculation = 1;
    }
    else if (properties.getProperty("giraph.disableAutoBounds", "false").equals("false"))
    {
      buffer = getAutoBoundsBuffer(maxCost, minFriction);
      schemeUsedForBoundsCalculation = 2;
    }

    conf.setInt(CostDistanceVertex.SCHEME_FOR_BOUNDS_CALCULATION, schemeUsedForBoundsCalculation);

    Bounds bounds = new Bounds(rvBounds.w - buffer, 
      rvBounds.s - buffer,
      rvBounds.e + buffer,
      rvBounds.n + buffer);

    LOG.info(String.format("Got bounding box %s, adding box %s and scheme=%d", 
      rvBounds, bounds, schemeUsedForBoundsCalculation));
    return bounds;

  }

  public static Bounds getBounds(String sourcePointsStr, 
    float maxCost, double minFriction,
    Configuration conf)
  {
    /*
     * We have two ways to infer bounds - the algorithm is as follows: 
     * 1. If a hard-coded bounds buffer (as per giraph.boundsBuffer) is provided, use that one
     * 2. Otherwise, unless explicitly forbidden by user (as per giraph.disableAutoBounds), 
     *    calculate a bounds buffer based on maxCost and minFriction 
     * 3. If neither case 1 or 2, return world bounds. This will be cropped to image bounds later 
     *    in BoundsCropper anyway
     */
    int schemeUsedForBoundsCalculation = 3; // set to 1,2,3 depending on above
    // algorithm
    double buffer = -1;
    final Properties properties = MrGeoProperties.getInstance();
    if (!properties.getProperty("giraph.boundsBuffer", "-1").equals("-1"))
    {
      buffer = Double.valueOf(properties.getProperty("giraph.boundsBuffer"));
      schemeUsedForBoundsCalculation = 1;
    }
    else if (properties.getProperty("giraph.disableAutoBounds", "false").equals("false"))
    {
      buffer = getAutoBoundsBuffer(maxCost, minFriction);
      schemeUsedForBoundsCalculation = 2;
    }

    conf.setInt(CostDistanceVertex.SCHEME_FOR_BOUNDS_CALCULATION, schemeUsedForBoundsCalculation);

    String[] sourcePointsArray = sourcePointsStr.split(";");
    if(sourcePointsArray.length > 1) {
      throw new IllegalArgumentException(
        "The source points string passed is \"" + sourcePointsStr + "\"." + 
            "Currently, CostDistance does not support multiple source points using " +
            "InlineCsv. To feed multiple source points, call RasterizeVector first, " +
            "and then feed the raster to CostDistance. Look at the MapAlgebra Reference " +
          "Guide for CostDistance calling syntax.");
    }
    final String sourcePoint = sourcePointsArray[0];

    String point = sourcePoint.substring(1, sourcePoint.length() - 1);
    Geometry pt = null;
    try
    {
      WKTReader reader = new WKTReader();
      pt = reader.read(point);

      final double lat = pt.getCoordinate().y;
      final double lon = pt.getCoordinate().x;

      Bounds bounds = Bounds.WORLD;
      if (buffer != -1)
      {
        // put buffer around source point
        bounds = new Bounds(lon - buffer, lat - buffer, lon + buffer, lat + buffer);
      }

      LOG.info(String.format("Got source point %f,%f, adding box %s and scheme=%d", 
        lon, lat, bounds, schemeUsedForBoundsCalculation));

      return bounds;
    }
    catch (ParseException e)
    {
      System.out.println("Could not read destination points inlineCsv!");
      e.printStackTrace();
    }
    
    return Bounds.WORLD;
  }

  private static double getAutoBoundsBuffer(float maxCost, double minFriction)
  {
    if (minFriction <= 0 || Double.isNaN(minFriction))
      return -1;

    /*
     * Estimate bounds buffer based on maxCost and min friction surface plus a
     * fudge factor
     */
    double distanceMeters = maxCost / minFriction;
    double metersPerDegree = 1852 * 60;
    double fudge = 1.01;
    double buffer = distanceMeters / metersPerDegree;
    buffer *= fudge;
    return buffer;
  }
}
