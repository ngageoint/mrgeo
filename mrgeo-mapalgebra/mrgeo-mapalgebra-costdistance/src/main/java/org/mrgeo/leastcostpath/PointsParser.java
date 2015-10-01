package org.mrgeo.leastcostpath;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.utils.TMSUtils;
import org.mrgeo.utils.TMSUtils.Pixel;
import org.mrgeo.utils.TMSUtils.Tile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PointsParser
{
  private static final Logger LOG = LoggerFactory.getLogger(PointsParser.class);

  /**
   * The expected format is 'POINT(66.6701 34.041)';'POINT(66.25 34.75)';..
   */
  public static Map<Long,List<Pixel>> parseSourcePoints(String pointsStr, MrsImagePyramidMetadata metadata) {

    // TODO not tested with multiple points

    Map<Long,List<Pixel>> pointsMap = new HashMap<Long,List<Pixel>>();

    String[] pointsArray = pointsStr.split(";");

    for(String pointWithParen : pointsArray) { 
      String point = pointWithParen.substring(1, pointWithParen.length() -1);
      Geometry pt = null;
      try
      {
        WKTReader reader = new WKTReader();
        pt = reader.read(point);

        final double lat = pt.getCoordinate().y;
        final double lon = pt.getCoordinate().x;

        Pixel pixel = TMSUtils.latLonToPixels(lat, lon, 
          metadata.getMaxZoomLevel(), metadata.getTilesize());
        Tile tile = TMSUtils.pixelsToTile(pixel.px, pixel.py, metadata.getTilesize());
        long tileId = TMSUtils.tileid(tile.tx, tile.ty, metadata.getMaxZoomLevel());

        List<Pixel> pixelList = pointsMap.get(tileId);
        if(pixelList == null) {
          pixelList = new ArrayList<Pixel>();
          pointsMap.put(tileId, pixelList);
        }

        /* To compute localPixel, we have to be cognizant of rasters having their 0,0 on the upper 
         * left, so we recompute the absolute pixel from the upper left (as pixelUL) and then do 
         * modulo arithmetic to figure out localPixel. We could have used pixel instead of pixelUL
         * and saved some computation, but the modulo arithmetic would look ugly, hence we err on the
         * side of code readability
         */ 
        Pixel pixelUL = TMSUtils.latLonToPixelsUL(lat, lon, 
          metadata.getMaxZoomLevel(), metadata.getTilesize());
        Pixel localPixel = new Pixel(pixelUL.px % metadata.getTilesize(), 
          pixelUL.py % metadata.getTilesize());
        pixelList.add(localPixel);
        LOG.debug(String.format("point lon=%f lat=%f abs_px=%d abs_py=%d px=%d py=%d tileId %d",
          lon,lat,pixel.px,pixel.py,localPixel.px,localPixel.py,tileId));
      }
      catch (ParseException e)
      {
        System.out.println("Error parsing points " + pointsStr);
        e.printStackTrace();
      }
    }

    return pointsMap;
  }
}
