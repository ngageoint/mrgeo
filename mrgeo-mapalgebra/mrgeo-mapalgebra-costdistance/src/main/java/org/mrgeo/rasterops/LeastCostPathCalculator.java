package org.mrgeo.rasterops;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mrgeo.image.MrsImage;
import org.mrgeo.utils.HadoopUtils;
import org.mrgeo.utils.TMSUtils;
import org.mrgeo.utils.TMSUtils.Pixel;
import org.mrgeo.utils.TMSUtils.Tile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.image.Raster;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class LeastCostPathCalculator
{
  private static final Logger LOG = LoggerFactory.getLogger(LeastCostPathCalculator.class);

  MrsImage image;

  private TileCache tileCache;

  private Raster curRaster;
  private Tile curTile;
  private org.mrgeo.utils.TMSUtils.Bounds curTileBounds;
  private double resolution;
  private Pixel curPixel;
  private float curValue;

  private String outputStr = "";
  private float pathCost = 0f;
  private float pathDistance = 0f;
  private DecimalFormat df = new DecimalFormat("###.#");
  private double pathMinSpeed = 1000000f;
  private double pathMaxSpeed = 0f;
  private double pathAvgSpeed = 0f;

  // stats
  private int numPixels=0;

  int numTiles=0;

  // start from the top left neighbors and work clock-wise down to the other 7 neighbors
  final short[] dx = { -1, 0, 1, 1, 1, 0, -1, -1 };
  final short[] dy = { -1, -1, -1, 0, 1, 1, 1, 0 };

  public LeastCostPathCalculator(String inputImagePath, int zoomLevel,
      final Properties providerProperties) throws IOException {
    tileCache = new TileCache();
    openImage(inputImagePath, zoomLevel, providerProperties);
  }

  public static void run(String destPoints, String inputImagePath, int zoomLevel,
      String outputName, final Properties providerProperties) throws IOException {
    // TODO - do some validation on inputs
    LeastCostPathCalculator lcp = null;
    try {
      lcp = new LeastCostPathCalculator(inputImagePath, zoomLevel, providerProperties);
      lcp.run(destPoints, outputName);
    } finally {
      if (lcp != null)
      {
        lcp.close();
      }
    }
  }

  private void run(String destPoints, String outputName) throws IOException {

    Map<Long,List<Pixel>> sourcePoints = PointsParser.parseSourcePoints(destPoints, image.getMetadata());

    Set<Long> tileIds = sourcePoints.keySet();
    long startTileId = tileIds.iterator().next();
    List<Pixel> startPixels = sourcePoints.get(startTileId);
    if(tileIds.size() > 1 || startPixels.size() > 1) {
      throw new UnsupportedOperationException("Oops - too many destination points - expected 1 got at least " + startPixels.size());
    }

    resolution = TMSUtils.resolution(image.getZoomlevel(), image.getTilesize());
    curTile = TMSUtils.tileid(startTileId, image.getZoomlevel());
    curTileBounds = TMSUtils.tileBounds(curTile.tx, curTile.ty,
      image.getZoomlevel(), image.getTilesize());
    curPixel = startPixels.get(0);
    curRaster = tileCache.getTile(curTile.tx, curTile.ty);
    curValue = curRaster.getSampleFloat((int)curPixel.px, (int)curPixel.py, 0);

    // dest pt outside cost surface is fatal
    if(Float.isNaN(curValue)) {

      throw new IllegalStateException(
        String.format("Destination point \"%s\" falls outside cost surface \"%s\"",
          destPoints, image));
    }

    // set total path cost to the pixel value of destination point
    pathCost = curValue;

    addPixelToOutput();

    // update stats with start pixel
    numPixels++;

    while(next()) {
      //convert pixel to lat/lon and add to outputStr
      addPixelToOutput();
    }
    
    pathAvgSpeed = pathDistance / pathCost;

    // write outputStr to file
    Path outFilePath = new Path(outputName, "leastcostpaths.tsv");
    writeToFile(outFilePath);

    if(LOG.isDebugEnabled()) {
      LOG.debug("Total pixels = " + numPixels + " and total tiles = " + numTiles);
    }
  }

  private boolean next() {
    if(LOG.isDebugEnabled()) {
      LOG.debug("curPixel = " + curPixel + " with value " + curValue);
    }

    Raster candNextRaster = curRaster;
    Tile candNextTile = curTile;

    Raster minNextRaster = null;
    Tile minNextTile = null;
    short minXNeighbor = Short.MAX_VALUE, minYNeighbor = Short.MAX_VALUE;

    final int tileWidth = image.getTilesize();
    final short widthMinusOne = (short)(tileWidth - 1);

    float leastValue = curValue;
    
    long deltaX = 0;
    long deltaY = 0;


    // Note - this may have to be tweaked to work for pixels that fall on the borders of border
    // tiles, since calculation of neighboring tiles and fetching them does not take into account
    // falling out of border
    for (int i = 0; i < 8; i++) {
      short xNeighbor = (short)(curPixel.px + dx[i]);
      short yNeighbor = (short)(curPixel.py + dy[i]);

      if(xNeighbor >= 0 && xNeighbor <= widthMinusOne
          && yNeighbor >= 0 && yNeighbor <= widthMinusOne) {
        // neighboring pixel falls within current tile
        candNextRaster = curRaster;
        candNextTile = curTile;
      } else if(xNeighbor == -1 && yNeighbor == -1){
        // top left neighboring tile
        candNextTile = new Tile(curTile.tx - 1, curTile.ty + 1);
        candNextRaster = tileCache.getTile(candNextTile.tx, candNextTile.ty);
        xNeighbor = yNeighbor = widthMinusOne;
      } else if(xNeighbor >= 0 && xNeighbor <= widthMinusOne && yNeighbor == -1) {
        // top neighboring tile
        candNextTile = new Tile(curTile.tx, curTile.ty + 1);
        candNextRaster = tileCache.getTile(candNextTile.tx, candNextTile.ty);
        yNeighbor = widthMinusOne;
      } else if(xNeighbor == tileWidth && yNeighbor == -1) {
        // top right neighboring tile
        candNextTile = new Tile(curTile.tx + 1, curTile.ty + 1);
        candNextRaster = tileCache.getTile(candNextTile.tx, candNextTile.ty);
        xNeighbor = 0;
        yNeighbor = widthMinusOne;
      } else if(xNeighbor == tileWidth && yNeighbor >= 0 && yNeighbor <= widthMinusOne) {
        // right neighboring tile
        candNextTile = new Tile(curTile.tx + 1, curTile.ty);
        candNextRaster = tileCache.getTile(candNextTile.tx, candNextTile.ty);
        xNeighbor = 0;
      } else if(xNeighbor == tileWidth && yNeighbor == tileWidth) {
        // bottom right neighboring tile
        candNextTile = new Tile(curTile.tx + 1, curTile.ty - 1);
        candNextRaster = tileCache.getTile(candNextTile.tx, candNextTile.ty);
        xNeighbor = yNeighbor = 0;
      } else if(xNeighbor >= 0 && xNeighbor <= widthMinusOne && yNeighbor == tileWidth) {
        // bottom neighboring tile
        candNextTile = new Tile(curTile.tx, curTile.ty - 1);
        candNextRaster = tileCache.getTile(candNextTile.tx, candNextTile.ty);
        yNeighbor = 0;
      } else if(xNeighbor == -1 && yNeighbor == tileWidth) {
        // bottom left neighboring tile
        candNextTile = new Tile(curTile.tx - 1, curTile.ty - 1);
        candNextRaster = image.getTile(candNextTile.tx, candNextTile.ty);
        xNeighbor = widthMinusOne;
        yNeighbor = 0;
      } else if(xNeighbor == -1 && yNeighbor >= 0 && yNeighbor <= widthMinusOne) {
        // left neighboring tile
        candNextTile = new Tile(curTile.tx - 1, curTile.ty);
        candNextRaster = tileCache.getTile(candNextTile.tx, candNextTile.ty);
        xNeighbor = widthMinusOne;
      } else {
        // houston, we have a problem!
        assert(true);
      }

      //final int neighborIndex = 3 * (xNeighbor + yNeighbor * tileWidth);
      float value = candNextRaster.getSampleFloat(xNeighbor, yNeighbor, 0);

      // exclude NaN's and -9999.0, include start points (value == 0)
      if(!Float.isNaN(value) && value >= 0 && value < leastValue) {
        minXNeighbor = xNeighbor;
        minYNeighbor = yNeighbor;
        minNextRaster = candNextRaster;
        minNextTile = candNextTile;
        leastValue = value;
        deltaX = dx[i];
        deltaY = dy[i];
      }
    }
    if(leastValue == curValue)
      return false;

    numPixels++;
    curPixel = new Pixel(minXNeighbor, minYNeighbor);
    
    //calc the delta distance, time and speed here
    double lat = curTileBounds.n - (curPixel.py * resolution);
    float deltaTime = curValue - leastValue;
    double rlat = Math.toRadians(lat);
    //http://fmepedia.safe.com/articles/How_To/Calculating-accurate-length-in-meters-for-lat-long-coordinate-systems
    double metersPerDegreeLat = 111132.92 - 559.82 * Math.cos(2*rlat) + 1.175 * Math.cos(4*rlat);
    double metersPerDegreeLon = 111412.84 * Math.cos(rlat) - 93.5 * Math.cos(3*rlat);
    double deltaDistance = Math.sqrt(Math.pow(metersPerDegreeLon * resolution * deltaX,2) + Math.pow(metersPerDegreeLat * resolution * deltaY,2));
    pathDistance += deltaDistance;
    double speed = deltaDistance / deltaTime;
    if (speed < pathMinSpeed) pathMinSpeed = speed;
    if (speed > pathMaxSpeed) pathMaxSpeed = speed;
    
    curValue = leastValue;

    // optimize a bit by not worrying about resetting curRaster/curTile/curTileBounds unless we
    // actually ended up finding a pixel in another tile
    if(!curTile.equals(minNextTile)) {
      curRaster = minNextRaster;
      curTile = minNextTile;
      curTileBounds = TMSUtils.tileBounds(curTile.tx, curTile.ty,
        image.getZoomlevel(), image.getTilesize());
    }
    return true;
  }

  private void writeToFile(Path outFilePath) throws IOException
  {
    //TODO upgrade to use mrgeo tsv output class
    Configuration conf = HadoopUtils.createConfiguration();
    FileSystem fs = outFilePath.getFileSystem(conf);

    if(fs.exists(outFilePath))
    {
      fs.delete(outFilePath, true);
    }

    // Create the file and write the point to it
    BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(outFilePath)));
    String finalOutputStr = packageFinalOutput();
    br.write(finalOutputStr);
    br.close();

    // Write the columns file

    Path columnsPath = new Path(outFilePath.toUri().toString() + ".columns");
    if(fs.exists(columnsPath))
    {
      fs.delete(columnsPath, true);
    }

    br=new BufferedWriter(new OutputStreamWriter(fs.create(columnsPath)));
    String columns = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";
    columns += "<AllColumns firstLineHeader=\"false\">\n";
    columns += "<Column name=\"GEOMETRY\" type=\"Nominal\"/>\n";
    columns += "<Column name=\"VALUE\" type=\"Numeric\"/>\n";
    columns += "<Column name=\"DISTANCE\" type=\"Numeric\"/>\n";
    columns += "<Column name=\"MINSPEED\" type=\"Numeric\"/>\n";
    columns += "<Column name=\"MAXSPEED\" type=\"Numeric\"/>\n";
    columns += "<Column name=\"AVGSPEED\" type=\"Numeric\"/>\n";
    columns += "</AllColumns>\n";
    br.write(columns);

    br.close();
  }

  private String packageFinalOutput() {
    StringBuffer outStr = new StringBuffer();
    outStr.append("LINESTRING(");
    int indexOfLastComma = outputStr.lastIndexOf(',');
    assert(indexOfLastComma > -1 && indexOfLastComma == outputStr.length() - 1);
    outStr.append(outputStr.substring(0, indexOfLastComma));
    outStr.append(")\t");
    outStr.append(df.format(pathCost));
    outStr.append("\t");
    outStr.append(df.format(pathDistance));
    outStr.append("\t");
    outStr.append(df.format(pathMinSpeed));
    outStr.append("\t");
    outStr.append(df.format(pathMaxSpeed));
    outStr.append("\t");
    outStr.append(df.format(pathAvgSpeed));
    outStr.append("\n");
    return outStr.toString();
  }

  private void addPixelToOutput() {
    double lat = curTileBounds.n - (curPixel.py * resolution);
    double lon = curTileBounds.w + (curPixel.px * resolution);

    outputStr += (Double.toString(lon) + " " + Double.toString(lat) + ",");
  }

  private void close() {
    tileCache.clear();
    closeImage();
  }

  private void openImage(String pyramidName, int zoomLevel,
      final Properties providerProperties) throws IOException {
    this.image = MrsImage.open(pyramidName, zoomLevel, providerProperties);
    if (this.image == null)
    {
      throw new IOException("The cost distance image pyramid at " + pyramidName +
          " does not have an image at zoom level " + zoomLevel);
    }
  }

  private void closeImage() {
    image.close();
  }

  private class TileCache {
    private Map<Long, Raster> tiles;

    public TileCache() {
      tiles = new HashMap<Long, Raster>();
    }

    public void clear() {
      tiles.clear();
    }

    public Raster getTile(long tx, long ty) {
      long tileId = TMSUtils.tileid(tx, ty, image.getZoomlevel());
      Raster tile = tiles.get(tileId);
      if(tile != null) {
        return tile;
      }
      tile = image.getTile(tx, ty);
      tiles.put(tileId, tile);

      // updating tile count
      numTiles++;

      return tile;
    }
  }
}
