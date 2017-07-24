/*
 * Copyright 2009-2017. DigitalGlobe, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.mrgeo.utils.tms;

public class TMSUtils
{

public final static int MAXZOOMLEVEL = 22; // max zoom level (the highest X value can be as an int)

// limits bounds to +=180, +=90
public static Bounds limit(Bounds bounds)
{
  double n, s, e, w;

  if (bounds.w < -180.0)
  {
    w = -180.0;
  }
  else if (bounds.w >= 180.0)
  {
    w = 179.9999999999;
  }
  else
  {
    w = bounds.w;
  }

  if (bounds.s < -90.0)
  {
    s = -90.0;
  }
  else if (bounds.s >= 90.0)
  {
    s = 89.9999999999;
  }
  else
  {
    s = bounds.s;
  }

  if (bounds.e < -180.0)
  {
    e = 180.0;
  }
  else if (bounds.e >= 180.0)
  {
    e = 179.9999999999;
  }
  else
  {
    e = bounds.e;
  }

  if (bounds.n < -90.0)
  {
    n = -90.0;
  }
  else if (bounds.n >= 90.0)
  {
    n = 89.9999999999;
  }
  else
  {
    n = bounds.n;
  }

  return new Bounds(w, s, e, n);
}


// Converts lat/lon bounds to the correct tile bounds for a zoom level
public static TileBounds boundsToTile(Bounds bounds, int zoom,
    int tilesize)
{
  Tile ll = latLonToTile(bounds.s, bounds.w, zoom, tilesize, false);
  Tile ur = latLonToTile(bounds.n, bounds.e, zoom, tilesize, true);

  // this takes care of the case where the bounds is a vert or horiz "line" and also on the tile
  // boundaries.
  if (ur.tx < ll.tx)
  {
    ur = new Tile(ll.tx, ur.ty);
  }
  if (ur.ty < ll.ty)
  {
    ur = new Tile(ur.tx, ll.ty);
  }

  return new TileBounds(ll.tx, ll.ty, ur.tx, ur.ty);
}

// Converts lat/lon bounds to the correct tile bounds for a zoom level. Use this function
// to compute the tile bounds when working with vector data because it does not use the
// excludeEdge feature of the latLonToTile() function when computing the upper right tile.
public static TileBounds boundsToTileExact(Bounds bounds, int zoom,
    int tilesize)
{
  Tile ll = latLonToTile(bounds.s, bounds.w, zoom, tilesize, false);
  Tile ur = latLonToTile(bounds.n, bounds.e, zoom, tilesize, false);

  // If the east coordinate is 180, the computed tx will be one larger than the max number
  // of tiles. Bring it back into range. Same for north.
  if (bounds.e >= 180.0)
  {
    ur = new Tile(ur.tx - 1, ur.ty);
  }
  if (bounds.n >= 90.0)
  {
    ur = new Tile(ur.tx, ur.ty - 1);
  }

  // this takes care of the case where the bounds is a vert or horiz "line" and also on the tile
  // boundaries.
  if (ur.tx < ll.tx)
  {
    ur = new Tile(ll.tx, ur.ty);
  }
  if (ur.ty < ll.ty)
  {
    ur = new Tile(ur.tx, ll.ty);
  }

  return new TileBounds(ll.tx, ll.ty, ur.tx, ur.ty);
}

public static Tile calculateTile(Tile tile, int srcZoom, int dstZoom,
    int tilesize)
{
  Bounds bounds = tileBounds(tile.tx, tile.ty, srcZoom, tilesize);

  return latLonToTile(bounds.s, bounds.w, dstZoom, tilesize);
}

public static boolean isValidTile(long tx, long ty, int zoomlevel)
{
  return tx >= 0 && tx < (long) Math.pow(2.0, zoomlevel - 1.0) * 2 && ty >= 0 &&
      ty < (long) Math.pow(2.0, zoomlevel - 1.0);
}

// Converts lat/lon to pixel coordinates in given zoom of the EPSG:4326
// pyramid
public static Pixel latLonToPixels(double lat, double lon, int zoom,
    int tilesize)
{
  double res = resolution(zoom, tilesize);

  return new Pixel((long) ((180.0 + lon) / res), (long) ((90.0 + lat) / res));
}

// Converts lat/lon to pixel coordinates in given zoom of the EPSG:4326
// pyramid in an upper-left as 0,0 coordinate grid
public static Pixel latLonToPixelsUL(double lat, double lon, int zoom,
    int tilesize)
{
  Pixel p = latLonToPixels(lat, lon, zoom, tilesize);
  return new Pixel(p.px, (numYTiles(zoom) * tilesize) - p.py - 1);
  // final double res = resolution(zoom, tilesize);
  //
  // return new Pixel((long) ((180.0 + lon) / res), (long) ((90.0 - lat) / res));
}

// Returns the tile for zoom which covers given lat/lon coordinates"
public static Tile latLonToTile(double lat, double lon, int zoom,
    int tilesize)
{
  return latLonToTile(lat, lon, zoom, tilesize, false);
}

/**
 * Returns the pixel within a tile (where 0, 0 is anchored at the bottom left of the tile) for
 * zoom which covers given lat/lon coordinates
 */
public static Pixel latLonToTilePixel(double lat, double lon, long tx,
    long ty, int zoom, int tilesize)
{
  Pixel p = latLonToPixels(lat, lon, zoom, tilesize);
  Bounds b = tileBounds(tx, ty, zoom, tilesize);
  Pixel ll = latLonToPixels(b.s, b.w, zoom, tilesize);
  return new Pixel(p.px - ll.px, p.py - ll.py);
}

/**
 * Returns the pixel within a tile (where 0, 0 is anchored at the top left of the tile) for zoom
 * which covers given lat/lon coordinates
 */
public static Pixel latLonToTilePixelUL(double lat, double lon, long tx,
    long ty, int zoom, int tilesize)
{
  Pixel p = latLonToTilePixel(lat, lon, tx, ty, zoom, tilesize);
  return new Pixel(p.px, tilesize - p.py - 1);
}

// formulae taken from GDAL's gdal2tiles.py GlobalGeodetic() src code...

public static long numXTiles(int zoomlevel)
{
  return (long) Math.pow(2.0, zoomlevel);
}

public static long numYTiles(int zoomlevel)
{
  return (long) Math.pow(2.0, zoomlevel - 1.0);
}

/**
 * Compute the worldwide tile in which the specified pixel resides. The pixel coordinates are
 * provided based on 0, 0 being bottom, left.
 */
public static Tile pixelsToTile(double px, double py, int tilesize)
{
  return new Tile((long) (px / tilesize), (long) (py / tilesize));
}

/**
 * Compute the worldwide tile in which the specified pixel resides. The pixel coordinates are
 * provided based on 0, 0 being top, left.
 */
public static Tile pixelsULToTile(double px, double py, int zoom,
    int tilesize)
{
  Tile tileFromBottom = pixelsToTile(px, py, tilesize);
  return new Tile(tileFromBottom.tx, numYTiles(zoom) - tileFromBottom.ty - 1);
  // long numYTiles = numYTiles(zoom);
  // long numXTiles = numXTiles(zoom);
  // long tilesFromTop = (long)(py / numXTiles);
  // long tileRow = numYTiles - tilesFromTop;
  // return new Tile((long) (px / tilesize), maxYTile - (long) (py / tilesize));
}

public static LatLon pixelToLatLon(long px, long py, int zoom,
    int tilesize)
{
  Tile tile = pixelsToTile(px, py, tilesize);
  Bounds bounds = tileBounds(tile.tx, tile.ty, zoom, tilesize);
  Pixel tilepx = latLonToPixels(bounds.s, bounds.w, zoom, tilesize);

  double resolution = resolution(zoom, tilesize);
  long pixelsFromTileLeft = px - tilepx.px;
  long pixelsFromTileBottom = py - tilepx.py;
  return new LatLon(bounds.s + (pixelsFromTileBottom * resolution), bounds.w +
      (pixelsFromTileLeft * resolution));
}

public static LatLon pixelToLatLonUL(long px, long py, int zoom,
    int tilesize)
{
  Tile tile = pixelsULToTile(px, py, zoom, tilesize);
  Bounds bounds = tileBounds(tile.tx, tile.ty, zoom, tilesize);
  Pixel tilepx = latLonToPixelsUL(bounds.n, bounds.w, zoom, tilesize);

  double resolution = resolution(zoom, tilesize);
  long pixelsFromTileLeft = px - tilepx.px;
  long pixelsFromTileTop = py - tilepx.py;
  return new LatLon(bounds.n - (pixelsFromTileTop * resolution), bounds.w +
      (pixelsFromTileLeft * resolution));
}

public static LatLon tilePixelToLatLon(long px, long py, Tile tile, int zoom,
    int tilesize)
{
  Bounds bounds = tileBounds(tile.tx, tile.ty, zoom, tilesize);

  double resolution = resolution(zoom, tilesize);
  return new LatLon(bounds.s + (py * resolution), bounds.w +
      (px * resolution));
}

public static LatLon tilePixelULToLatLon(long px, long py, Tile tile, int zoom,
    int tilesize)
{
  Bounds bounds = tileBounds(tile.tx, tile.ty, zoom, tilesize);

  double resolution = resolution(zoom, tilesize);
  return new LatLon(bounds.n - (py * resolution), bounds.w +
      (px * resolution));
}

// Resolution (deg/pixel) for given zoom level (measured at Equator)"
public static double resolution(int zoom, int tilesize)
{
  if (zoom > 0)
  {
    return 180.0 / tilesize / Math.pow(2.0, zoom - 1.0);
  }

  return 0.0;
}

public static Bounds tileBounds(long tx, long ty, int zoom, int tilesize)
{
  double res = resolution(zoom, tilesize);

  return new Bounds(tx * tilesize * res - 180.0, // left/west (lon, x)
      ty * tilesize * res - 90.0, // lower/south (lat, y)
      (tx + 1) * tilesize * res - 180.0, // right/east (lon, x)
      (ty + 1) * tilesize * res - 90.0); // upper/north (lat, y)

}

public static Bounds tileBounds(Tile tile, int zoom, int tilesize)
{
  return tileBounds(tile.tx, tile.ty, zoom, tilesize);
}

// Converts lat/lon bounds to the correct tile bounds, in lat/lon for a zoom level
public static Bounds tileBounds(Bounds bounds, int zoom, int tilesize)
{
  TileBounds tb = boundsToTile(bounds, zoom, tilesize);
  return tileToBounds(tb, zoom, tilesize);
}

// Returns bounds of the given tile
public static double[] tileBoundsArray(long tx, long ty, int zoom,
    int tilesize)
{
  Bounds b = tileBounds(tx, ty, zoom, tilesize);

  double bounds[] = new double[4];

  bounds[0] = b.w;
  bounds[1] = b.s;
  bounds[2] = b.e;
  bounds[3] = b.n;

  return bounds;
}

public static Tile tileid(long tileid, int zoomlevel)
{
  long width = (long) Math.pow(2, zoomlevel);
  long ty = tileid / width;
  long tx = tileid - (ty * width);

  return new Tile(tx, ty);
}

public static long tileid(long tx, long ty, int zoomlevel)
{
  return (ty * (long) Math.pow(2, zoomlevel)) + tx;
}

public static long maxTileId(int zoomlevel)
{
  return numXTiles(zoomlevel) * numYTiles(zoomlevel) - 1;
}

// Returns bounds of the given tile in the SWNE form
public static double[] tileSWNEBoundsArray(long tx, long ty, int zoom,
    int tilesize)
{
  Bounds b = tileBounds(tx, ty, zoom, tilesize);

  double bounds[] = new double[4];

  bounds[0] = b.s;
  bounds[1] = b.w;
  bounds[2] = b.n;
  bounds[3] = b.e;

  return bounds;
}

// Converts tile bounds to the correct lat/lon bounds for a zoom level
public static Bounds tileToBounds(TileBounds bounds, int zoom,
    int tilesize)
{
  Bounds ll = tileBounds(bounds.w, bounds.s, zoom, tilesize);
  Bounds ur = tileBounds(bounds.e, bounds.n, zoom, tilesize);

  return new Bounds(ll.w, ll.s, ur.e, ur.n);
}

// Maximal scaledown zoom of the pyramid closest to the pixelSize."
public static int zoomForPixelSize(double pixelSize, int tilesize)
{
  double pxep = pixelSize + 0.00000001; // pixelsize + epsilon
  for (int i = 1; i <= MAXZOOMLEVEL; i++)
  {
    if (pxep >= resolution(i, tilesize))
    {
      if (i > 0)
      {
        return i;
      }
    }
  }
  return 0; // We don't want to scale up
}

// Returns the tile for zoom which covers given lat/lon coordinates"
private static Tile latLonToTile(double lat, double lon, int zoom,
    int tilesize, boolean excludeEdge)
{
  Pixel p = latLonToPixels(lat, lon, zoom, tilesize);

  if (excludeEdge)
  {
    Tile tile = pixelsToTile(p.px, p.py, tilesize);
    Tile t = pixelsToTile(p.px - 1, p.py - 1, tilesize);

    // lon is on an x tile boundary, so we'll move it to the left
    if (t.tx < tile.tx)
    {
      tile = new Tile(t.tx, tile.ty);
    }

    // lat is on a y tile boundary, so we'll move it down
    if (t.ty < tile.ty)
    {
      tile = new Tile(tile.tx, t.ty);
    }

    return tile;
  }

  return pixelsToTile(p.px, p.py, tilesize);
}

}
