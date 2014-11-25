package org.mrgeo.cmd;

import org.apache.commons.cli.*;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.geotools.coverage.CoverageFactoryFinder;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.data.DataStore;
import org.geotools.data.DataUtilities;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.Transaction;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geometry.GeneralEnvelope;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.geometry.GeometryCollection;
import org.mrgeo.image.*;
import org.mrgeo.image.geotools.GeotoolsRasterUtils;
import org.mrgeo.vector.mrsvector.MrsVector;
import org.mrgeo.vector.mrsvector.MrsVectorPyramid;
import org.mrgeo.vector.mrsvector.VectorTile;
import org.mrgeo.pyramid.MrsPyramid;
import org.mrgeo.rasterops.OpImageRegistrar;
import org.mrgeo.tile.TileNotFoundException;
import org.mrgeo.data.raster.RasterUtils;
import org.mrgeo.utils.*;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.image.BufferedImage;
import java.awt.image.Raster;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public class ExportWithVector extends Command
{
  private static Logger log = LoggerFactory.getLogger(ExportWithVector.class);

  int maxTiles = -1;
  boolean useRand = false;
  boolean singleImage = false;
  int mosaicTileCount = -1;
  boolean mosaicTiles = false;
  Bounds bounds = null;
  boolean useBounds = false;
  Set<Long> tileset = null;
  boolean useTileSet = false;
  boolean all = false;

  public static Options createOptions()
  {
    final Options result = new Options();

    final Option output = new Option("o", "output", true, "Output directory");
    output.setRequired(true);
    result.addOption(output);

    final Option zoom = new Option("z", "zoom", true, "Zoom level");
    zoom.setRequired(false);
    result.addOption(zoom);

    final Option count = new Option("c", "count", true, "Number of tiles to export");
    count.setRequired(false);
    result.addOption(count);

    final Option mosaic = new Option("m", "mosaic", true, "Number of adjacent tiles to mosaic");
    mosaic.setRequired(false);
    result.addOption(mosaic);

    final Option random = new Option("r", "random", false, "Randomize export");
    random.setRequired(false);
    result.addOption(random);

    final Option single = new Option("s", "single", false, "Export as a single image");
    single.setRequired(false);
    result.addOption(single);

    final Option tileIds = new Option("t", "tileids", true,
        "A comma separated list of tile ID's to export");
    tileIds.setRequired(false);
    result.addOption(tileIds);

    final Option bounds = new Option("b", "bounds", true,
      "Returns the tiles intersecting and interior to"
          + "the bounds specified.  Lat/Lon Bounds in w, s, e, n format "
          + "(e.g., \"-180\",\"-90\",180,90). Does not honor \"-r\"");
    bounds.setRequired(false);
    result.addOption(bounds);

    final Option local = new Option("l", "local-runner", false,
        "Use Hadoop's local runner (used for debugging)");
    local.setRequired(false);
    result.addOption(local);

    final Option all = new Option("a", "all-levels", false, "Output all levels");
    all.setRequired(false);
    result.addOption(all);

    result.addOption(new Option("v", "verbose", false, "Verbose logging"));
    result.addOption(new Option("d", "debug", false, "Debug (very verbose) logging"));

    return result;
  }

  private static Bounds parseBounds(String boundsOption)
  {
    // since passed the arg parser, now can remove the " " around negative numbers
    boundsOption = boundsOption.replace("\"", "");
    final String[] bounds = boundsOption.split(",");
    assert (bounds.length == 4);
    final double w = Double.valueOf(bounds[0]);
    final double s = Double.valueOf(bounds[1]);
    final double e = Double.valueOf(bounds[2]);
    final double n = Double.valueOf(bounds[3]);

    assert (w >= -180 && w <= 180);
    assert (s >= -90 && s <= 90);
    assert (e >= -180 && e <= 180);
    assert (n >= -90 && n <= 90);
    assert (s <= n);
    assert (w <= e);

    return new Bounds(w, s, e, n);
  }

  private static boolean saveSingleTile(final String output, final MrsImage image, final long ty,
    final long tx)
  {
    try
    {
      final MrsImagePyramidMetadata metadata = image.getMetadata();

      final Raster raster = image.getTile(tx, ty);

      log.info("tx: " + tx + " ty: " + ty);

      if (raster != null)
      {
        GeotoolsRasterUtils.saveLocalGeotiff(output, raster, tx, ty, image.getZoomlevel(), image
          .getTilesize(), metadata.getDefaultValue(0));
        System.out.println("Wrote output to " + output);
        return true;
      }

      log.info("no raster!");

    }
    catch (final Exception e)
    {
      e.printStackTrace();
    }
    return false;
  }

  private static class GeometrySaver
  {
    private static final String GEOM_FIELD_NAME = "the_geom";
    private List<Geometry> geometries = new ArrayList<Geometry>();
    private SimpleFeatureType featureType = null;

    /**
     * Since features can have differing sets of attributes, but shapefiles
     * expect the same attributes for each feature, compute the feature type
     * as the collection of all unique attributes across all the features.
     *
     * @return
     */
    public SimpleFeatureType getFeatureType()
    {
      if (featureType == null)
      {
        boolean addedGeometryAttr = false;
        Set<String> addedAttributes = new HashSet<String>();
        SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
        typeBuilder.setName( "CSVFeatures" );
        typeBuilder.setNamespaceURI("");
        typeBuilder.setSRS( "EPSG:4326" );

        for (Geometry geom : geometries)
        {
          Map<String, String> attrs = geom.getAllAttributes();
          for (String attrName : attrs.keySet())
          {
            if (!addedAttributes.contains(attrName))
            {
              typeBuilder.add(attrName, String.class);
              addedAttributes.add(attrName);
            }
          }
          if (!addedGeometryAttr)
          {
            typeBuilder.add(GEOM_FIELD_NAME, geom.toJTS().getClass());
            addedGeometryAttr = true;
          }
        }
        featureType = typeBuilder.buildFeatureType();
      }
      return featureType;
    }

    public List<SimpleFeature> getFeatures()
    {
      long featureCount = 0;
      SimpleFeatureType ft = getFeatureType();
      SimpleFeatureBuilder builder = new SimpleFeatureBuilder(ft);
      List<SimpleFeature> features = new ArrayList<SimpleFeature>(geometries.size());

      for (Geometry geom : geometries)
      {
        builder.reset();
        //    builder.set("id", "" + featureCount);
        Map<String, String> attrs = geom.getAllAttributesSorted();
        for (String attrName : attrs.keySet())
        {
          builder.set(attrName, attrs.get(attrName));
        }
        builder.set(GEOM_FIELD_NAME, geom.toJTS());
        features.add(builder.buildFeature("" + featureCount));
        featureCount++;
      }
      return features;
    }

    private void saveCollection(GeometryCollection collection)
    {
      for (int i = 0; i < collection.getNumGeometries(); i++)
      {
        final Geometry geometry = collection.getGeometry(i);
        if (geometry.type() == Geometry.Type.COLLECTION)
        {
          saveCollection((GeometryCollection) geometry);
        }
        else
        {
          saveNonCollection(geometry);
        }
      }
    }

    private void saveNonCollection(Geometry geom)
    {
      geometries.add(geom);
    }

    public void saveGeometry(Geometry geom)
    {
      if (geom.type() == Geometry.Type.COLLECTION)
      {
        saveCollection((GeometryCollection) geom);
      }
      else
      {
        saveNonCollection(geom);
      }
    }
  }

  /**
   * Saves vector data from a single vector tile to the specified output directory
   * in the shapefile format. Shapefiles only allow one type of geometry in the
   * data they store, so we output a different shapefile to this directory for each
   * type of geometry.
   *
   * @param output
   * @param vector
   * @param tiles
   * @return
   * @throws java.io.IOException
   */
  private static boolean saveVectorTiles(final String output, final MrsVector vector,
    final long[] tiles) throws IOException
    {
    ShapefileDataStoreFactory dataStoreFactory = new ShapefileDataStoreFactory();

    Map<String, Serializable> params = new HashMap<String, Serializable>();
    params.put("create spatial index", Boolean.TRUE);

    ShapefileDataStore newDataStore = null;

    // Now run through the geometries in the tile, writing each one to the output file
    Map<Geometry.Type, GeometrySaver> savers = new HashMap<Geometry.Type, GeometrySaver>();
    for (long tileId : tiles)
    {
      TMSUtils.Tile t = TMSUtils.tileid(tileId, vector.getZoomlevel());
      VectorTile tile = vector.getTile(t.tx, t.ty);
      if (tile == null)
      {
        System.out.println("Unable to export tile " + tileId +
          " (" + t.tx + ", " + t.ty + "). Skipping.");
        continue;
      }

      Iterable<Geometry> geoms = tile.geometries();
      for (Geometry geom : geoms)
      {
        saveGeometry(geom, savers);
      }
    }

    for (Geometry.Type geomType : savers.keySet())
    {
      Transaction transaction = new DefaultTransaction("create");
      try
      {
        GeometrySaver saver = savers.get(geomType);
        SimpleFeatureType featureType = saver.getFeatureType();
        // Write the features to the shapefile
        // First create the output shapefile
        File newFile = new File(output + "_" + geomType.toString() + ".shp");
        params.put("url", newFile.toURI().toURL());
        DataStore ds = dataStoreFactory.createNewDataStore(params);
        newDataStore = (ShapefileDataStore) ds;
        newDataStore.createSchema(featureType);

        /*
         * You can comment out this line if you are using the createFeatureType method (at end of
         * class file) rather than DataUtilities.createType
         */
        newDataStore.forceSchemaCRS(DefaultGeographicCRS.WGS84);
        String typeName = newDataStore.getTypeNames()[0];
        SimpleFeatureSource featureSource = newDataStore.getFeatureSource(typeName);

        if (featureSource instanceof SimpleFeatureStore) {
          SimpleFeatureStore featureStore = (SimpleFeatureStore) featureSource;

          featureStore.setTransaction(transaction);
          featureStore.addFeatures(DataUtilities.collection(saver.getFeatures()));
          transaction.commit();
          System.out.println("Wrote output to " + newFile.toString());
        } else {
          System.out.println(typeName + " does not support read/write access");
        }
      }
      finally
      {
        transaction.close();
      }
    }
    return false;
    }

  private static void saveGeometry(final Geometry geom, Map<Geometry.Type, GeometrySaver> savers)
  {
    if (geom.type() == Geometry.Type.COLLECTION)
    {
      GeometryCollection collection = (GeometryCollection)geom;
      for (int i=0; i < collection.getNumGeometries(); i++)
      {
        saveGeometry(collection.getGeometry(i), savers); 
      }
    }
    else
    {
      GeometrySaver saver = savers.get(geom.type());
      if (saver == null)
      {
        saver = new GeometrySaver();
        savers.put(geom.type(), saver);
      }
      saver.saveGeometry(geom);
    }
  }

  private static boolean saveMultipleTiles(final String output, final MrsImage image, final long[] tiles)
  {
    try
    {
      final MrsImagePyramidMetadata metadata = image.getMetadata();

      final Raster raster = RasterTileMerger.mergeTiles(image, tiles);
      GeneralEnvelope envelope = null;
      Raster sampleRaster = null;

      final int tilesize = image.getTilesize();
      final int zoomlevel = image.getZoomlevel();
      for (final long lid : tiles)
      {
        final TMSUtils.Tile tile = TMSUtils.tileid(lid, zoomlevel);
        final TMSUtils.Bounds bounds = TMSUtils.tileBounds(tile.tx, tile.ty, zoomlevel, tilesize);

        final GeneralEnvelope tileBounds = new GeneralEnvelope(new double[] { bounds.w, bounds.s },
          new double[] { bounds.e, bounds.n });

        // expand the image bounds by the tile
        if (envelope == null)
        {
          envelope = tileBounds;
        }
        else
        {
          envelope.add(tileBounds);
        }

        if (sampleRaster == null)
        {
          try
          {
            sampleRaster = image.getTile(tile.tx, tile.ty);
          }
          catch (final TileNotFoundException e)
          {
            // we can eat the TileNotFoundException...
          }
        }
      }

      if (envelope == null)
      {
        throw new MrsImageException("Error, could not calculate the bounds of the tiles");
      }
      if (sampleRaster == null)
      {
        throw new MrsImageException("Error, could not load any tiles");
      }

      final CoordinateReferenceSystem crs = CRS.decode("EPSG:4326");
      envelope.setCoordinateReferenceSystem(crs);

      // now build a PlanarImage from the raster
      @SuppressWarnings("unused")
      final int type = raster.getTransferType();
      final int bands = raster.getNumBands();
      final int offsets[] = new int[bands];
      for (int i = 0; i < offsets.length; i++)
      {
        offsets[i] = 0;
      }

      final BufferedImage img = RasterUtils.makeBufferedImage(raster);

      final GridCoverageFactory factory = CoverageFactoryFinder.getGridCoverageFactory(null);
      final GridCoverage2D coverage = factory.create(output, img, envelope);

      GeotoolsRasterUtils.saveLocalGeotiff(output + ".tif", coverage, metadata.getDefaultValue(0));
      System.out.println("Wrote output to " + output + ".tif");
      return true;

    }
    catch (final Exception e)
    {
      e.printStackTrace();
    }
    return false;
  }

  @Override
  public int run(final String[] args, Configuration conf, Properties providerProperties) 
  {
    log.info("Export");

    OpImageRegistrar.registerMrGeoOps();
    try
    {
      final Options options = ExportWithVector.createOptions();
      CommandLine line = null;

      try
      {
        final CommandLineParser parser = new PosixParser();
        line = parser.parse(options, args);

        if (line.hasOption("b") &&
            (line.hasOption("t") || line.hasOption("c") || line.hasOption("r")))
        {
          log.debug("Option -b is currently incompatible with -t, -c, and -r");
          throw new ParseException("Incorrect combination of arguments");
        }

        if (line.hasOption("s") && line.hasOption("m"))
        {
          log.debug("Cannot use both -s and -m");
          throw new ParseException("Incorrect combination of arguments");
        }
        if (line.hasOption("v"))
        {
          LoggingUtils.setDefaultLogLevel(LoggingUtils.INFO);
        }
        if (line.hasOption("d"))
        {
          LoggingUtils.setDefaultLogLevel(LoggingUtils.DEBUG);
        }

        if (line.hasOption("l"))
        {
          System.out.println("Using local runner");
          HadoopUtils.setupLocalRunner(conf);
        }

        final String outputbase = line.getOptionValue("o");

        if (line.hasOption("c"))
        {
          maxTiles = Integer.valueOf(line.getOptionValue("c"));
        }

        useRand = line.hasOption("r");
        all = line.hasOption("a");

        singleImage = line.hasOption("s");
        mosaicTiles = line.hasOption("m");
        if (mosaicTiles)
        {
          mosaicTileCount = Integer.valueOf(line.getOptionValue("m"));
        }

        useBounds = line.hasOption("b");
        if (useBounds)
        {
          final String boundsOption = line.getOptionValue("b");
          bounds = parseBounds(boundsOption);
        }

        useTileSet = line.hasOption("t");
        if (useTileSet)
        {
          final String tileIdOption = line.getOptionValue("t");
          final String[] tileIds = tileIdOption.split(",");
          for (final String tileId : tileIds)
          {
            tileset.add(Long.valueOf(tileId));
          }
        }

        int zoomlevel = -1;
        if (line.hasOption("z"))
        {
          zoomlevel = Integer.valueOf(line.getOptionValue("z"));
        }

        if (!singleImage)
        {
          org.apache.commons.io.FileUtils.forceMkdir(new File(outputbase));
        }

        for (final String arg : line.getArgs())
        {
          // The input can be either an image or a vector.
          MrsImagePyramid imagePyramid = null;
          MrsVectorPyramid vectorPyramid = null;
          MrsPyramid pyramid = null;
          try
          {
            imagePyramid = MrsImagePyramid.open(arg, providerProperties);
            pyramid = imagePyramid;
          }
          catch(IOException e)
          {
            imagePyramid = null;
          }

          if (imagePyramid == null)
          {
            try
            {
              vectorPyramid = MrsVectorPyramid.open(arg);
              pyramid = vectorPyramid;
            }
            catch(IOException e)
            {
              vectorPyramid = null;
            }
          }
          if (imagePyramid == null && vectorPyramid == null)
          {
            throw new IOException("Specified input must be either an image or a vector");
          }
          if (zoomlevel <= 0)
          {
            zoomlevel = pyramid.getMaximumLevel();
          }

          int end = zoomlevel;
          if (all)
          {
            end = 1;
          }

          while (zoomlevel >= end)
          {
            final String output = outputbase + (all ? "_" + Integer.toString(zoomlevel) : "");

            MrsImage image = null;
            MrsVector vector = null;
            if (imagePyramid != null)
            {
              image = imagePyramid.getImage(zoomlevel);
            }
            else if (vectorPyramid != null)
            {
              vector = vectorPyramid.getVector(zoomlevel);
            }
            try
            {
              final Set<Long> tiles = calculateTiles(pyramid, zoomlevel);

              if (singleImage)
              {
                if (imagePyramid != null)
                {
                  saveMultipleTiles(output, image, ArrayUtils.toPrimitive(tiles.toArray(new Long[] {})));
                }
                else if (vectorPyramid != null)
                {
                  saveVectorTiles(output, vector, ArrayUtils.toPrimitive(tiles.toArray(new Long[] {})));
                }
              }
              else if (mosaicTiles && mosaicTileCount > 0)
              {
                for (final Long tileid : tiles)
                {
                  final TMSUtils.Tile t = TMSUtils.tileid(tileid, zoomlevel);
                  final Set<Long> tilesToMosaic = new HashSet<Long>();
                  final LongRectangle tileBounds = pyramid.getTileBounds(zoomlevel);
                  for (long ty1 = t.ty; ((ty1 < (t.ty + mosaicTileCount)) && (ty1 <= tileBounds
                      .getMaxY())); ty1++)
                  {
                    for (long tx1 = t.tx; ((tx1 < (t.tx + mosaicTileCount)) && (tx1 <= tileBounds
                        .getMaxX())); tx1++)
                    {
                      tilesToMosaic.add(TMSUtils.tileid(tx1, ty1, zoomlevel));
                    }
                  }
                  final String mosaicOutput = output + "/" + t.ty + "-" + t.tx + "-" +
                      TMSUtils.tileid(t.tx, t.ty, zoomlevel);
                  if (imagePyramid != null)
                  {
                    saveMultipleTiles(mosaicOutput, image,
                      ArrayUtils.toPrimitive(tilesToMosaic.toArray(new Long[] {})));
                  }
                  else if (vectorPyramid != null)
                  {
                    saveVectorTiles(mosaicOutput, vector,
                      ArrayUtils.toPrimitive(tilesToMosaic.toArray(new Long[] {})));
                  }
                }
              }
              else
              {
                for (final Long tileid : tiles)
                {
                  final TMSUtils.Tile tile = TMSUtils.tileid(tileid, zoomlevel);
                  if (imagePyramid != null)
                  {
                    String imageOutput = output + "/" + tile.ty + "-" + tile.tx + "-" +
                        TMSUtils.tileid(tile.tx, tile.ty, zoomlevel);
                    saveSingleTile(imageOutput, image, tile.ty, tile.tx);
                  }
                  else if (vectorPyramid != null)
                  {
                    String vectorOutput = output + "/" + tile.ty + "-" + tile.tx + "-" +
                        TMSUtils.tileid(tile.tx, tile.ty, zoomlevel);
                    long[] tempTiles = new long[] { tileid };
                    saveVectorTiles(vectorOutput, vector, tempTiles);
                  }
                }
              }
            }
            finally
            {
              if (image != null)
              {
                image.close();
              }
              if (vector != null)
              {
                vector.close();
              }
            }

            zoomlevel--;
          }
        }
      }
      catch (final ParseException e)
      {
        new HelpFormatter().printHelp("Export", options);
        return 1;
      }

      return 0;
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }

    return -1;
  }

  private Set<Long> calculateTiles(final MrsPyramid pyramid, int zoomlevel)
  {
    final Set<Long> tiles = new HashSet<Long>();
    //final int zoomlevel = pyramid.getMaximumLevel();

    if (tileset != null)
    {
      return tileset;
    }

    LongRectangle tileBounds = pyramid.getTileBounds(zoomlevel);
    if (mosaicTiles && mosaicTileCount > 0)
    {
      for (long ty = tileBounds.getMinY(); ty <= tileBounds.getMaxY(); ty += mosaicTileCount)
      {
        for (long tx = tileBounds.getMinX(); tx <= tileBounds.getMaxX(); tx += mosaicTileCount)
        {
          tiles.add(TMSUtils.tileid(tx, ty, zoomlevel));
        }
      }

      return tiles;
    }

    if (useBounds)
    {
      return RasterTileMerger.getTileIdsFromBounds(bounds, zoomlevel, pyramid.getTileSize());
    }

    if (useRand && maxTiles > 0)
    {
      for (int i = 0; i < maxTiles; i++)
      {
        final long tx = (long) (tileBounds.getMinX() +
            (Math.random() * (tileBounds.getMaxX() - tileBounds.getMinX())));
        final long ty = (long) (tileBounds.getMinY() +
            (Math.random() * (tileBounds.getMaxY() - tileBounds.getMinY())));

        final long id = TMSUtils.tileid(tx, ty, zoomlevel);
        if (!tiles.contains(id))
        {
          tiles.add(id);
        }
      }

      return tiles;
    }

    for (long ty = tileBounds.getMinY(); ty <= tileBounds.getMaxY(); ty++)
    {
      for (long tx = tileBounds.getMinX(); tx <= tileBounds.getMaxX(); tx++)
      {
        tiles.add(TMSUtils.tileid(tx, ty, zoomlevel));
      }
    }

    return tiles;
  }
}
