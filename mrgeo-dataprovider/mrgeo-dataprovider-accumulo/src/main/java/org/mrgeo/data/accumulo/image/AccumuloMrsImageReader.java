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

package org.mrgeo.data.accumulo.image;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Decompressor;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.data.DataProviderException;
import org.mrgeo.data.KVIterator;
import org.mrgeo.data.accumulo.metadata.AccumuloMrsImagePyramidMetadataReader;
import org.mrgeo.data.accumulo.utils.AccumuloConnector;
import org.mrgeo.data.accumulo.utils.AccumuloUtils;
import org.mrgeo.data.accumulo.utils.MrGeoAccumuloConstants;
import org.mrgeo.data.image.MrsImagePyramidReaderContext;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.image.MrsImageReader;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.image.MrsImageException;
import org.mrgeo.pyramid.MrsPyramidMetadata;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.HadoopUtils;
import org.mrgeo.utils.LongRectangle;
import org.mrgeo.utils.TMSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.image.Raster;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

public class AccumuloMrsImageReader extends MrsImageReader
{

  // logger for the class
  static final Logger log = LoggerFactory.getLogger(AccumuloMrsImageReader.class);

  // accumulo connector
  protected Connector connector;

  // single threaded scanner
  protected Scanner scanner;

  // multithreaded scanner
  protected BatchScanner batchScanner;

  // compression items
  private CompressionCodec codec;
  private Decompressor decompressor;

  // is this a mock connection
  private boolean mock = false;

  // authorization strings needed for reading - this may need to be set every time
  private String authStr = null;
  private Authorizations auths;

  // metadata of the connection
  private MrsPyramidMetadata metadata = null;

  // table in use
  protected String table = null;

  // zookeeper instance
  private String instance = null;

  // user name
  private String user = null;

  // password for the user
  private String pass = null;

  // zppkeeper servers
  private String zooServers = null;

  // number of threads to use for the connection
  private int numQueryThreads = 2;

  // zoom level being accessed
  protected int zoomLevel = -1;

  // compression is not used right now
  private boolean useCompression = false;

  // metadata reader
  private AccumuloMrsImagePyramidMetadataReader reader;

  // properties needed for connecting to out
  private Properties AMTR_props;

  private AccumuloMrsImageDataProvider provider;
  private MrsImagePyramidReaderContext context;
  private int tileSize = -1;
  private Properties queryProps;
  //private int zoomLevel = -1;

  public AccumuloMrsImageReader(AccumuloMrsImageDataProvider provider,
      MrsImagePyramidReaderContext context) throws IOException {
    String enc = provider.getResolvedName();
    // set the zoom level
    this.zoomLevel = context.getZoomlevel();

    // get the properties for connecting out to Accumulo
    Properties props = null;
    try
    {

      if(AccumuloConnector.isEncoded(enc))
      {
        props = AccumuloConnector.decodeAccumuloProperties(enc);
        table = props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_RESOURCE);
      }
      else
      {
        table = enc;
        props = AccumuloConnector.getAccumuloProperties();
      }
    }
    catch (Exception e)
    {
      log.error("Unable to get Accumulo connection properties", e);
    }

    if (props != null)
    {
      // set up all needed information
      initialize(props);

      try
      {
        reader = new AccumuloMrsImagePyramidMetadataReader(table);
        metadata = reader.read();
      }
      catch (DataProviderException dpe)
      {
        log.warn("no connection to accumulo: " + dpe.getMessage());
      }
      catch (IOException ioe)
      {

      }

      // get the metadata!
      initializeScanners();
    }

    this.provider = provider;
    this.context = context;
    this.tileSize = provider.getMetadataReader().read().getTilesize();

  }

  /**
   * Constructor for instantiating the connection for reading tiles from Accumulo.
   *
   * @param props - properties to be considered for connections and scans
   */
  public AccumuloMrsImageReader(Properties props, AccumuloMrsImageDataProvider provider,
      MrsImagePyramidReaderContext context) throws IOException {
    String enc = provider.getResolvedName();
    if(AccumuloConnector.isEncoded(enc)){
      try
      {
        Properties p = AccumuloConnector.decodeAccumuloProperties(enc);
        table = p.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_RESOURCE);
      }
      catch (IOException | ClassNotFoundException e)
      {
        e.printStackTrace();
      }

    } else {
      table = enc;
    }
    initialize(props);
    initializeScanners();
    this.provider = provider;
    this.context = context;
    this.tileSize = provider.getMetadataReader().read().getTilesize();
    zoomLevel = this.context.getZoomlevel();
    queryProps = new Properties();
    queryProps.setProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_AUTHS, MrGeoAccumuloConstants.MRGEO_ACC_NOAUTHS);
    queryProps.putAll(props);

  }


  /**
   * Constructor configures all the items needed to communicate with Accumulo. This constructor
   * can also have a mock connector instantiated. This is for testing purposes to test the
   * instance of the reader.
   *
   * @param table
   *          the table to use
   * @param numQueryThreads
   *          the number of threads for multithreaded queries
   * @param instance
   *          the accumulo instance in ZooKeeper
   * @param zooServers
   *          the ZooKeepers to use
   * @param user
   *          the user to use to connect to accumulo
   * @param pass
   *          the password to use to connect to accumulo
   */
  public AccumuloMrsImageReader(final String table, final int numQueryThreads, final String instance,
                                final String zooServers, final String user, final String pass)
  {

    // keep track of connection parameters

    String tmp = table;
    if(tmp.startsWith("accumulo:")){
      tmp = tmp.replace("accumulo:", "");
    }

    final String[] els = tmp.split("/");
    if(els.length > 0){
      this.table = els[0];
    }

    if(els.length > 2){
      zoomLevel = Integer.parseInt(els[1]);
    }

    this.instance = instance;
    this.zooServers = zooServers;
    this.user = user;
    this.pass = pass;

    // this.numQueryThreads = numQueryThreads;

    // check if we are in test mode
    if(MrGeoProperties.getInstance().containsKey("accumulo.connector")){
      if(MrGeoProperties.getInstance().getProperty("accumulo.connector").equals("mock")){
        log.info("Using mock connector.");
        mock = true;
      }
    }

    authStr = MrGeoProperties.getInstance().getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_AUTHS);
    auths = AccumuloUtils.createAuthorizationsFromDelimitedString(authStr);

    initializeScanners();

  } // end constructor


  /**
   * Prepare the scanners that end up being used for getting items out of Accumulo
   */
  private void initializeScanners(){

    if(AMTR_props != null){

      String authsStr = AMTR_props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_AUTHS);
      this.auths = AccumuloUtils.createAuthorizationsFromDelimitedString(authsStr);

      if(AMTR_props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_COMPRESS) != null){
        String tmp = AMTR_props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_COMPRESS);
        useCompression = Boolean.parseBoolean(AMTR_props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_COMPRESS));
      }

    }

    try {

      if(useCompression){
        codec = HadoopUtils.getCodec(HadoopUtils.createConfiguration());
        decompressor = CodecPool.getDecompressor(codec);
      } else {
        codec = null;
        decompressor = null;
      }

      // see if we are in a test state
      if(mock){

        // in test mode - use a mock connector
        final MockInstance mi = new MockInstance(this.instance);
        connector = mi.getConnector(this.user, this.pass.getBytes());
        connector.tableOperations().create(this.table);

      } else if(this.instance != null){

        // get a real connector
        connector = AccumuloConnector.getConnector(this.instance,
                                                   this.zooServers,
                                                   this.user,
                                                   this.pass);
        if(useCompression){
          codec = HadoopUtils.getCodec(HadoopUtils.createConfiguration());
          decompressor = CodecPool.getDecompressor(codec);
        } else {
          codec = null;
          decompressor = null;
        }

      } else {

        // we did not get the information needed from the properties objects - so use the configs from the install

        connector = AccumuloConnector.getConnector();

        // TODO: compression items need to be worked out
        codec = null;
        decompressor = null;

      }

      // establish the scanners
      scanner = connector.createScanner(this.table, this.auths);
      batchScanner = connector.createBatchScanner(this.table, this.auths, numQueryThreads);

      if(!mock){

        // I AM MOCKING YOU!!!

        //metadata = loadGenericMetadata();

      }
    }
    catch (final TableNotFoundException e)
    {
      throw new MrsImageException(e);
    }
    catch (final IOException e)
    {
      throw new MrsImageException(e);
    }
    catch (final AccumuloSecurityException e)
    {
      throw new MrsImageException(e);
    }
    catch (final AccumuloException e)
    {
      throw new MrsImageException(e);
    }
    catch (final TableExistsException e)
    {
      throw new MrsImageException(e);
    }

  } // end initializeScanners


  /**
   * Pull together all the needed items for connecting to Accumulo.
   *
   * @param props - the object that has the information for connecting out to Accumulo.
   */
  private void initialize(Properties props){
    Properties tmpProps = null;
    try
    {
      tmpProps = AccumuloConnector.getAccumuloProperties();
    }
    catch (Exception e)
    {
      log.error("Unable to get Accumulo connection properties", e);
      return;
    }
    // initialize properties
    if(AMTR_props == null){
      AMTR_props = new Properties();
    }
    AMTR_props.putAll(tmpProps);

    String authsString = AMTR_props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_AUTHS);

    // pull in all properties for input
    if(props != null){
      if(props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_AUTHS) != null){
        authsString = props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_AUTHS);
      }
      AMTR_props.putAll(props);

    } //else {
    // read in properties from file?
    //}

    // get all needed properties for connecting to Accumulo

    //table = props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_RESOURCE);
    instance = AMTR_props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_RESOURCE);
    zooServers = AMTR_props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_ZOOKEEPERS);
    user = AMTR_props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_USER);
    pass = AMTR_props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_PASSWORD);

    //log.info("auth string = " + AMTR_props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_AUTHS));
    //log.info("provider rolses = " + AMTR_props.getProperty(DataProviderFactory.PROVIDER_PROPERTY_USER_ROLES));

    // authorizations - this will need to be set on a per query basis
    auths = AccumuloUtils.createAuthorizationsFromDelimitedString(authsString);
  } // end initialize


//  @Override
//  public MrsImagePyramidMetadata loadMetadata()
//  {
//    // TODO Auto-generated method stub
//    return null;
//  }

  public long calculateTileCount(){
    int zl = context.getZoomlevel();
    try{
      MrsPyramidMetadata meta = provider.getMetadataReader().read();
      LongRectangle lr = meta.getOrCreateTileBounds(zl);
      long count = (lr.getMaxX() - lr.getMinX() + 1) *
          (lr.getMaxY() - lr.getMinY() + 1);
      
      return count;
    } catch(IOException ioe){
      return -1;
    }
  } // end calculateTileCount

  @Override
  public boolean canBeCached()
  {
    return true;
  }

  protected Raster toNonWritable(byte[] val, CompressionCodec codec, Decompressor decompressor)
      throws IOException
  {
    if(codec == null || decompressor == null){
      return RasterWritable.toRaster(new RasterWritable(val));
    }
    return RasterWritable.toRaster(new RasterWritable(val), codec, decompressor);
  }

  @Override
  public int getTileSize()
  {
    return tileSize;
  }




  /**
   * Cleanup the scanners connected to Accumulo
   */
  @Override
  public void close()
  {
    if(batchScanner != null){
      batchScanner.close();
    }
    if(scanner != null){
      scanner.close();
    }
  } // end close


  /**
   * This will check to see if a tile is in the data store
   *
   * @param key - the item to check for
   * @return the result of the scan
   */
  @Override
  public boolean exists(final TileIdWritable key){

    // TODO: scan for the item
    Raster t = get(key);
    if(t != null){
      return true;
    } else {
      return false;
    }

    //throw new NotImplementedException("AccumuloReader.exists not implemented");

  } // end exist


  /**
   * This is meant to pull back everything from the image.  This is not implemented.
   * @return iterator through all rasters for an image
   */
  @Override
  public KVIterator<TileIdWritable, Raster> get()
  {
    return get(null, null);
  } // end get()


  /**
   * This will get an iterator that will be able to pull images for a rectangle
   * @param tileBounds is a LongRectangle of area of interest
   * @return an iterator over the rasters needed to satisfy request
   */
  @Override
  public KVIterator<TileIdWritable, Raster> get(final LongRectangle tileBounds){

    // TODO: make this work for specific varying ranges that will come out of this request

    throw new NotImplementedException(this.getClass().getName() + ".get from Bounds not implemented");

//    return get(new TileIdWritable(TMSUtils.tileid(tileBounds.getMinX(),
//                                                  tileBounds.getMinY(),
//                                                  zoomLevel)),
//               new TileIdWritable(TMSUtils.tileid(tileBounds.getMaxX(),
//                                                  tileBounds.getMaxY(),
//                                                  zoomLevel)));
  } // end get


  /**
   * This will get an iterator over the bounds of a given bounding box.
   * @param bounds is a bounding box to be used for queries
   */
  @Override
  public KVIterator<Bounds, Raster> get(final Bounds bounds){

    //TODO: make this bounds request work

    TMSUtils.Bounds newBounds = TMSUtils.Bounds.convertOldToNewBounds(bounds);

    TMSUtils.TileBounds tileBounds = TMSUtils.boundsToTile(newBounds,
                                                           zoomLevel,
                                                           getTileSize());

    throw new NotImplementedException("AccumuloReader.get from Bounds not implemented");
  } // end get


  /**
   * Retrieve a tile from the Accumulo instance.  This ignores zoom level and just
   * pulls from rowid.
   *
   * @param key the tile to get
   * @return the raster of the data
   */
  @Override
  public Raster get(final TileIdWritable key)
  {
    log.debug("getting single (no zoom level) tile of id = " + key.get());

    // set the scanner for the tile id
    scanner.setRange(new Range(AccumuloUtils.toRowId(key.get())));
    if(zoomLevel != -1){
      scanner.fetchColumnFamily(new Text(Integer.toString(zoomLevel)));
    }

    final Iterator<Map.Entry<Key, Value>> it = scanner.iterator();
    try
    {
      if (it.hasNext()){
        /*
         * TODO: this assumes that the codecs are in use in the data for the table this will have
         * to come from the information in the table
         */

        final Map.Entry<Key, Value> ent = it.next();
        log.debug("tid = " + ent.getKey().getColumnQualifier().toString() + " image byte size = " + ent.getValue().getSize());

        // done with the scanner
        scanner.close();

        // return the object
        return toNonWritable(ent.getValue().get(), codec, decompressor);

      } else {
        log.info("no image at " + key.get());
      }

      return null; // no item
    }
    catch (final IOException e)
    {
      throw new MrsImageException(e);
    }
  } // end get


  /**
   * Retrieve a series of tiles from the Accumulo store.
   *
   * @param startKey
   *          the start of the list of items to pull
   * @param endKey
   *          the end (inclusive) of items to pull
   * @return an iterator through the list of items to pull
   */
  @Override
  public KVIterator<TileIdWritable, Raster> get(final TileIdWritable startKey,
                                           final TileIdWritable endKey)
  {

    // start
    long startLong;
    if(startKey != null){
      startLong = startKey.get();
    } else {
      startLong = 0;
    }

    // make sure the end is selected correctly
    long endLong;
    if(endKey != null){
      endLong = endKey.get();
    } else {
      if(startLong == 0){
        byte[] b = new byte[8];
        b[0] = 'A';
        endLong = ByteBuffer.wrap(b).getLong();
      } else {
        endLong = startLong;
      }
    }

    // check if we are getting one tile only
    boolean oneTile = false;
    if(startLong == endLong){
      oneTile = true;
    }

    if(!oneTile){
      // this is done to ensure of getting all tiles in the range
      endLong++;
    }

    // set up the keys for the ranges
    Key sKey = new Key(AccumuloUtils.toKey(startLong));
    Key eKey = new Key(AccumuloUtils.toKey(endLong));


    scanner.clearColumns();
    scanner.clearScanIterators();

    /*
     * TODO: how do you know if you are missing items in the tile list - it is possible right now
     * it appears there is a one to one mapping in the tiles and list
     */

    // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    String strAuths = AMTR_props.getProperty(MrGeoAccumuloConstants.MRGEO_ACC_KEY_AUTHS);
    auths = AccumuloUtils.createAuthorizationsFromDelimitedString(strAuths);

    if(connector == null){
      try{
        connector = AccumuloConnector.getConnector();
      } catch(DataProviderException dpe){
        //throw new IOException(dpe.getMessage());
      }
    }

    if (log.isDebugEnabled())
    {
      String authsStr = "";
      for (byte[] b : auths.getAuthorizations())
      {
        authsStr += new String(b) + " ";
      }
      //log.info("startkey = " + startKey.get() + " endkey = " + endKey.get());
      log.debug("accStartkey = " + AccumuloUtils.toLong(sKey.getRow()) +
                " accEndKey = " + AccumuloUtils.toLong(eKey.getRow()) +
                " zoomLevel = " + zoomLevel + "\tonetile = " + oneTile + "\tauths = " + authsStr);
    }

    Range r;
    if(oneTile){
      r = new Range(AccumuloUtils.toRowId(startLong));
    } else {
      r = new Range(sKey, true, eKey, true);
    }

    // set the scanner
    scanner.setRange(r);

    if(zoomLevel != -1){
      scanner.fetchColumnFamily(new Text(Integer.toString(zoomLevel)));
    }

    // create a new iterator out of the batchscanner iterator
    /**
     * it is important to realize that the core does not work like a traditional
     * iterator.  This is just the way they did it.
     */
    return new KVIterator<TileIdWritable, Raster>()
    {
      //final Iterator<Entry<Key, Value>> it = batchScanner.iterator();
      final Iterator<Map.Entry<Key, Value>> it = scanner.iterator();
      Map.Entry<Key, Value> current = null;
      Map.Entry<Key, Value> nextCurrent = null;
      ArrayList<Map.Entry<Key, Value>> vals = new ArrayList<Map.Entry<Key, Value>>();

      // this goes false after reading first element
      private boolean readFirst = true;

      int cnt = 0;

      @Override
      public TileIdWritable currentKey(){
        return new TileIdWritable(AccumuloUtils.toLong(current.getKey().getRow()));
      } // end currentKey

      @Override
      public Raster currentValue()
      {
        try
        {
          return toNonWritable(current.getValue().get(), null, null);
        }
        catch (final IOException e)
        {
          throw new MrsImageException(e);
        }
      } // end currentValue


      /**
       * It is expected for the core that hasNext sets the new value.  This is backwards
       * from how things normally work.
       *
       * @return true if the current value is set and false if there is nothing to set
       */
      @Override
      public boolean hasNext(){

        if(current == null && it.hasNext()){
          current = it.next();
          return true;
        }
        if(it.hasNext()){
          current = it.next();
          return true;
        }
        current = null;
        return false;

      } // ent hasNext


      @Override
      public Raster next()
      {
        try
        {
          if(current == null && it.hasNext()){
            current = it.next();
          }

          log.debug("Current key = " + Hex.encodeHexString(current.getKey().getRow().getBytes()));
          log.debug("Size of value = " + current.getValue().get().length);


          return toNonWritable(current.getValue().get(), null, null);
        }
        catch (final IOException e)
        {
          throw new MrsImageException(e);
        }

      }

      @Override
      public void remove()
      {
        throw new UnsupportedOperationException("iterator is read-only");
      }

    };
  } // end get

  /**
   * Return the zoom level of the data - the is the maximum zoom level of the data
   *
   * @return the maximum zoom level
   */
  @Override
  public int getZoomlevel()
  {
    if(zoomLevel == -1){
      if (metadata != null)
      {
        zoomLevel = metadata.getMaxZoomLevel();
      }

    }
    return zoomLevel;
  } // end getZoomlevel


} // end AccumuloMrsImageReader
