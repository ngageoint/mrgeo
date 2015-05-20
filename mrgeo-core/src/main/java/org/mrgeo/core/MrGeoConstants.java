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

package org.mrgeo.core;

/**
 * 
 * MrGeoConstants is a class for maintaining the constants
 * for the runtime environment of MrGeo.
 * 
 */
public class MrGeoConstants
{

  
  /*
   * Start conatants
   */
  
  
  /*
   * Global configuration keys
   */
  public static final String MRGEO_ENV_HOME = "MRGEO_HOME";
  public static final String MRGEO_SETTINGS = "settings.properties";
  public static final String MRGEO_CONF = "conf/mrgeo.conf";
  public static final String MRGEO_DEVELOPMENT_MODE = "development.mode";

  public static final String MRGEO_CLUSTER = "cluster";
  /*
   * Accumulo configuration keys
   */  
//  public static final String MRGEO_ACC_USER = "accumulo.user";
//  public static final String MRGEO_ACC_ZOO = "accumulo.zooservers";
//  public static final String MRGEO_ACC_PASSWORD = "accumulo.password";
//  public static final String MRGEO_ACC_INST = "accumulo.instance";
//  public static final String MRGEO_ACC_TABLE = "accumulo.table";
//  public static final boolean MRGEO_ACC_PASSB64 = false;

  /*
   * HDFS configuration keys
   */
  public static final String MRGEO_HDFS_RESOURCE = "resource.base";
  public static final String MRGEO_HDFS_SHAPE = "shp.base";
  public static final String MRGEO_HDFS_TSV = "tsv.base";
  public static final String MRGEO_HDFS_VECTOR = "vector.base";
  public static final String MRGEO_HDFS_COLORSCALE = "colorscale.base";
  public static final String MRGEO_HDFS_IMAGE = "image.base";
  public static final String MRGEO_HDFS_KML = "kml.base";

  public static final String MRGEO_HDFS_DISTRIBUTED_CACHE = "distributed.base";

  /* Spark configuration keys
   *
   */
  public static final String MRGEO_USE_KRYO="use.kryo.serialization";
  /*
   * Runtime configuration keys
   */
  public static final String MRGEO_JAR = "jar.path";
  public static final String GDAL_PATH = "gdal.path";

  /*
   * Legion configuration keys
   */
  public static final String MRGEO_LEGION_PROC = "legion.processortype.preference";

  /*
   * Image configuration keys
   */
  public static final String MRGEO_MRS_TILESIZE = "mrsimage.tilesize";
  public static final int MRGEO_MRS_TILESIZE_DEFAULT = 512;

  /*
   * Security classification keys
   */
  public static final String MRGEO_PROTECTION_LEVEL_REQUIRED = "protection.level.required";
  public static final String MRGEO_PROTECTION_LEVEL_DEFAULT = "protection.level.default";
  public static final String MRGEO_PROTECTION_LEVEL = "protectionLevel";

  /**
   * The default constructor for the class is private.  Additionally,
   * nothing can instantiate this class.
   */
  private MrGeoConstants()
  {
    // nothing will instantiate this class - not even within this class  
    throw new AssertionError();
  }
}
