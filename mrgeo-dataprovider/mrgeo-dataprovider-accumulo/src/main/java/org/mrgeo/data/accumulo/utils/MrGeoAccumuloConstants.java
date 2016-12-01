/*
 * Copyright 2009-2016 DigitalGlobe, Inc.
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
 *
 */

package org.mrgeo.data.accumulo.utils;

@SuppressWarnings("squid:S2068") // not a password
public class MrGeoAccumuloConstants
{
  public static final String MRGEO_ACC_PREFIX = "accumulo:";
  public static final String MRGEO_ACC_PREFIX_NC = "accumulo";
  public static final String MRGEO_ACC_ENCODED_PREFIX = "accumuloencoded:";
  public static final String MRGEO_ACC_KEY_ENCODED = "accumuloencoded";
  public static final String MRGEO_ACC_ENCODED_DELIM = "accumuloencoded";
  
  public static final String MRGEO_ACC_KEY_RESOURCE = "accumulo.resource";  
  
  public static final String MRGEO_ACC_METADATA = "METADATA";
  public static final String MRGEO_ACC_CQALL = "ALL";
  
  public static final String MRGEO_ACC_CONF_FILE_NAME = "mrgeo-accumulo.conf";

  public static final String MRGEO_ACC_KEY_PREFIX = "accumulo.";

  public static final String MRGEO_ACC_KEY_INSTANCE = "accumulo.instance";
  public static final String MRGEO_ACC_KEY_ZOOKEEPERS = "accumulo.zookeepers";

  public static final String MRGEO_ACC_KEY_USER = "accumulo.user";
  public static final String MRGEO_ACC_KEY_PASSWORD = "accumulo.password";
  public static final String MRGEO_ACC_KEY_PWENCODED64 = "accumulo.pwencoded64"; // is the password encoded

  public static final String MRGEO_ACC_KEY_VIZ = "accumulo.viz";

  // make sure these are understood
  public static final String MRGEO_ACC_KEY_AUTHS = "accumulo.auths";
  public static final String MRGEO_ACC_KEY_QUERY_AUTHS = "accumulo.queryauths";
  public static final String MRGEO_ACC_NOAUTHS = "";
  public static final String MRGEO_ACC_AUTHS_U = "U";
  
  public static final String MRGEO_ACC_KEY_DEFAULT_WRITE_VIZ = "accumulo.default.write.viz";
  public static final String MRGEO_ACC_KEY_DEFAULT_READ_AUTHS = "accumulo.default.read.auths";
  
  public static final String MRGEO_ACC_KEY_EXCLUDETABLES = "accumulo.excludetables";
  
  public static final String MRGEO_ACC_KEY_INPUT_TABLE = "accumulo.inputtable"; // is the password encoded
  public static final String MRGEO_ACC_KEY_OUTPUT_TABLE = "accumulo.outputtable"; // is the password encoded

  public static final String MRGEO_ACC_KEY_CODEC = "accumulo.codec";
  public static final String MRGEO_ACC_KEY_COMPRESS = "accumulo.compress";
  
  public static final String MRGEO_ACC_KEY_JOBTYPE = "accumulo.jobtype";
  public static final String MRGEO_ACC_VALUE_JOB_BULK = "bulk";
  public static final String MRGEO_ACC_VALUE_JOB_DIRECT = "direct";
  
  public static final String MRGEO_ACC_KEY_BULK_THRESHOLD = "accumulo.bulkthreshold";
  
  public static final String MRGEO_ACC_KEY_WORKDIR = "accumulo.workdir";
  
  public static final String MRGEO_ACC_KEY_ZOOMLEVEL = "accumulo.zoomlevel";

  public static final String MRGEO_ACC_KEY_FORCE_BULK = "accumulo.forcebulk";
  
  public static final String MRGEO_ACC_FILE_NAME_BULK_DONE = "bulkDone";
  public static final String MRGEO_ACC_FILE_NAME_BULK_WORKING = "bulkWorking";


  public static final String MRGEO_KEY_PROTECTIONLEVEL = "protectionLevel";

  public static final int MRGEO_DEFAULT_NUM_REDUCERS = 4;
  
  private MrGeoAccumuloConstants(){
    throw new AssertionError();
  }
  
} // end MrGeoAccumuloConstants
