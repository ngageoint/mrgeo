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

package org.mrgeo.cmd.showconfiguration;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.mrgeo.cmd.Command;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.data.ProviderProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Properties;

/**
 * ShowConfiguration is a utility for showing the running configuration of
 * the environment.  This is meant to be a diagnostic of the environment.
 */
public class ShowConfiguration extends Command
{
private static final Logger log = LoggerFactory.getLogger(ShowConfiguration.class);
private FileSystem fs = null;

//private Connector conn = null;
private Properties props = null;

public boolean isHadoopAvailable()
{
  return fs != null;
} // end isHadoopAvailable

public String reportOS()
{
  return "Operating system is " + System.getProperty("os.name");
} // end reportOS

public String reportUser()
{
  return "User is " + System.getProperty("user.name");
} // end reportUser

@SuppressWarnings("squid:S1166") // Exceptions caught and errors printed
public String reportMrGeoSettingsProperties()
{
  StringBuffer sb = new StringBuffer();
  Properties psp = new Properties();
  try (InputStream is = MrGeoProperties.class.getClass().getResourceAsStream(MrGeoConstants.MRGEO_SETTINGS))
  {
    sb.append("Found default configuration file " + MrGeoConstants.MRGEO_SETTINGS + ".\n");
    try
    {
      psp.load(is);
      is.close();
    }
    catch (IOException ioe)
    {
      sb.append("\tProblem loading " + MrGeoConstants.MRGEO_SETTINGS + " file.\n");
    }
    sb.append(reportProperties("\t", psp)).append("\n");
  }
  catch (IOException ignored)
  {
    sb.append("MrGeo default file " + MrGeoConstants.MRGEO_SETTINGS + " does not exist.\n");
  }
  return sb.toString();
} // end getMrGeoSettingsProperties

@SuppressWarnings("squid:S1166") // Exception caught and handled
public boolean isMrGeoSettingsPropertiesAvailable()
{
  try (InputStream is = MrGeoProperties.class.getClass().getResourceAsStream(MrGeoConstants.MRGEO_SETTINGS))
  {
    return true;
  }
  catch (IOException ignored)
  {
  }

  return false;
} // end isMrGeoSettingsPropertiesAvailable

@SuppressWarnings("squid:S1166") // Exception caught and handled
public String reportMrGeoConfInfo()
{
  StringBuilder sb = new StringBuilder();

  sb.append(MrGeoConstants.MRGEO_COMMON_HOME);

  String mgch = System.getenv(MrGeoConstants.MRGEO_COMMON_HOME);
  if (mgch == null)
  {
    sb.append(" environment variable not set for the user running Hadoop.\n");
    mgch = System.getenv(MrGeoConstants.MRGEO_HOME);
    if (mgch != null)
    {
      sb.append(MrGeoConstants.MRGEO_HOME);
      sb.append(" environment variable is set, but is deprecated. Use ");
      sb.append(MrGeoConstants.MRGEO_CONF_DIR);
      sb.append(" and ");
      sb.append(MrGeoConstants.MRGEO_COMMON_HOME);
      sb.append(" instead\n");
    }
  }

  if (mgch != null)
  {
    sb.append(" environment variable points to: ");
    sb.append(mgch);
    sb.append("\n");

    try
    {
      String conf = MrGeoProperties.findMrGeoConf();
      sb.append("Found mrgeo.conf at: ");
      sb.append(conf);
      sb.append("\n");
      props = MrGeoProperties.getInstance();
      sb.append(reportProperties("\t", props));
    }
    catch (IOException e)
    {
      sb.append("Local mrgeo.conf file does not exist.\n");
    }
  }

  return sb.toString();
} // end getMrGeoConfInfo

@SuppressWarnings("squid:S1166") // Exception caught and handled
public boolean existsInHDFS(String p)
{
  try
  {
    return fs.exists(new Path(p));
  }
  catch (IOException ioe)
  {
    return false;
  }

} // end existsInHDFS

@SuppressWarnings("squid:S1166") // Exception caught and handled
public String reportHDFSPath(String p)
{
  StringBuffer sb = new StringBuffer();

  if (props.get(p) == null)
  {
    sb.append("HDFS directory for '" + p + "' is not set.\n");
  }
  else
  {
    sb.append("HDFS image directory: " + props.get(p));
    try
    {
      if (fs.exists(new Path((String) props.get(p))))
      {
        sb.append("\tExists: ");
        FileStatus fstat = fs.getFileStatus(new Path((String) props.get(p)));
        sb.append("\tuser.group = " + fstat.getOwner() + "." + fstat.getGroup());
        FsPermission fsperm = fstat.getPermission();
        sb.append("\tu: " + fsperm.getUserAction());
        sb.append("\tg: " + fsperm.getGroupAction());
        sb.append("\to: " + fsperm.getOtherAction() + "\n");

      }
      else
      {
        sb.append("\tDoes not exist.\n");
      }
    }
    catch (IOException ioe)
    {
      sb.append("\tDoes not exist.\n");
    }
  }
  return sb.toString();
} // end showHDFSPath

public String reportProperties(String pad, Properties p)
{
  StringBuffer sb = new StringBuffer();
  ArrayList<String> arrayKeys = new ArrayList<String>();
  Enumeration<Object> keys = p.keys();

  while (keys.hasMoreElements())
  {
    arrayKeys.add(keys.nextElement().toString());
  }

  Collections.sort(arrayKeys);

  for (String k : arrayKeys)
  {
    String kstr = String.format("%45s", k);
    sb.append(pad + kstr + "\t= " + p.getProperty(k) + "\n");
  }

  return sb.toString();
} // end showProperties

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "File() used for existence")
public boolean isJobJarAvailable()
{
  String jj = props.getProperty(MrGeoConstants.MRGEO_JAR);
  if (jj == null)
  {
    return false;
  }
  File file = new File(jj);
  return file.exists();

} // end isJobJarAvailable

public String reportJobJar()
{
  StringBuilder sb = new StringBuilder();
  String jj = props.getProperty(MrGeoConstants.MRGEO_JAR);
  sb.append("Jar for hadoop jobs ");
  if (jj == null)
  {
    sb.append("is not assigned.\n");
    return sb.toString();
  }

  sb.append(jj).append(" ");
  if (isJobJarAvailable())
  {
    sb.append(" Exists!\n");
  }
  else
  {
    sb.append(" does not exist!\n");
  }
  return sb.toString();
} // end reportJobJar

public String buildReport()
{
  StringBuffer sb = new StringBuffer();

  sb.append("Environment information for MrGeo configuration:\n\n");

  // basics
  sb.append(reportOS()).append("\n");
  sb.append(reportUser()).append("\n");

  // check on JBoss settings
  if (isMrGeoSettingsPropertiesAvailable())
  {
    sb.append(reportMrGeoSettingsProperties());
  }
  else
  {
    sb.append(MrGeoConstants.MRGEO_SETTINGS + " is not available.\n");
  }

  // MrGeo info
  if (isMrGeoConfAvailable())
  {
    sb.append(reportMrGeoConfInfo()).append("\n");
  }

  // HDFS checks
  if (props != null && fs != null)
  {
    sb.append(reportHDFSPath(MrGeoConstants.MRGEO_HDFS_COLORSCALE));
    sb.append(reportHDFSPath(MrGeoConstants.MRGEO_HDFS_IMAGE));
    sb.append(reportHDFSPath(MrGeoConstants.MRGEO_HDFS_KML));
    sb.append(reportHDFSPath(MrGeoConstants.MRGEO_HDFS_TSV));
    sb.append(reportHDFSPath(MrGeoConstants.MRGEO_HDFS_VECTOR));
  }

  // look for job jar
  sb.append(reportJobJar());
  sb.append("\n");

  // if Accumulo is in the configuration - check on Accumulo
//    if(isAccumuloUsed()){
//      sb.append(reportAccumulo());
//
//    }

  return sb.toString();
} // end buildReport

@Override
public int run(String[] args, Configuration conf, ProviderProperties providerProperties)
{
  initialize(conf);
  System.out.println(buildReport());
  return 0;
}

private void initialize(Configuration conf)
{
  try
  {
    fs = FileSystem.get(conf);
    //fs = HadoopFileUtils.getFileSystem();
  }
  catch (IOException e)
  {
    log.error("Exception thrown", e);
    System.out.println("Hadoop file system not available.");
  }
  props = MrGeoProperties.getInstance();

} // end initialize

@SuppressWarnings("squid:S1166") // Exception caught and handled
private boolean isMrGeoConfAvailable()
{

  try
  {
    MrGeoProperties.findMrGeoConf();
    return true;
  }
  catch (IOException ignored)
  {
  }

  return false;
} // end isMrGeoConfAvailable

} // end ShowConfiguration
