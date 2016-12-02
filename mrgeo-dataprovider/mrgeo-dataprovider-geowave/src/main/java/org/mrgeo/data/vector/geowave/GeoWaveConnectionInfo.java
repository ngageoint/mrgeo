package org.mrgeo.data.vector.geowave;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.conf.Configuration;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class GeoWaveConnectionInfo
{
public static final String GEOWAVE_HAS_CONNECTION_INFO_KEY = "geowave.has.connection.info";
public static final String GEOWAVE_STORENAMES_KEY = "geowave.storenames";
public static final String GEOWAVE_FORCE_BBOX_COMPUTE_KEY = "geowave.force.bbox.compute";
static Logger log = LoggerFactory.getLogger(GeoWaveConnectionInfo.class);
private String[] storeNames;
private boolean forceBboxCompute = false;

public GeoWaveConnectionInfo(String[] storeNames, boolean forceBboxCompute)
{
  if (storeNames != null)
  {
    this.storeNames = new String[storeNames.length];
    for (int i = 0; i < storeNames.length; i++)
    {
      this.storeNames[i] = storeNames[i];
    }
  }
  else
  {
    this.storeNames = null;
  }
  this.forceBboxCompute = forceBboxCompute;
}

public static GeoWaveConnectionInfo load()
{
  Properties props = MrGeoProperties.getInstance();
  String storeNames = props.getProperty(GEOWAVE_STORENAMES_KEY);
  if (storeNames == null || storeNames.isEmpty())
  {
    log.debug("Missing GeoWave connection info - namespaces");
    return null;
  }
  log.debug("Geowave storenames = " + storeNames);
  String strForceBboxCompute = props.getProperty(GEOWAVE_FORCE_BBOX_COMPUTE_KEY);
  if (strForceBboxCompute != null)
  {
    strForceBboxCompute = strForceBboxCompute.trim();
  }
  boolean forceBboxCompute = false;
  if (strForceBboxCompute != null &&
      (strForceBboxCompute.equalsIgnoreCase("true") || strForceBboxCompute.equals("1")))
  {
    forceBboxCompute = true;
  }
  return new GeoWaveConnectionInfo(storeNames.split(","), forceBboxCompute);
}

public static GeoWaveConnectionInfo load(final Configuration conf)
{
  // Check to see if connection info exists in this configuration before attempting
  // to load it. Otherwise, we attempt to load it from its default location.
  boolean hasConnectionInfo = conf.getBoolean(GEOWAVE_HAS_CONNECTION_INFO_KEY, false);
  if (!hasConnectionInfo)
  {
    // The config does not contain GeoWave connection info, so we try to load
    // it from the default location.
    return load();
  }
  // The configuration does contain GeoWave connection info, so load it.
  String storeNames = conf.get(GEOWAVE_STORENAMES_KEY);
  if (storeNames == null || storeNames.isEmpty())
  {
    log.error("Missing GeoWave connection info from configuration - namespaces");
    throw new IllegalArgumentException("Missing namespaces setting for GeoWave");
  }
  String strForceBboxCompute = conf.get(GEOWAVE_FORCE_BBOX_COMPUTE_KEY);
  boolean forceBboxCompute = false;
  if (strForceBboxCompute != null &&
      (strForceBboxCompute.equalsIgnoreCase("true") || strForceBboxCompute.equals("1")))
  {
    forceBboxCompute = true;
  }
  return new GeoWaveConnectionInfo(storeNames.split(","), forceBboxCompute);
}

public static GeoWaveConnectionInfo fromMap(Map<String, String> settings)
{
  String storeNames = settings.get(GEOWAVE_STORENAMES_KEY);
  String strForceBbox = settings.get(GEOWAVE_FORCE_BBOX_COMPUTE_KEY);
  boolean forceBbox = false;
  if (strForceBbox != null)
  {
    forceBbox = Boolean.valueOf(strForceBbox);
  }
  return new GeoWaveConnectionInfo(storeNames.split(","), forceBbox);
}

public void writeToConfig(final Configuration conf)
{
  conf.setBoolean(GEOWAVE_HAS_CONNECTION_INFO_KEY, true);
  conf.set(GEOWAVE_STORENAMES_KEY, StringUtils.join(storeNames, ","));
}

@SuppressFBWarnings(value = "PZLA_PREFER_ZERO_LENGTH_ARRAYS", justification = "Zero length array is valid")
public String[] getStoreNames()
{
  if (storeNames != null)
  {
    String[] result = new String[storeNames.length];
    for (int i = 0; i < storeNames.length; i++)
    {
      result[i] = storeNames[i];
    }
    return result;
  }
  return null;
}

public boolean getForceBboxCompute()
{
  return forceBboxCompute;
}

public Map<String, String> toMap()
{
  Map<String, String> result = new HashMap<String, String>();
  if (storeNames != null)
  {
    result.put(GEOWAVE_STORENAMES_KEY, StringUtils.join(storeNames, ","));
  }
  result.put(GEOWAVE_FORCE_BBOX_COMPUTE_KEY, Boolean.toString(forceBboxCompute));
  return result;
}
}
