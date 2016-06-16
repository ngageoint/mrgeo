package org.mrgeo.data.vector.geowave;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.conf.Configuration;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class GeoWaveConnectionInfo
{
  static Logger log = LoggerFactory.getLogger(GeoWaveConnectionInfo.class);

  public static final String GEOWAVE_HAS_CONNECTION_INFO_KEY = "geowave.has.connection.info";
  public static final String GEOWAVE_STORENAME_KEY = "geowave.storename";
  public static final String GEOWAVE_NAMESPACES_KEY = "geowave.namespaces";
  public static final String GEOWAVE_FORCE_BBOX_COMPUTE_KEY = "geowave.force.bbox.compute";

  private String storeName;
  private String[] namespaces;
  private boolean forceBboxCompute = false;

  public static GeoWaveConnectionInfo load()
  {
    Properties props = MrGeoProperties.getInstance();
    String storeName = props.getProperty(GEOWAVE_STORENAME_KEY);
    if (storeName == null || storeName.isEmpty())
    {
      log.info("Missing GeoWave connection info - store name");
      return null;
    }
    String namespaces = props.getProperty(GEOWAVE_NAMESPACES_KEY);
    if (namespaces == null || namespaces.isEmpty())
    {
      log.debug("Missing GeoWave connection info - namespaces");
      return null;
    }
    log.debug("Geowave namespaces = " + namespaces);
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
    return new GeoWaveConnectionInfo(storeName, namespaces.split(","), forceBboxCompute);
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
    String storeName = conf.get(GEOWAVE_STORENAME_KEY);
    if (storeName == null || storeName.isEmpty())
    {
      log.info("Missing GeoWave connection info from configuration - store name");
      throw new IllegalArgumentException("Missing store name setting for GeoWave");
    }
    String namespaces = conf.get(GEOWAVE_NAMESPACES_KEY);
    if (namespaces == null || namespaces.isEmpty())
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
    return new GeoWaveConnectionInfo(storeName, namespaces.split(","), forceBboxCompute);
  }

  public void writeToConfig(final Configuration conf)
  {
    conf.setBoolean(GEOWAVE_HAS_CONNECTION_INFO_KEY, true);
    conf.set(GEOWAVE_STORENAME_KEY, storeName);
    conf.set(GEOWAVE_NAMESPACES_KEY, StringUtils.join(namespaces, ","));
  }

  public GeoWaveConnectionInfo(String storeName, String[] namespaces, boolean forceBboxCompute)
  {
    this.storeName = storeName;
    if (namespaces != null)
    {
      this.namespaces = new String[namespaces.length];
      for (int i=0; i < namespaces.length; i++) {
        this.namespaces[i] = namespaces[i];
      }
    }
    else {
      this.namespaces = null;
    }
    this.forceBboxCompute = forceBboxCompute;
  }

  public String getStoreName()
  {
    return storeName;
  }

  @SuppressFBWarnings(value="PZLA_PREFER_ZERO_LENGTH_ARRAYS", justification = "Zero length array is valid")
  public String[] getNamespaces()
  {
    if (namespaces != null)
    {
      String[] result = new String[namespaces.length];
      for (int i=0; i < namespaces.length; i++) {
        result[i] = namespaces[i];
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
    if (storeName != null)
    {
      result.put(GEOWAVE_STORENAME_KEY, storeName);
    }
    if (namespaces != null)
    {
      result.put(GEOWAVE_NAMESPACES_KEY, StringUtils.join(namespaces, ","));
    }
    result.put(GEOWAVE_FORCE_BBOX_COMPUTE_KEY, Boolean.toString(forceBboxCompute));
    return result;
  }

  public static GeoWaveConnectionInfo fromMap(Map<String, String> settings)
  {
    String storeName = settings.get(GEOWAVE_STORENAME_KEY);
    String namespaces = settings.get(GEOWAVE_NAMESPACES_KEY);
    String strForceBbox = settings.get(GEOWAVE_FORCE_BBOX_COMPUTE_KEY);
    boolean forceBbox = false;
    if (strForceBbox != null)
    {
      forceBbox = Boolean.valueOf(strForceBbox);
    }
    return new GeoWaveConnectionInfo(storeName, namespaces.split(","), forceBbox);
  }
}
