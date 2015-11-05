package org.mrgeo.data.vector.geowave;

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
  static Logger log = LoggerFactory.getLogger(GeoWaveConnectionInfo.class);

  public static final String GEOWAVE_HAS_CONNECTION_INFO_KEY = "geowave.has.connection.info";
  public static final String GEOWAVE_ZOOKEEPER_SERVERS_KEY = "geowave.zookeeper.servers";
  public static final String GEOWAVE_INSTANCE_KEY = "geowave.instance";
  public static final String GEOWAVE_USERNAME_KEY = "geowave.username";
  public static final String GEOWAVE_PASSWORD_KEY = "geowave.password";
  public static final String GEOWAVE_NAMESPACES_KEY = "geowave.namespaces";
  public static final String GEOWAVE_FORCE_BBOX_COMPUTE_KEY = "geowave.force.bbox.compute";

  private String zookeeperServers;
  private String instanceName;
  private String userName;
  private String password;
  private String[] namespaces;
  private boolean forceBboxCompute = false;

  public static GeoWaveConnectionInfo load()
  {
    Properties props = MrGeoProperties.getInstance();
    String zookeeperServers = props.getProperty(GEOWAVE_ZOOKEEPER_SERVERS_KEY);
    if (zookeeperServers == null || zookeeperServers.isEmpty())
    {
      log.info("Missing GeoWave connection info - zookeeper servers");
      return null;
    }
    String instance = props.getProperty(GEOWAVE_INSTANCE_KEY);
    if (instance == null || instance.isEmpty())
    {
      log.info("Missing GeoWave connection info - instance");
      return null;
    }
    String userName = props.getProperty(GEOWAVE_USERNAME_KEY);
    if (userName == null || userName.isEmpty())
    {
      log.info("Missing GeoWave connection info - user name");
      return null;
    }
    String password = props.getProperty(GEOWAVE_PASSWORD_KEY);
    if (password == null || password.isEmpty())
    {
      log.info("Missing GeoWave connection info - password");
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
    return new GeoWaveConnectionInfo(zookeeperServers, instance, userName,
        password, namespaces.split(","), forceBboxCompute);
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
    String zookeeperServers = conf.get(GEOWAVE_ZOOKEEPER_SERVERS_KEY);
    if (zookeeperServers == null || zookeeperServers.isEmpty())
    {
      log.info("Missing GeoWave connection info from configuration - zookeeper servers");
      throw new IllegalArgumentException("Missing zookeeper setting for GeoWave");
    }
    String instance = conf.get(GEOWAVE_INSTANCE_KEY);
    if (instance == null || instance.isEmpty())
    {
      log.info("Missing GeoWave connection info from configuration - instance");
      throw new IllegalArgumentException("Missing instance setting for GeoWave");
    }
    String userName = conf.get(GEOWAVE_USERNAME_KEY);
    if (userName == null || userName.isEmpty())
    {
      log.info("Missing GeoWave connection info from configuration - user name");
      throw new IllegalArgumentException("Missing user name setting for GeoWave");
    }
    // TODO: Encrypt the password. Maybe initially we can just Base64 it? See
    // what the Accumulo plugin does.
    String password = conf.get(GEOWAVE_PASSWORD_KEY);
    if (password == null || password.isEmpty())
    {
      log.info("Missing GeoWave connection info from configuration - password");
      throw new IllegalArgumentException("Missing password setting for GeoWave");
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
    return new GeoWaveConnectionInfo(zookeeperServers, instance, userName,
        password, namespaces.split(","), forceBboxCompute);
  }

  public void writeToConfig(final Configuration conf)
  {
    conf.setBoolean(GEOWAVE_HAS_CONNECTION_INFO_KEY, true);
    conf.set(GEOWAVE_ZOOKEEPER_SERVERS_KEY, zookeeperServers);
    conf.set(GEOWAVE_INSTANCE_KEY, instanceName);
    conf.set(GEOWAVE_USERNAME_KEY, userName);
    // TODO: Encrypt the password. Maybe initially we can just Base64 it? See
    // what the Accumulo plugin does.
    conf.set(GEOWAVE_PASSWORD_KEY, password);
    conf.set(GEOWAVE_NAMESPACES_KEY, StringUtils.join(namespaces, ","));
  }

  public GeoWaveConnectionInfo(String zookeeperServers, String instanceName,
      String userName, String password, String[] namespaces, boolean forceBboxCompute)
  {
    this.zookeeperServers = zookeeperServers;
    this.instanceName = instanceName;
    this.userName = userName;
    this.password = password;
    this.namespaces = namespaces;
    this.forceBboxCompute = forceBboxCompute;
  }

  public String getZookeeperServers()
  {
    return zookeeperServers;
  }

  public String getInstanceName()
  {
    return instanceName;
  }

  public String getUserName()
  {
    return userName;
  }

  public String getPassword()
  {
    return password;
  }

  public String[] getNamespaces()
  {
    return namespaces;
  }

  public boolean getForceBboxCompute()
  {
    return forceBboxCompute;
  }

  public Map<String, String> toMap()
  {
    Map<String, String> result = new HashMap<String, String>();
    if (instanceName != null)
    {
      result.put(GEOWAVE_INSTANCE_KEY, instanceName);
    }
    if (zookeeperServers != null)
    {
      result.put(GEOWAVE_ZOOKEEPER_SERVERS_KEY, zookeeperServers);
    }
    if (namespaces != null)
    {
      result.put(GEOWAVE_NAMESPACES_KEY, StringUtils.join(namespaces, ","));
    }
    if (userName != null)
    {
      result.put(GEOWAVE_USERNAME_KEY, userName);
    }
    if (password != null)
    {
      result.put(GEOWAVE_PASSWORD_KEY, password);
    }
    result.put(GEOWAVE_FORCE_BBOX_COMPUTE_KEY, Boolean.toString(forceBboxCompute));
    return result;
  }

  public static GeoWaveConnectionInfo fromMap(Map<String, String> settings)
  {
    String instanceName = settings.get(GEOWAVE_INSTANCE_KEY);
    String zookeeperServers = settings.get(GEOWAVE_ZOOKEEPER_SERVERS_KEY);
    String namespaces = settings.get(GEOWAVE_NAMESPACES_KEY);
    String userName = settings.get(GEOWAVE_USERNAME_KEY);
    String password = settings.get(GEOWAVE_PASSWORD_KEY);
    String strForceBbox = settings.get(GEOWAVE_FORCE_BBOX_COMPUTE_KEY);
    boolean forceBbox = false;
    if (strForceBbox != null)
    {
      forceBbox = Boolean.valueOf(strForceBbox);
    }
    return new GeoWaveConnectionInfo(zookeeperServers, instanceName,
                                     userName, password, namespaces.split(","), forceBbox);
  }
}
