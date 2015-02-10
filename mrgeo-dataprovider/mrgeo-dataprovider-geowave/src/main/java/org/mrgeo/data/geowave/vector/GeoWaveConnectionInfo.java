package org.mrgeo.data.geowave.vector;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.mrgeo.core.MrGeoProperties;

public class GeoWaveConnectionInfo
{
  public static final String GEOWAVE_ZOOKEEPER_SERVERS_KEY = "geowave.zookeeper.servers";
  public static final String GEOWAVE_INSTANCE_KEY = "geowave.instance";
  public static final String GEOWAVE_USERNAME_KEY = "geowave.username";
  public static final String GEOWAVE_PASSWORD_KEY = "geowave.password";
  public static final String GEOWAVE_NAMESPACE_KEY = "geowave.namespace";

  private String zookeeperServers;
  private String instanceName;
  private String userName;
  private String password;
  private String namespace;

  public static GeoWaveConnectionInfo load()
  {
    Properties props = MrGeoProperties.getInstance();
    String zookeeperServers = props.getProperty(GEOWAVE_ZOOKEEPER_SERVERS_KEY);
    if (zookeeperServers == null || zookeeperServers.isEmpty())
    {
      return null;
    }
    String instance = props.getProperty(GEOWAVE_INSTANCE_KEY);
    if (instance == null || instance.isEmpty())
    {
      return null;
    }
    String userName = props.getProperty(GEOWAVE_USERNAME_KEY);
    if (userName == null || userName.isEmpty())
    {
      return null;
    }
    String password = props.getProperty(GEOWAVE_PASSWORD_KEY);
    if (password == null || password.isEmpty())
    {
      return null;
    }
    String namespace = props.getProperty(GEOWAVE_NAMESPACE_KEY);
    if (namespace == null || namespace.isEmpty())
    {
      return null;
    }
    return new GeoWaveConnectionInfo(zookeeperServers, instance, userName,
        password, namespace);
  }

  public static GeoWaveConnectionInfo load(final Configuration conf)
  {
    String zookeeperServers = conf.get(GEOWAVE_ZOOKEEPER_SERVERS_KEY);
    if (zookeeperServers == null || zookeeperServers.isEmpty())
    {
      throw new IllegalArgumentException("Missing zookeeper setting for GeoWave");
    }
    String instance = conf.get(GEOWAVE_INSTANCE_KEY);
    if (instance == null || instance.isEmpty())
    {
      throw new IllegalArgumentException("Missing instance setting for GeoWave");
    }
    String userName = conf.get(GEOWAVE_USERNAME_KEY);
    if (userName == null || userName.isEmpty())
    {
      throw new IllegalArgumentException("Missing user name setting for GeoWave");
    }
    // TODO: Encrypt the password. Maybe initially we can just Base64 it? See
    // what the Accumulo plugin does.
    String password = conf.get(GEOWAVE_PASSWORD_KEY);
    if (password == null || password.isEmpty())
    {
      throw new IllegalArgumentException("Missing password setting for GeoWave");
    }
    String namespace = conf.get(GEOWAVE_NAMESPACE_KEY);
    if (namespace == null || namespace.isEmpty())
    {
      throw new IllegalArgumentException("Missing namespace setting for GeoWave");
    }
    return new GeoWaveConnectionInfo(zookeeperServers, instance, userName,
        password, namespace);
  }

  public void writeToConfig(final Configuration conf)
  {
    conf.set(GEOWAVE_ZOOKEEPER_SERVERS_KEY, zookeeperServers);
    conf.set(GEOWAVE_INSTANCE_KEY, instanceName);
    conf.set(GEOWAVE_USERNAME_KEY, userName);
    // TODO: Encrypt the password. Maybe initially we can just Base64 it? See
    // what the Accumulo plugin does.
    conf.set(GEOWAVE_PASSWORD_KEY, password);
    conf.set(GEOWAVE_NAMESPACE_KEY, namespace);
  }

  public GeoWaveConnectionInfo(String zookeeperServers, String instanceName,
      String userName, String password, String namespace)
  {
    this.zookeeperServers = zookeeperServers;
    this.instanceName = instanceName;
    this.userName = userName;
    this.password = password;
    this.namespace = namespace;
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

  public String getNamespace()
  {
    return namespace;
  }
}
