package org.mrgeo.data.accumulo.utils;

import org.apache.hadoop.conf.Configuration;

import java.util.Properties;

public class AccumuloConnectionInfo
{
  public static final String ACCUMULO_HAS_CONNECTION_INFO_KEY = "accumulo.has.connection.info";
  public static final String ACCUMULO_ZOOKEEPER_SERVERS_KEY = "accumulo.zookeeper.servers";
  public static final String ACCUMULO_INSTANCE_KEY = "accumulo.instance";
  public static final String ACCUMULO_USERNAME_KEY = "accumulo.username";
  public static final String ACCUMULO_PASSWORD_KEY = "accumulo.password";

  private String zookeeperServers;
  private String instanceName;
  private String userName;
  private String password;

  public static AccumuloConnectionInfo load()
  {
    Properties props = AccumuloConnector.getAccumuloProperties();
    String zookeeperServers = props.getProperty(ACCUMULO_ZOOKEEPER_SERVERS_KEY);
    if (zookeeperServers == null || zookeeperServers.isEmpty())
    {
      return null;
    }
    String instance = props.getProperty(ACCUMULO_INSTANCE_KEY);
    if (instance == null || instance.isEmpty())
    {
      return null;
    }
    String userName = props.getProperty(ACCUMULO_USERNAME_KEY);
    if (userName == null || userName.isEmpty())
    {
      return null;
    }
    String password = props.getProperty(ACCUMULO_PASSWORD_KEY);
    if (password == null || password.isEmpty())
    {
      return null;
    }
    return new AccumuloConnectionInfo(zookeeperServers, instance, userName, password);
  }

  public static AccumuloConnectionInfo load(final Configuration conf)
  {
    // Check to see if connection info exists in this configuration before attempting
    // to load it. Otherwise, we attempt to load it from its default location.
    boolean hasConnectionInfo = conf.getBoolean(ACCUMULO_HAS_CONNECTION_INFO_KEY, false);
    if (!hasConnectionInfo)
    {
      // The config does not contain Accumulo connection info, so we try to load
      // it from the default location.
      return load();
    }
    // The configuration does contain Accumulo connection info, so load it.
    String zookeeperServers = conf.get(ACCUMULO_ZOOKEEPER_SERVERS_KEY);
    if (zookeeperServers == null || zookeeperServers.isEmpty())
    {
      throw new IllegalArgumentException("Missing zookeeper setting for Accumulo");
    }
    String instance = conf.get(ACCUMULO_INSTANCE_KEY);
    if (instance == null || instance.isEmpty())
    {
      throw new IllegalArgumentException("Missing instance setting for Accumulo");
    }
    String userName = conf.get(ACCUMULO_USERNAME_KEY);
    if (userName == null || userName.isEmpty())
    {
      throw new IllegalArgumentException("Missing user name setting for Accumulo");
    }
    // TODO: Encrypt the password. Maybe initially we can just Base64 it? See
    // what the Accumulo plugin does.
    String password = conf.get(ACCUMULO_PASSWORD_KEY);
    if (password == null || password.isEmpty())
    {
      throw new IllegalArgumentException("Missing password setting for Accumulo");
    }
    return new AccumuloConnectionInfo(zookeeperServers, instance, userName, password);
  }

  public void writeToConfig(final Configuration conf)
  {
    conf.setBoolean(ACCUMULO_HAS_CONNECTION_INFO_KEY, true);
    conf.set(ACCUMULO_ZOOKEEPER_SERVERS_KEY, zookeeperServers);
    conf.set(ACCUMULO_INSTANCE_KEY, instanceName);
    conf.set(ACCUMULO_USERNAME_KEY, userName);
    // TODO: Encrypt the password. Maybe initially we can just Base64 it? See
    // what the Accumulo plugin does.
    conf.set(ACCUMULO_PASSWORD_KEY, password);
  }

  public AccumuloConnectionInfo(String zookeeperServers, String instanceName,
                               String userName, String password)
  {
    this.zookeeperServers = zookeeperServers;
    this.instanceName = instanceName;
    this.userName = userName;
    this.password = password;
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
}
