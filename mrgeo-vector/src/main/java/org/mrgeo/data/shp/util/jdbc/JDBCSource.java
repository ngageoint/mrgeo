/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.data.shp.util.jdbc;

public class JDBCSource implements java.io.Serializable
{
  private static final long serialVersionUID = 1L;
  public String driver; // JDBC driver class name
  public transient int freeConn = 0; // current number of connections free in
                                     // pool
  public transient int inuseConn = 0; // current number of connections estimated
                                      // in use
  public transient int maxConn = 0; // maximum number of connections (specified)
  public String name; // pool identifier
  protected String password; // user name's password
  public String url; // JDBC database URL
  public String user; // user name for the pool

  public String getDriver()
  {
    return driver;
  }

  public String getName()
  {
    return name;
  }

  public String getUrl()
  {
    return url;
  }

  public String getUser()
  {
    return user;
  }

  public void setPassword(String password)
  {
    this.password = password;
  }

  @Override
  public String toString()
  {
    return name + " " + driver + " " + url + " (" + inuseConn + "+" + freeConn + "=" + maxConn
        + ")";
  }
}
