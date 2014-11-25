/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.data.shp.util.jdbc;

/**
 *
 */
public interface JDBCListener
{

  /**
   * @param source
   * @param message
   */
  public void notifyJDBCMessage(String key, String message);

  /**
   * @param source
   * @param sql
   * @param duration
   */
  public void notifyJDBCQuery(String key, String sql, long duration);
}
