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

package org.mrgeo.hdfs.vector.shp.util.jdbc;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("unchecked")
public class JDBCConnection
{
  @SuppressWarnings("rawtypes")
  private List batchList;
  private JDBCConnectionImpl.JDBCPool connectionPool; // the connection pool
  private CallableStatement cstmt = null;
  // primary variables
  private String db_key; // database instance name
  private JDBCConnectionImpl impl = null; // the connection
  private boolean inUse; // flag: is there an open ResultSet in use?

  // misc vars
  private String lastSql = null;
  private int maxRows = 0;
  private boolean notify = false;
  private ResultSet rs = null;
  private long startTime = 0;

  // JDBC statement and resultset
  private Statement stmt = null;
  private long timeout = 0;

  // constructors: get connection to database pool
  public JDBCConnection()
  {
    this(null);
  }

  public JDBCConnection(String db_key)
  {
    this.db_key = db_key;
    connectionPool = JDBCConnectionImpl.JDBCPool.getInstance();
    notify = JDBCConnectionImpl.JDBCPool.getDebug();
    inUse = false;
  }

  @SuppressWarnings("rawtypes")
  public void addBatchRequest(String sql)
  {
    if (batchList == null)
      batchList = new ArrayList();
    batchList.add(sql);
  }

  @SuppressWarnings("rawtypes")
  public void addBatchRequest(String sql, Object[] args)
  {
    if (batchList == null)
      batchList = new ArrayList();
    String temp = "" + sql;
    if (args != null)
    {
      int n = 0;
      while (temp.indexOf("?") != -1 && args.length > n)
      {
        temp = temp.replaceFirst("\\?", "" + args[n]);
        n++;
      }
    }
    batchList.add(temp);
  }

  public void clearBatchRequest()
  {
    batchList = null;
  }

  // close request and free resources
  public synchronized void closeRequest()
  {
    try
    {
      if (rs != null)
      {
        rs.close();
      }
      if (stmt != null)
      {
        stmt.close();
      }
      if (impl != null)
      {
        connectionPool.releaseImpl(impl);
      }
    }
    catch (SQLException e)
    {
      e.printStackTrace(System.err);
    }
    finally
    {
      rs = null;
      stmt = null;
      impl = null;
      inUse = false;
    }
  }

  // free resources when object is destroyed
  @Override
  protected void finalize()
  {
    if (inUse)
      closeRequest();
  }

  public JDBCSource getJDBCSource()
  {
    return connectionPool.getJDBCSource((db_key == null) ? connectionPool.getDefaultDatabase()
        : db_key);
  }

  // return the result set of the request
  public ResultSet getResultSet()
  {
    return rs;
  }

  // send a ping (test the connection)
//  @SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED", justification = "Just need a create, not using it.")
//  public synchronized void ping() throws SQLException, JDBCPoolException
//  {
//    if (inUse)
//      closeRequest();
//    impl = connectionPool.acquireImpl(db_key, timeout);
//    Connection temp = impl.getConnection();
//    temp.createStatement(); // 1st test
//    closeRequest();
//  }

  // returns raw sql string for prepared statements
  private String preparedSQL(String originalSQL)
  {
    try
    {
      // int start = temp.indexOf("PreparedStatement");
      // start = temp.indexOf(" ", start)+1;
      // int finish = temp.lastIndexOf("[");
      // temp = temp.substring(start, finish);
      // if (temp.startsWith("INSERT")) temp += ")";
      return originalSQL; // temp;
    }
    catch (Exception e)
    {
      return originalSQL;
    }
  }

  public synchronized void requery() throws SQLException, JDBCPoolException
  {
    closeRequest();
    if (lastSql != null)
      sendRequest(lastSql);
  }

  public int[] sendBatchRequest(boolean returnResultSet) throws SQLException, JDBCPoolException
  {
    if (batchList == null || batchList.size() == 0)
      throw new SQLException("No batch statements defined.");
    if (inUse)
      closeRequest();
    if (notify)
      startTime = System.currentTimeMillis();
    int[] result;
    String sqlString = null;
    impl = connectionPool.acquireImpl(db_key, timeout);
    Connection dbConn = impl.getConnection();
    stmt = impl.getConnection().createStatement();
    if (maxRows > 0)
      stmt.setMaxRows(maxRows);
    try
    {
      sqlString = (String) batchList.get(batchList.size() - 1);
      if (returnResultSet)
      {
        if (batchList.size() > 1)
        {
          for (int i = 0; i < batchList.size() - 1; i++)
          {
            stmt.addBatch((String) batchList.get(i));
          }
          result = stmt.executeBatch();
          stmt = null;
          stmt = dbConn.createStatement();
          rs = stmt.executeQuery((String) batchList.get(batchList.size() - 1)); // last
                                                                                // is
                                                                                // conventional
                                                                                // query
        }
        else
        {
          result = null;
          rs = stmt.executeQuery((String) batchList.get(0)); // only
                                                             // conventional
                                                             // query
        }
      }
      else
      {
        for (int i = 0; i < batchList.size(); i++)
        {
          stmt.addBatch((String) batchList.get(i));
        }
        result = stmt.executeBatch();
      }
    }
    catch (SQLException e)
    {
      if (notify)
        JDBCConnectionImpl.JDBCPool.notifyJDBCQuery(db_key, sqlString, -1);
      throw e;
    }
    lastSql = sqlString;
    inUse = true;
    if (notify)
      JDBCConnectionImpl.JDBCPool.notifyJDBCQuery(
        db_key, sqlString, System.currentTimeMillis() - startTime);
    return result;
  }

  // send a callable statement request (e.g. insert) to a SQLServer database via
  // a Stored Procedure
  // procedure call returns Primary Key of a numeric data type value
  // representing the record inserted
  public synchronized long sendMSSQLSPInsert(String sqlString, Object[] args) throws SQLException,
      JDBCPoolException
  {
    if (inUse)
      closeRequest();
    if (notify)
      startTime = System.currentTimeMillis();
    impl = connectionPool.acquireImpl(db_key, timeout);
    cstmt = impl.getConnection().prepareCall(sqlString);
    cstmt.registerOutParameter(1, Types.NUMERIC);

    if (args != null)
    {
      for (int i = 0; i < args.length; i++)
      {
        if (args[i] == null)
        {
          cstmt.setNull(i + 2, Types.NULL);
        }
        else if (args[i] instanceof String)
        {
          cstmt.setString(i + 2, (String) args[i]);
        }
        else if (args[i] instanceof Long)
        {
          cstmt.setLong(i + 2, ((Long) args[i]).longValue());
        }
        else if (args[i] instanceof Integer)
        {
          cstmt.setInt(i + 2, ((Integer) args[i]).intValue());
        }
        else if (args[i] instanceof Double)
        {
          cstmt.setDouble(i + 2, ((Double) args[i]).doubleValue());
        }
        else if (args[i] instanceof Boolean)
        {
          cstmt.setBoolean(i + 2, ((Boolean) args[i]).booleanValue());
        }
        else if (args[i] instanceof Character)
        {
          cstmt.setString(i + 2, ((Character) args[i]).toString());
        }
        else if (args[i] instanceof Byte)
        {
          cstmt.setByte(i + 2, ((Byte) args[i]).byteValue());
        }
        else if (args[i] instanceof byte[])
        {
          cstmt.setBytes(i + 2, (byte[]) args[i]);
        }
        else if (args[i] instanceof java.sql.Date)
        {
          cstmt.setDate(i + 2, (java.sql.Date) args[i]);
        }
        else if (args[i] instanceof java.sql.Timestamp)
        {
          cstmt.setTimestamp(i + 2, (java.sql.Timestamp) args[i]);
        }
        else if (args[i] instanceof java.sql.Time)
        {
          cstmt.setTime(i + 2, (java.sql.Time) args[i]);
        }
      }
    }
    long id = -1;
    try
    {
      cstmt.execute();
      id = cstmt.getLong(1);
      cstmt.close();
    }
    catch (SQLException e)
    {
      if (notify)
        JDBCConnectionImpl.JDBCPool.notifyJDBCQuery(db_key, preparedSQL(sqlString), -1);
      throw e;
    }
    inUse = true;
    rs = null;
    if (notify)
      JDBCConnectionImpl.JDBCPool.notifyJDBCQuery(db_key, preparedSQL(sqlString), 
        System.currentTimeMillis() - startTime);
    return id;
  }

  // send a prepared query request (e.g. select) to the database
  public synchronized void sendMSSQLSPRequest(String sqlString, Object[] args) throws SQLException,
      JDBCPoolException
  {
    if (inUse)
      closeRequest();
    if (notify)
      startTime = System.currentTimeMillis();
    impl = connectionPool.acquireImpl(db_key, timeout);
    cstmt = impl.getConnection().prepareCall(sqlString);

    if (args != null)
    {
      for (int i = 0; i < args.length; i++)
      {
        if (args[i] == null)
        {
          cstmt.setNull(i + 1, Types.NULL);
        }
        else if (args[i] instanceof String)
        {
          cstmt.setString(i + 1, (String) args[i]);
        }
        else if (args[i] instanceof Long)
        {
          cstmt.setLong(i + 1, ((Long) args[i]).longValue());
        }
        else if (args[i] instanceof Integer)
        {
          cstmt.setInt(i + 1, ((Integer) args[i]).intValue());
        }
        else if (args[i] instanceof Double)
        {
          cstmt.setDouble(i + 1, ((Double) args[i]).doubleValue());
        }
        else if (args[i] instanceof Boolean)
        {
          cstmt.setBoolean(i + 1, ((Boolean) args[i]).booleanValue());
        }
        else if (args[i] instanceof Character)
        {
          cstmt.setString(i + 1, ((Character) args[i]).toString());
        }
        else if (args[i] instanceof Byte)
        {
          cstmt.setByte(i + 1, ((Byte) args[i]).byteValue());
        }
        else if (args[i] instanceof byte[])
        {
          cstmt.setBytes(i + 1, (byte[]) args[i]);
        }
        else if (args[i] instanceof java.sql.Date)
        {
          cstmt.setDate(i + 1, (java.sql.Date) args[i]);
        }
        else if (args[i] instanceof java.sql.Timestamp)
        {
          cstmt.setTimestamp(i + 1, (java.sql.Timestamp) args[i]);
        }
        else if (args[i] instanceof java.sql.Time)
        {
          cstmt.setTime(i + 1, (java.sql.Time) args[i]);
        }
      }
    }
    if (maxRows > 0)
      cstmt.setMaxRows(maxRows);
    try
    {
      rs = cstmt.executeQuery();
    }
    catch (SQLException e)
    {
      if (notify)
        JDBCConnectionImpl.JDBCPool.notifyJDBCQuery(db_key, preparedSQL(sqlString), -1);
      throw e;
    }
    lastSql = sqlString;
    inUse = true;
    if (notify)
      JDBCConnectionImpl.JDBCPool.notifyJDBCQuery(db_key, preparedSQL(sqlString), 
        System.currentTimeMillis() - startTime);
  }

  // send a callable statement request (e.g. insert) to a SQLServer database via
  // a Stored Procedure
  // procedure call returns Primary Key of a numeric data type value
  // representing the record inserted
  public synchronized long sendMSSQLSPUpdate(String sqlString, Object[] args) throws SQLException,
      JDBCPoolException
  {
    if (inUse)
      closeRequest();
    if (notify)
      startTime = System.currentTimeMillis();
    impl = connectionPool.acquireImpl(db_key, timeout);
    cstmt = impl.getConnection().prepareCall(sqlString);
    cstmt.registerOutParameter(1, Types.NUMERIC);

    if (args != null)
    {
      for (int i = 0; i < args.length; i++)
      {
        if (args[i] == null)
        {
          cstmt.setNull(i + 2, Types.NULL);
        }
        else if (args[i] instanceof String)
        {
          cstmt.setString(i + 2, (String) args[i]);
        }
        else if (args[i] instanceof Long)
        {
          cstmt.setLong(i + 2, ((Long) args[i]).longValue());
        }
        else if (args[i] instanceof Integer)
        {
          cstmt.setInt(i + 2, ((Integer) args[i]).intValue());
        }
        else if (args[i] instanceof Double)
        {
          cstmt.setDouble(i + 2, ((Double) args[i]).doubleValue());
        }
        else if (args[i] instanceof Boolean)
        {
          cstmt.setBoolean(i + 2, ((Boolean) args[i]).booleanValue());
        }
        else if (args[i] instanceof Character)
        {
          cstmt.setString(i + 2, ((Character) args[i]).toString());
        }
        else if (args[i] instanceof Byte)
        {
          cstmt.setByte(i + 2, ((Byte) args[i]).byteValue());
        }
        else if (args[i] instanceof byte[])
        {
          cstmt.setBytes(i + 2, (byte[]) args[i]);
        }
        else if (args[i] instanceof java.sql.Date)
        {
          cstmt.setDate(i + 2, (java.sql.Date) args[i]);
        }
        else if (args[i] instanceof java.sql.Timestamp)
        {
          cstmt.setTimestamp(i + 2, (java.sql.Timestamp) args[i]);
        }
        else if (args[i] instanceof java.sql.Time)
        {
          cstmt.setTime(i + 2, (java.sql.Time) args[i]);
        }
      }
    }
    long rows = -1;
    try
    {
      cstmt.execute();
      rows = cstmt.getLong(1);
      cstmt.close();
    }
    catch (SQLException e)
    {
      if (notify)
        JDBCConnectionImpl.JDBCPool.notifyJDBCQuery(db_key, preparedSQL(sqlString), -1);
      throw e;
    }
    inUse = true;
    rs = null;
    if (notify)
      JDBCConnectionImpl.JDBCPool.notifyJDBCQuery(db_key, preparedSQL(sqlString), 
        System.currentTimeMillis() - startTime);
    return rows;
  }

  // send a query request (e.g. select) to the database
  public synchronized void sendRequest(String sqlString) throws SQLException, JDBCPoolException
  {
    if (inUse)
      closeRequest();
    if (notify)
      startTime = System.currentTimeMillis();
    impl = connectionPool.acquireImpl(db_key, timeout);
    stmt = impl.getConnection().createStatement();
    if (maxRows > 0)
      stmt.setMaxRows(maxRows);
    try
    {
      rs = stmt.executeQuery(sqlString);
    }
    catch (SQLException e)
    {
      // if (logErrors) log.error(sqlString + " (FAILED!)", e);
      if (notify)
        JDBCConnectionImpl.JDBCPool.notifyJDBCQuery(db_key, sqlString, -1);
      throw e;
    }
    lastSql = sqlString;
    inUse = true;
    if (notify)
      JDBCConnectionImpl.JDBCPool.notifyJDBCQuery(db_key, sqlString, 
        System.currentTimeMillis() - startTime);
  }

  // send a prepared query request (e.g. select) to the database
  public synchronized void sendRequest(String sqlString, Object[] args) throws SQLException,
      JDBCPoolException
  {
    if (inUse)
      closeRequest();
    if (notify)
      startTime = System.currentTimeMillis();
    impl = connectionPool.acquireImpl(db_key, timeout);
    stmt = impl.getConnection().prepareStatement(sqlString);
    if (args != null)
    {
      for (int i = 0; i < args.length; i++)
      {
        if (args[i] == null)
        {
          ((PreparedStatement) stmt).setNull(i + 1, Types.NULL);
        }
        else if (args[i] instanceof String)
        {
          ((PreparedStatement) stmt).setString(i + 1, (String) args[i]);
        }
        else if (args[i] instanceof Long)
        {
          ((PreparedStatement) stmt).setLong(i + 1, ((Long) args[i]).longValue());
        }
        else if (args[i] instanceof Integer)
        {
          ((PreparedStatement) stmt).setInt(i + 1, ((Integer) args[i]).intValue());
        }
        else if (args[i] instanceof Double)
        {
          ((PreparedStatement) stmt).setDouble(i + 1, ((Double) args[i]).doubleValue());
        }
        else if (args[i] instanceof Boolean)
        {
          ((PreparedStatement) stmt).setBoolean(i + 1, ((Boolean) args[i]).booleanValue());
        }
        else if (args[i] instanceof Character)
        {
          ((PreparedStatement) stmt).setString(i + 1, ((Character) args[i]).toString());
        }
        else if (args[i] instanceof Byte)
        {
          ((PreparedStatement) stmt).setByte(i + 1, ((Byte) args[i]).byteValue());
        }
        else if (args[i] instanceof byte[])
        {
          ((PreparedStatement) stmt).setBytes(i + 1, (byte[]) args[i]);
        }
        else if (args[i] instanceof java.sql.Date)
        {
          ((PreparedStatement) stmt).setDate(i + 1, (java.sql.Date) args[i]);
        }
        else if (args[i] instanceof java.sql.Timestamp)
        {
          ((PreparedStatement) stmt).setTimestamp(i + 1, (java.sql.Timestamp) args[i]);
        }
        else if (args[i] instanceof java.sql.Time)
        {
          ((PreparedStatement) stmt).setTime(i + 1, (java.sql.Time) args[i]);
        }
      }
    }
    if (maxRows > 0)
      stmt.setMaxRows(maxRows);
    try
    {
      rs = ((PreparedStatement) stmt).executeQuery();
    }
    catch (SQLException e)
    {
      if (notify)
        JDBCConnectionImpl.JDBCPool.notifyJDBCQuery(db_key, preparedSQL(sqlString), -1);
      throw e;
    }
    lastSql = sqlString;
    inUse = true;
    if (notify)
      JDBCConnectionImpl.JDBCPool.notifyJDBCQuery(db_key, preparedSQL(sqlString), 
        System.currentTimeMillis() - startTime);
  }

  // send an update request (e.g. update, delete, or insert) to the database
  public synchronized int sendUpdate(String sqlString) throws SQLException, JDBCPoolException
  {
    if (inUse)
      closeRequest();
    if (notify)
      startTime = System.currentTimeMillis();
    impl = connectionPool.acquireImpl(db_key, timeout);
    stmt = impl.getConnection().createStatement();
    int rows = -1;
    try
    {
      rows = stmt.executeUpdate(sqlString);
    }
    catch (SQLException e)
    {
      if (notify)
        JDBCConnectionImpl.JDBCPool.notifyJDBCQuery(db_key, sqlString, -1);
      throw e;
    }
    inUse = true;
    rs = null;
    if (notify)
      JDBCConnectionImpl.JDBCPool.notifyJDBCQuery(db_key, sqlString, 
        System.currentTimeMillis() - startTime);
    return rows;
  }

  // send a prepared update request (e.g. update, delete, or insert) to the
  // database
  public synchronized int sendUpdate(String sqlString, Object[] args) throws SQLException,
      JDBCPoolException
  {
    if (inUse)
      closeRequest();
    if (notify)
      startTime = System.currentTimeMillis();
    impl = connectionPool.acquireImpl(db_key, timeout);
    stmt = impl.getConnection().prepareStatement(sqlString);
    if (args != null)
    {
      for (int i = 0; i < args.length; i++)
      {
        if (args[i] == null)
        {
          ((PreparedStatement) stmt).setNull(i + 1, Types.NULL);
        }
        else if (args[i] instanceof String)
        {
          ((PreparedStatement) stmt).setString(i + 1, (String) args[i]);
        }
        else if (args[i] instanceof Long)
        {
          ((PreparedStatement) stmt).setLong(i + 1, ((Long) args[i]).longValue());
        }
        else if (args[i] instanceof Integer)
        {
          ((PreparedStatement) stmt).setInt(i + 1, ((Integer) args[i]).intValue());
        }
        else if (args[i] instanceof Double)
        {
          ((PreparedStatement) stmt).setDouble(i + 1, ((Double) args[i]).doubleValue());
        }
        else if (args[i] instanceof Boolean)
        {
          ((PreparedStatement) stmt).setBoolean(i + 1, ((Boolean) args[i]).booleanValue());
        }
        else if (args[i] instanceof Character)
        {
          ((PreparedStatement) stmt).setString(i + 1, ((Character) args[i]).toString());
        }
        else if (args[i] instanceof Byte)
        {
          ((PreparedStatement) stmt).setByte(i + 1, ((Byte) args[i]).byteValue());
        }
        else if (args[i] instanceof byte[])
        {
          ((PreparedStatement) stmt).setBytes(i + 1, (byte[]) args[i]);
        }
        else if (args[i] instanceof java.sql.Date)
        {
          ((PreparedStatement) stmt).setDate(i + 1, (java.sql.Date) args[i]);
        }
        else if (args[i] instanceof java.sql.Timestamp)
        {
          ((PreparedStatement) stmt).setTimestamp(i + 1, (java.sql.Timestamp) args[i]);
        }
        else if (args[i] instanceof java.sql.Time)
        {
          ((PreparedStatement) stmt).setTime(i + 1, (java.sql.Time) args[i]);
        }
      }
    }
    int rows = -1;
    try
    {
      rows = ((PreparedStatement) stmt).executeUpdate();
    }
    catch (SQLException e)
    {
      if (notify)
        JDBCConnectionImpl.JDBCPool.notifyJDBCQuery(db_key, preparedSQL(sqlString), -1);
      throw e;
    }
    inUse = true;
    rs = null;
    if (notify)
      JDBCConnectionImpl.JDBCPool.notifyJDBCQuery(db_key, preparedSQL(sqlString), 
        System.currentTimeMillis() - startTime);
    return rows;
  }

  // set max rows (limit)
  public void setMaxRows(int max)
  {
    maxRows = max;
  }

  public void setNotify(boolean value)
  {
    notify = value;
  }

  // set timeout
  public void setTimeout(long milliseconds)
  {
    timeout = milliseconds;
  }
}
