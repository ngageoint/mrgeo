/*
 * Copyright 2009-2015 DigitalGlobe, Inc.
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
 */

package org.mrgeo.data.shp.util.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;

@SuppressWarnings("unchecked")
public class JDBCConnectionImpl
{
  // Singleton patternned class [A Singleton is a class with just one instance.
  // Other objects can get a reference to the single instance through a static
  // method (class method).]
  public static class JDBCPool
  {
    private static JDBCPool _instance; // class instance
    private static boolean debug = false;
    @SuppressWarnings("rawtypes")
    private static List listeners = null; // JDBCListeners, if any

    @SuppressWarnings("rawtypes")
    public static void addListener(JDBCListener listener)
    {
      if (listener == null)
        return;
      if (listeners == null)
        listeners = new ArrayList(1);
      if (listeners.contains(listener))
        return;
      listeners.add(listener);
    }

    public static boolean getDebug()
    {
      return debug;
    }

    // Singleton getter utilizing Double Checked Locking pattern
    public static JDBCPool getInstance()
    {
      if (_instance == null)
      {
        synchronized (JDBCPool.class)
        {
          if (_instance == null)
            _instance = new JDBCPool();
        }
      }
      return _instance;
    }

    public static void setDebug(boolean value)
    {
      debug = value;
    }

    private String defaultDbKey = null; // default database

    @SuppressWarnings("rawtypes")
    private Map poolMap = new HashMap(); // dictionary of database names with
                                         // corresponding list of free
                                         // connections

    @SuppressWarnings("rawtypes")
    private Map sourceMap = new HashMap(); // dictionary of database names with
                                           // corresponding connection
                                           // parameters

    @SuppressWarnings("rawtypes")
    private Map waitMap = new HashMap(); // dictionary of database names with
                                         // corresponding list of wait
                                         // semaphores

    // private empty constructor
    private JDBCPool()
    {
    }

    public JDBCConnectionImpl acquireImpl(String db_key) throws SQLException, JDBCPoolException
    {
      return acquireImpl(db_key, 0);
    }

    public JDBCConnectionImpl acquireImpl(String db_key, long timeout) throws SQLException,
        JDBCPoolException
    {
      String key = db_key;
      long to = timeout;
      
      if (key == null)
        key = defaultDbKey; // use default if dbName is null
      if (key == null)
        throw new JDBCPoolException("Attempted AcquireImpl With Null DBName!");
      if (defaultDbKey == null)
        defaultDbKey = key; // set default
      JDBCConnectionImpl temp = null;
      long elapsed = 0;
      long remaining = 0;
      long start = System.currentTimeMillis();
      if (to < 0)
        to = 0;
      // boolean priority = false;
      while (temp == null && elapsed <= to)
      {
        temp = acquireInternal(key);
        if (temp == null)
        {
          Object obj = new Object();
          @SuppressWarnings("rawtypes")
          List wait = (List) waitMap.get(key);
          synchronized (wait)
          {
            wait.add(obj);
          }
          try
          {
            if (to == 0)
            {
              remaining = 0; // infinite
            }
            else
            {
              elapsed = System.currentTimeMillis() - start;
              remaining = to - elapsed;
              if (elapsed > to)
                break;
            }
            synchronized (obj)
            {
              obj.wait(remaining);
            }
          }
          catch (InterruptedException e)
          {
          }
        }
        else
        {
          elapsed = Long.MAX_VALUE;
        }
      }
      if (temp == null)
        throw new JDBCPoolException("Acquire Timeout Reached (" + timeout + " ms)");
      return temp;
    }

    // get connection from pool
    private JDBCConnectionImpl acquireInternal(String db_key) throws SQLException,
        JDBCPoolException
    {
      // get connection parameters matching database name
      JDBCSource p = (JDBCSource) sourceMap.get(db_key);

      // first call to database?
      if (p == null)
        throw new JDBCPoolException("Database Pool For '" + db_key + "' Does Not Exist!");

      // get connection pool matching database name
      @SuppressWarnings("rawtypes")
      List pool = (List) poolMap.get(db_key);
      synchronized (pool)
      {
        if (pool.size() > 0)
        {
          // retrieve & remove existing unused connection
          JDBCConnectionImpl impl = (JDBCConnectionImpl) pool.remove(pool.size() - 1);
          // change counters
          p.inuseConn++;
          p.freeConn--;
          // return connection
          return impl;
        }
      }

      // pool is empty so create new connection, if allowed
      if (p.inuseConn < p.maxConn)
      {
        p.inuseConn++;
        JDBCConnectionImpl impl = new JDBCConnectionImpl(db_key, p.url, p.user, p.password);
        synchronized (pool)
        {
          pool.add(impl);
        }
        return impl;
      }

      // nothing free at this time!
      return null;
    }

    @SuppressWarnings("rawtypes")
    public void addJDBCSource(JDBCSource p) throws SQLException
    {
      // save parameters
      sourceMap.put(p.name, p);

      // create wait list
      waitMap.put(p.name, Collections.synchronizedList(new LinkedList()));

      // create pool
      List pool = Collections.synchronizedList(new ArrayList(p.maxConn));
      poolMap.put(p.name, pool);

      // add connections to pool
      synchronized (pool)
      {
        for (int i = pool.size(); i < p.maxConn; i++)
        {
          pool.add(new JDBCConnectionImpl(p.name, p.url, p.user, p.password));
          p.freeConn++;
        }
      }

      // default
      if (defaultDbKey == null)
        defaultDbKey = p.name;
    }

    @SuppressWarnings("rawtypes")
    public void close() throws SQLException
    {
      for (Iterator iterator = sourceMap.keySet().iterator(); iterator.hasNext();)
      {
        JDBCSource p = (JDBCSource) sourceMap.get(iterator.next());
        close(p.name);
      }
    }

    public void close(String db_key) throws SQLException
    {
      // get connection pool matching database name
      String key = db_key;
      
      if (key == null)
        key = defaultDbKey;
      
      @SuppressWarnings("rawtypes")
      List pool = (List) poolMap.get(key);
      if (pool != null)
      {
        while (pool.size() > 0)
        { // NOTE: changed to while from if (mark)
          notifyJDBCMessage(key, key + " closing " + pool.size());
          JDBCConnectionImpl impl = null;
          // retrieve existing unused connection
          impl = (JDBCConnectionImpl) pool.remove(pool.size() - 1);
          // close connection
          impl.close();
        }
        JDBCSource p = (JDBCSource) sourceMap.get(key);
        p.freeConn = 0;
      }
    }

    @SuppressWarnings("rawtypes")
    public String debug()
    {
      String temp = "";
      int i = 0;
      // build string
      for (Iterator iterator = sourceMap.keySet().iterator(); iterator.hasNext();)
      {
        JDBCSource p = (JDBCSource) sourceMap.get(iterator.next());
        if (i > 0)
          temp += "\n";
        temp += "Database #" + (++i) + ": " + p;
      }
      // return string
      if (i == 0)
      {
        return null;
      }
      
      return temp;
    }

    // free resources when object is destroyed
    @Override
    protected void finalize()
    {
      try
      {
        close();
      }
      catch (Exception e)
      {
      }
    }

    public String getDefaultDatabase()
    {
      return defaultDbKey;
    }

    public JDBCSource getJDBCSource(String db_key)
    {
      return (JDBCSource) sourceMap.get(db_key);
    }

    public int getNumFree(String dbName)
    {
      String name = dbName;
      if (name == null)
        name = defaultDbKey; // use default
      JDBCSource p = (JDBCSource) sourceMap.get(name);
      
      return p.freeConn;
    }

    public int getNumTaken(String dbName)
    {
      String name = dbName;
      if (name == null)
        name = defaultDbKey; // use default
      JDBCSource p = (JDBCSource) sourceMap.get(name);
      return p.inuseConn;
    }

    public JDBCSource getParams(String db_key)
    {
      return (JDBCSource) sourceMap.get(db_key);
    }

    public boolean hasJDBCSource(String name)
    {
      return sourceMap.containsKey(name);
    }

    @SuppressWarnings("rawtypes")
    private synchronized JDBCSource initDatabase(String db_key, String lookup_db_key,
        Properties props) throws SQLException, JDBCPoolException
    {
      // process properties
      JDBCSource p = (JDBCSource) sourceMap.get(db_key);
      if (p != null)
      {
        return p;
      }
      
      p = new JDBCSource();
      notifyJDBCMessage(db_key, "Initializing properties for database '" + db_key + "'.");
      if (props == null)
        throw new JDBCPoolException("Database parameters not specified: " + lookup_db_key);
      p.name = db_key;
      p.driver = props.getProperty(lookup_db_key + ".driver");
      if (p.driver == null)
        throw new JDBCPoolException("Database parameters not found: " + lookup_db_key);
      p.url = props.getProperty(lookup_db_key + ".url");
      p.user = props.getProperty(lookup_db_key + ".user");
      p.password = props.getProperty(lookup_db_key + ".password");
      String temp = props.getProperty(lookup_db_key + ".maximum");
      try
      {
        p.maxConn = Integer.valueOf(temp).intValue();
        if (p.maxConn < 1)
          throw new Exception();
      }
      catch (Exception e)
      {
        throw new SQLException("Invalid maximum! (" + temp + ")");
      }
      p.inuseConn = 0;

      try
      {
        // load database driver
        Driver driver = (Driver) Class.forName(p.driver).newInstance();
        DriverManager.registerDriver(driver);
      }
      catch (InstantiationException e)
      {
        throw new SQLException("InstantiationException: " + p.driver + " (database driver)");
      }
      catch (IllegalAccessException e)
      {
        throw new SQLException("IllegalAccessException: " + p.driver + " (database driver)");
      }
      catch (ClassNotFoundException e)
      {
        throw new SQLException("ClassNotFoundException: " + p.driver + " (database driver)");
      }

      // save parameters
      sourceMap.put(db_key, p);

      // create wait list
      waitMap.put(db_key, Collections.synchronizedList(new LinkedList()));

      // create pool
      List pool = Collections.synchronizedList(new ArrayList(p.maxConn));
      poolMap.put(db_key, pool);

      // return
      return p;
    }

    // generates all necessary connections
    public void initializeImpl(String lookup_db_key, Properties props) throws SQLException,
        JDBCPoolException
    {
      if (lookup_db_key == null || props == null)
        throw new SQLException("Null Initialization Paramaters!");
      // true db key is the '.name' property
      String db_key = props.getProperty(lookup_db_key + ".name", null);
      if (db_key == null)
        db_key = lookup_db_key;

      // get connection parameters matching database name
      if (defaultDbKey == null)
        defaultDbKey = db_key; // set default
      JDBCSource p = (JDBCSource) sourceMap.get(db_key);

      // first call to database
      if (p == null)
        p = initDatabase(db_key, lookup_db_key, props);

      // get connection pool matching database name
      @SuppressWarnings("rawtypes")
      List pool = (List) poolMap.get(db_key);

      // add connections to pool
      synchronized (pool)
      {
        for (int i = pool.size(); i < p.maxConn; i++)
        {
          pool.add(new JDBCConnectionImpl(db_key, p.url, p.user, p.password));
          p.freeConn++;
        }
      }
    }

    protected static void notifyJDBCMessage(String key, String message)
    {
      if (listeners == null)
        return;
      for (int i = 0; i < listeners.size(); i++)
      {
        JDBCListener listener = (JDBCListener) listeners.get(i);
        listener.notifyJDBCMessage(key, message);
      }
    }

    protected static void notifyJDBCQuery(String key, String sql, long duration)
    {
      if (listeners == null)
        return;
      for (int i = 0; i < listeners.size(); i++)
      {
        JDBCListener listener = (JDBCListener) listeners.get(i);
        listener.notifyJDBCQuery(key, sql, duration);
      }
    }

    // return connection to pool
    public void releaseImpl(JDBCConnectionImpl impl)
    {
      String db_key = impl.getDatabaseName();
      // return connection to pool for that database
      @SuppressWarnings("rawtypes")
      List pool = (List) poolMap.get(db_key);
      synchronized (pool)
      {
        pool.add(impl);
      }
      // change counters
      JDBCSource p = (JDBCSource) sourceMap.get(db_key);
      synchronized (p)
      {
        p.inuseConn--;
        p.freeConn++;
      }
      // notify
      @SuppressWarnings("rawtypes")
      List wait = (List) waitMap.get(db_key);
      synchronized (wait)
      {
        if (wait.size() > 0)
        {
          Object obj = wait.remove(0);
          if (obj != null)
          {
            synchronized (obj)
            {
              obj.notify();
            }
          }
        }
      }
    }

    public static void removeAllListeners()
    {
      listeners = null;
    }

    public void removeJDBCSource(String name)
    {
      sourceMap.remove(name);
      waitMap.remove(name);
      poolMap.remove(name);
    }

    public static void removeListener(JDBCListener listener)
    {
      if (listener == null)
        return;
      if (listeners == null)
        return;
      if (listeners.contains(listener))
        listeners.remove(listener);
    }

    public void setDefaultDatabase(String defaultDbKey)
    {
      this.defaultDbKey = defaultDbKey;
    }
  } // static class JDBC Pool

  private transient Connection conn; // the precious connection

  private transient String dbName; // the database name

  // private constructor
  JDBCConnectionImpl(String dbName, String dbUrl, String dbUser, String dbPwd)
      throws SQLException
  {
    this.dbName = dbName;
    conn = DriverManager.getConnection(dbUrl, dbUser, dbPwd);
  }

  public void close() throws SQLException
  {
    if (conn == null)
      return;
    conn.close();
  }

  @Override
  protected void finalize() throws SQLException
  {
    close();
  }

  public Connection getConnection()
  {
    return conn;
  }

  public String getDatabaseName()
  {
    return dbName;
  }
}
