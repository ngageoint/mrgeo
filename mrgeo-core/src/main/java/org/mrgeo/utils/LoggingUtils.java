/*
 * Copyright 2009-2014 DigitalGlobe, Inc.
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

package org.mrgeo.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class LoggingUtils
{
  public static final String ALL = "all";
  public static final String FINE = "fine";
  public static final String TRACE = "trace";
  public static final String DEBUG = "debug";
  public static final String INFO = "info";
  public static final String WARN = "warn";
  public static final String ERROR = "error";
  public static final String OFF = "off";

  static final Logger log = LoggerFactory.getLogger(LoggingUtils.class);

  private static String defaultLevel = OFF;

  public static void setLogLevel(final String className, final String level)
  {
    String loggerClassName = log.getClass().getSimpleName();

    if (loggerClassName.equalsIgnoreCase("SimpleLogger") ||
        loggerClassName.equalsIgnoreCase("Slf4jLogger"))
    {
      // TODO:  If we update to slf4j 1.7.1+ the properties prefix is in a string constant
      System.setProperty("org.slf4j.simpleLogger.log." + className, level);
      log.info("Setting log level to: " + level);
    }
    else if (loggerClassName.equalsIgnoreCase("Log4JLoggerAdapter"))
    {
      try
      {
        Class<?> log4jLoggerFactory = Class.forName("org.apache.log4j.Logger");
        Class<?> levelClass = Class.forName("org.apache.log4j.Level");

        Method getLogger = log4jLoggerFactory.getMethod("getLogger", String.class);
        Object logger = getLogger.invoke(null, className);

        Method setLevel = logger.getClass().getMethod("setLevel",  levelClass);

        Object levelEnum = getLog4JLevel(level);

        setLevel.invoke(logger, levelEnum);

        log.debug("Changed log level for: {} to: {}", className, level);
      }
      catch (SecurityException e)
      {
        e.printStackTrace();
      }
      catch (NoSuchMethodException e)
      {
        e.printStackTrace();
      }
      catch (ClassNotFoundException e)
      {
        e.printStackTrace();
      }
      catch (IllegalArgumentException e)
      {
        e.printStackTrace();
      }
      catch (IllegalAccessException e)
      {
        e.printStackTrace();
      }
      catch (InvocationTargetException e)
      {
        e.printStackTrace();
      }
    }
    else
    {
      throw new UnsupportedOperationException("Only the Log4J & slf4j SimpleLogger is supported for logging, found: " + loggerClassName);
    }
  }

  public static void setLogLevel(final Class<?> clazz, final String level)
  {
    setLogLevel(clazz.getName(), level);
  }

  public static String getLogLevel(final Class<?> clazz)
  {
    return getLogLevel(clazz.getName());
  }

  public static String getLogLevel(final String className)
  {
    String loggerClassName = log.getClass().getSimpleName();

    String level = null;
    if (loggerClassName.equalsIgnoreCase("SimpleLogger") ||
        loggerClassName.equalsIgnoreCase("Slf4jLogger"))
    {
      level = System.getProperty("org.slf4j.simpleLogger.log." + className, null);

      if (level == null)
      {
        level = System.getProperty("org.slf4j.simpleLogger.defaultLogLevel", OFF);
      }
    }
    else if (loggerClassName.equalsIgnoreCase("Log4JLoggerAdapter"))
    {
      try
      {
        Class<?> log4jLoggerFactory = Class.forName("org.apache.log4j.Logger");

        Method getLogger = log4jLoggerFactory.getMethod("getLogger", String.class);
        Object logger = getLogger.invoke(null, className);

        Method getLevel = logger.getClass().getMethod("getLevel",  (Class<?>[])null);

        Object levelEnum = getLevel.invoke(logger);
        if (levelEnum != null)
        {
          level = getSlf4jLevel(levelEnum);
        }

        if (level == null)
        {
          level = defaultLevel;
        }

      }
      catch (SecurityException e)
      {
        e.printStackTrace();
      }
      catch (NoSuchMethodException e)
      {
        e.printStackTrace();
      }
      catch (ClassNotFoundException e)
      {
        e.printStackTrace();
      }
      catch (IllegalArgumentException e)
      {
        e.printStackTrace();
      }
      catch (IllegalAccessException e)
      {
        e.printStackTrace();
      }
      catch (InvocationTargetException e)
      {
        e.printStackTrace();
      }
    }
    else
    {
      throw new UnsupportedOperationException("Only the Log4J & slf4j SimpleLogger is supported for logging, found: " + loggerClassName);
    }

    return level == null ? OFF : level;
  }


  public static void setDefaultLogLevel(final String level)
  {
    defaultLevel = level;

    String loggerClassName = log.getClass().getSimpleName();

    if (loggerClassName.equalsIgnoreCase("SimpleLogger"))
    {
      // TODO:  If we update to slf4j 1.7.1+ the properties are in a string constant
      System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", level);
      log.info("Setting default log level to: " + level);
    }
    else if (loggerClassName.equalsIgnoreCase("Log4JLoggerAdapter"))
    {
      try
      {
        Class<?> log4jLoggerFactory = Class.forName("org.apache.log4j.Logger");
        Class<?> levelClass = Class.forName("org.apache.log4j.Level");

        Method getLogger = log4jLoggerFactory.getMethod("getRootLogger", (Class<?>[])null);
        Object logger = getLogger.invoke(null);
        Method setLevel = logger.getClass().getMethod("setLevel",  levelClass);

        Object levelEnum = getLog4JLevel(level);

        setLevel.invoke(logger, levelEnum);

        log.debug("Changed default log level to: {}", level);

      }
      catch (ClassNotFoundException e)
      {
        e.printStackTrace();
      }
      catch (SecurityException e)
      {
        e.printStackTrace();
      }
      catch (NoSuchMethodException e)
      {
        e.printStackTrace();
      }
      catch (IllegalArgumentException e)
      {
        e.printStackTrace();
      }
      catch (IllegalAccessException e)
      {
        e.printStackTrace();
      }
      catch (InvocationTargetException e)
      {
        e.printStackTrace();
      }
    }
    else
    {
      throw new UnsupportedOperationException("Only the Log4J & slf4j SimpleLogger is supported for logging.  Additional loggers can easily be added");
    }
  }

  public static String getDefaultLogLevel()
  {
    return defaultLevel;
  }

  public static void setLoggerToStdOut()
  {
    setLoggerToFile("System.out");
  }
  public static void setLoggerToStdErr()
  {
    setLoggerToFile("System.err");
  }

  public static void redirect()
  {
    System.setOut(new PrintStream(System.out){
      @Override
      public void print(String s){ log.info(s);}
    });

    System.setErr(new PrintStream(System.err){
      @Override
      public void print(String s){ log.error(s);}
    });

  }

  public static void setLoggerToFile(final String file)
  {
    String loggerClass = log.getClass().getSimpleName();

    if (loggerClass.equalsIgnoreCase("SimpleLogger"))
    {
      // TODO:  If we update to slf4j 1.7.1+ the properties are in a string constant
      System.setProperty("org.slf4j.simpleLogger.logFile", file);
      log.info("Setting logging output to: " + file);
    }
    //    else if (loggerClass.equalsIgnoreCase("Log4JLoggerAdapter"))
    //    {
    //      
    //    }
    else
    {
      throw new UnsupportedOperationException("Only the slf4j SimpleLogger is supported for logging.  Additional loggers can easily be added");
    }
  }

  public static void setLoggerFormat()
  {
    String loggerClass = log.getClass().getSimpleName();
    System.out.println(log.getClass().getName());
    if (loggerClass.equalsIgnoreCase("SimpleLogger"))
    {
      // TODO:  If we update to slf4j 1.7.1+ the properties are in a string constant
      System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
      System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "HH:mm:ss.SSS");
      System.setProperty("org.slf4j.simpleLogger.showThreadName", "true");
      System.setProperty("org.slf4j.simpleLogger.showLogName", "true");
      System.setProperty("org.slf4j.simpleLogger.showShortLogName", "false");
      System.setProperty("org.slf4j.simpleLogger.levelInBrackets", "true");
      log.info("Setting up default output format");

    }
    //    else if (loggerClass.equalsIgnoreCase("Log4JLoggerAdapter"))
    //    {
    //      
    //    }
    else
    {
      throw new UnsupportedOperationException("Only the slf4j SimpleLogger is supported for logging.  Additional loggers can easily be added");
    }
  }
  
  public static boolean finer(String level, String compareTo)
  {
//    public static final String ALL = "all";
//    public static final String FINE = "fine";
//    public static final String TRACE = "trace";
//    public static final String DEBUG = "debug";
//    public static final String INFO = "info";
//    public static final String WARN = "warn";
//    public static final String ERROR = "error";
//    public static final String OFF = "off";

    if (level == ALL)
    {
      return false;
    }
    else if (level ==  FINE)
    {
      return !(compareTo == ALL);
    }
    else if (level == TRACE)
    {
      return !(compareTo == ALL || compareTo == FINE);
    }
    else if (level == DEBUG)
    {
      return !(compareTo == ALL || compareTo == FINE || compareTo == TRACE);
    }
    else if (level == INFO)
    {
      return !(compareTo == ALL || compareTo == FINE || compareTo == TRACE || compareTo == DEBUG);
    }
    else if (level == WARN)
    {
      return (compareTo != OFF && compareTo != ERROR && compareTo != WARN);
    }
    else if (level == ERROR)
    {
      return (compareTo != OFF && compareTo != ERROR);
    }
    else if (level == OFF)
    {
      return (compareTo != OFF);
    }

    return false;
  }

  private static String getSlf4jLevel(final Object levelEnum)
  {
    try
    {
      Class<?> levelClass = Class.forName("org.apache.log4j.Level");
      Method toLevel = levelClass.getMethod("toString",  ( Class<?>[])null);
      String level = (String) toLevel.invoke(null, levelEnum);

      return level;

    }
    catch (ClassNotFoundException e)
    {
      e.printStackTrace();
    }
    catch (SecurityException e)
    {
      e.printStackTrace();
    }
    catch (NoSuchMethodException e)
    {
      e.printStackTrace();
    }
    catch (IllegalArgumentException e)
    {
      e.printStackTrace();
    }
    catch (IllegalAccessException e)
    {
      e.printStackTrace();
    }
    catch (InvocationTargetException e)
    {
      e.printStackTrace();
    }

    return null;
  }

  private static Object getLog4JLevel(final String level)
  {
    try
    {
      Class<?> levelClass = Class.forName("org.apache.log4j.Level");
      Method toLevel = levelClass.getMethod("toLevel",  String.class);
      Object levelEnum = toLevel.invoke(null, level);

      return levelEnum;

    }
    catch (ClassNotFoundException e)
    {
      e.printStackTrace();
    }
    catch (SecurityException e)
    {
      e.printStackTrace();
    }
    catch (NoSuchMethodException e)
    {
      e.printStackTrace();
    }
    catch (IllegalArgumentException e)
    {
      e.printStackTrace();
    }
    catch (IllegalAccessException e)
    {
      e.printStackTrace();
    }
    catch (InvocationTargetException e)
    {
      e.printStackTrace();
    }

    return null;
  }
}
