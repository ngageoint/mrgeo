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

package org.mrgeo.utils.logging;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.log4j.Appender;
import org.apache.log4j.spi.RootLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;

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

static
{
  initializeForReal();
}

public static void setLogLevel(final String className, final String level)
{
  String loggerClassName = log.getClass().getSimpleName();

  if (loggerClassName.equalsIgnoreCase("SimpleLogger") ||
      loggerClassName.equalsIgnoreCase("Slf4jLogger"))
  {
    // TODO:  If we update to slf4j 1.7.1+ the properties prefix is in a string constant
    System.setProperty("org.slf4j.simpleLogger.log." + className, level);
    System.err.println("Setting log level to: " + level);
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

      System.err.println("Changed log level for: " + className + " to: " + level);
    }
    catch (SecurityException | NoSuchMethodException | ClassNotFoundException |
        IllegalAccessException | IllegalArgumentException | InvocationTargetException e)
    {
      log.error("Exception thrown {}", e);
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
    catch (ClassNotFoundException | SecurityException | IllegalArgumentException |
        NoSuchMethodException | IllegalAccessException | InvocationTargetException e)
    {
      log.error("Exception thrown {}", e);
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
    System.err.println("LoggingUtils: Setting default log level to: " + level.toUpperCase());
  }
  else if (loggerClassName.equalsIgnoreCase("Log4JLoggerAdapter"))
  {
    try
    {
      Object logger = getRootLogger();
      Class<?> levelClass = Class.forName("org.apache.log4j.Level");

      Method setLevel = logger.getClass().getMethod("setLevel",  levelClass);

      Object levelEnum = getLog4JLevel(level);

      setLevel.invoke(logger, levelEnum);

      setDefaultLoggerFormat();
      System.err.println("LoggingUtils: Setting default (root) log level to: " + level.toUpperCase());
    }
    catch (ClassNotFoundException | SecurityException |
        IllegalArgumentException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e)
    {
      log.error("Exception thrown {}", e);
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
    @SuppressFBWarnings(value = "CRLF_INJECTION_LOGS", justification = "CRLF Neutralized")
    public void print(String s){ log.info(s.replaceAll("(\\r|\\n)", ""));}
  });

  System.setErr(new PrintStream(System.err){
    @Override
    @SuppressFBWarnings(value = "CRLF_INJECTION_LOGS", justification = "CRLF Neutralized")
    public void print(String s){ log.error(s.replaceAll("(\\r|\\n)", ""));}
  });

}

public static void setLoggerToFile(final String file)
{
  String loggerClass = log.getClass().getSimpleName();

  if (loggerClass.equalsIgnoreCase("SimpleLogger"))
  {
    // TODO:  If we update to slf4j 1.7.1+ the properties are in a string constant
    System.setProperty("org.slf4j.simpleLogger.logFile", file);
    System.err.println("Setting logging output to: " + file);
  }
  else if (loggerClass.equalsIgnoreCase("Log4JLoggerAdapter"))
  {
    try
    {
      Object rootlogger = getRootLogger();
      Method getAppenders = rootlogger.getClass().getMethod("getAllAppenders");

      Enumeration e = (Enumeration) getAppenders.invoke(rootlogger);
      Object fileappender = null;
      Object layout = null;
      while (e.hasMoreElements())
      {
        Object appender = e.nextElement();

        Method getLayout = appender.getClass().getMethod("getLayout");
        layout = getLayout.invoke(appender);

        if (appender.getClass().getSimpleName().equalsIgnoreCase("FileAppender")) {
          fileappender = appender;
          break;
        }
      }

      if (fileappender == null)
      {
        Class<?> fileappenderclass = Class.forName("org.apache.log4j.FileAppender");
        Constructor<?> constructor = fileappenderclass.getConstructor();
        fileappender = constructor.newInstance();

        Class<?> appenderclass = Class.forName("org.apache.log4j.Appender");

        Method addappender = rootlogger.getClass().getMethod("addAppender", appenderclass);
        addappender.invoke(rootlogger, fileappender);
      }

      // setFile(String file)
      Method setfile = fileappender.getClass().getMethod("setFile", String.class);
      setfile.invoke(fileappender, file);

      // setAppend(boolean append)
      Method append = fileappender.getClass().getMethod("setAppend", boolean.class);
      append.invoke(fileappender, true);


      if (layout != null)
      {
        // setLayout(Layout layout)
        Class<?> layoutclass = Class.forName("org.apache.log4j.Layout");
        Method setLayout = fileappender.getClass().getMethod("setLayout", layoutclass);
        setLayout.invoke(fileappender, layout);
      }
      else
      {
        setDefaultLoggerFormat();
      }

      // activateOptions()
      Method activate = fileappender.getClass().getMethod("activateOptions");
      activate.invoke(fileappender);

      System.err.println("Setting logging output to: " + file);

    }
    catch (ClassNotFoundException | SecurityException | IllegalArgumentException |
        NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e)
    {
      log.error("Exception thrown {}", e);
    }

  }
  else
  {
    throw new UnsupportedOperationException("Only the slf4j SimpleLogger is supported for logging.  Additional loggers can easily be added");
  }
}

public static void setDefaultLoggerFormat()
{
  setLoggerFormat("%d{HH:mm:ss.SSS} (%r) %p %c{3}: %m%n");
}

public static void setLoggerFormat(String pattern)
{
  String loggerClass = log.getClass().getSimpleName();
  if (loggerClass.equalsIgnoreCase("SimpleLogger"))
  {
    // TODO:  If we update to slf4j 1.7.1+ the properties are in a string constant
    System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
    System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "HH:mm:ss.SSS");
    System.setProperty("org.slf4j.simpleLogger.showThreadName", "true");
    System.setProperty("org.slf4j.simpleLogger.showLogName", "true");
    System.setProperty("org.slf4j.simpleLogger.showShortLogName", "false");
    System.setProperty("org.slf4j.simpleLogger.levelInBrackets", "true");
    System.err.println("Setting up Simplelogger default output format");
  }
  else if (loggerClass.equalsIgnoreCase("Log4JLoggerAdapter"))
  {
    try
    {
      Class<?> log4jLoggerFactory = Class.forName("org.apache.log4j.Logger");
      Method getLogger = log4jLoggerFactory.getMethod("getRootLogger", (Class<?>[])null);
      Object logger = getLogger.invoke(null);

      Method allAppendersCall = logger.getClass().getMethod("getAllAppenders");

      Enumeration e = (Enumeration) allAppendersCall.invoke(logger);

      Class<?> layoutClass = Class.forName("org.apache.log4j.Layout");
      Class<?> layoutFactory = Class.forName("org.apache.log4j.PatternLayout");
      Constructor constructor = layoutFactory.getConstructor(String.class);

      Object layout = constructor.newInstance(pattern);

      Class<?> appenderFactory = Class.forName("org.apache.log4j.Appender");
      Method setLayout = appenderFactory.getMethod("setLayout", layoutClass);

      while (e.hasMoreElements())
      {
        Object appender = e.nextElement();
        setLayout.invoke(appender, layout);
      }

      System.err.println("Setting up Log4JLoggerAdapter default output format");

    }
    catch (ClassNotFoundException | SecurityException | IllegalArgumentException |
        NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e)
    {
      log.error("Exception thrown {}", e);
    }
  }
  else
  {
    throw new UnsupportedOperationException("Only the log4j slf4j SimpleLogger is supported for logging.  Additional loggers can easily be added");
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

  switch (level)
  {
  case ALL:
    return false;
  case FINE:
    return !(compareTo.equals(ALL));
  case TRACE:
    return !(compareTo.equals(ALL) || compareTo.equals(FINE));
  case DEBUG:
    return !(compareTo.equals(ALL) || compareTo.equals(FINE) || compareTo.equals(TRACE));
  case INFO:
    return !(compareTo.equals(ALL) || compareTo.equals(FINE) ||
        compareTo.equals(TRACE) || compareTo.equals(DEBUG));
  case WARN:
    return (!compareTo.equals(OFF) && !compareTo.equals(ERROR) && !compareTo.equals(WARN));
  case ERROR:
    return (!compareTo.equals(OFF) && !compareTo.equals(ERROR));
  case OFF:
    return (!compareTo.equals(OFF));
  }

  return false;
}

private static String getSlf4jLevel(final Object levelEnum)
{
  try
  {
    Class<?> levelClass = Class.forName("org.apache.log4j.Level");
    Method toLevel = levelClass.getMethod("toString",  ( Class<?>[])null);

    return (String) toLevel.invoke(null, levelEnum);

  }
  catch (ClassNotFoundException | SecurityException | IllegalArgumentException |
      NoSuchMethodException | IllegalAccessException | InvocationTargetException e)
  {
    log.error("Exception thrown {}", e);
  }

  return null;
}

private static Object getLog4JLevel(final String level)
{
  try
  {
    Class<?> levelClass = Class.forName("org.apache.log4j.Level");
    Method toLevel = levelClass.getMethod("toLevel",  String.class);

    return toLevel.invoke(null, level);

  }
  catch (ClassNotFoundException | SecurityException | IllegalArgumentException |
      NoSuchMethodException | IllegalAccessException | InvocationTargetException e)
  {
    log.error("Exception thrown {}", e);
  }

  return null;
}

private static void initializeForReal()
{
  String loggerClassName = log.getClass().getSimpleName();

  if (loggerClassName.equalsIgnoreCase("SimpleLogger"))
  {
    // no op
  }
  else if (loggerClassName.equalsIgnoreCase("Log4JLoggerAdapter"))
  {
    try
    {
      // See if the root logger has any appenders. If not, initialize with the basic configurator
      Object rootlogger = getRootLogger();
      Method getAppenders = rootlogger.getClass().getMethod("getAllAppenders");

      Enumeration appenders = (Enumeration)getAppenders.invoke(rootlogger);
      if (!appenders.hasMoreElements())
      {
        Class<?> basicConfiguratorClass = Class.forName("org.apache.log4j.BasicConfigurator");
        //Class<?> levelClass = Class.forName("org.apache.log4j.Level");
        Method configure = basicConfiguratorClass.getMethod("configure");
        configure.invoke(null);

        System.err.println("LoggingUtils: Initializing log4j logger to use a basic configuration:");
      }

      appenders = (Enumeration)getAppenders.invoke(rootlogger);
      wrapAppenders(rootlogger, appenders);
    }
    catch (IllegalAccessException | InvocationTargetException | ClassNotFoundException | NoSuchMethodException e)
    {
      log.error("Exception thrown {}", e);
    }
  }
  else
  {
    throw new UnsupportedOperationException(
        "Only the Log4J & slf4j SimpleLogger is supported for logging.  Additional loggers can easily be added");
  }
}

private static void wrapAppenders(Object rootlogger, Enumeration appenders)
{
  RootLogger rl;
  if (rootlogger instanceof RootLogger)
  {
    rl = (RootLogger)rootlogger;

    while (appenders.hasMoreElements())
    {
      Object next = appenders.nextElement();
      if (next instanceof Appender)
      {
        Appender app = (Appender)next;

        Appender wrapped = new Log4JAppenderWrapper(app);
        rl.removeAppender(app);
        rl.addAppender(wrapped);
      }
    }

  }
}

private static Object getRootLogger() throws ClassNotFoundException
{
  try
  {
    Class<?> log4jLoggerFactory = Class.forName("org.apache.log4j.Logger");

    Method getLogger = log4jLoggerFactory.getMethod("getRootLogger", (Class<?>[])null);

    return getLogger.invoke(null);
  }
  catch (ClassNotFoundException | InvocationTargetException | IllegalAccessException |
      IllegalArgumentException | NoSuchMethodException | SecurityException e)
  {
    log.error("Exception thrown {}", e);
  }

  throw new ClassNotFoundException("Can't find root logger!");

}

  public static void initialize() {
    // No need to do anything - just need to invoke static initializer when class is loaded
  }
}
