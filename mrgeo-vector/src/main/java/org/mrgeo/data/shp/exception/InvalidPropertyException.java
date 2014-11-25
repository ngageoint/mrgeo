/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.data.shp.exception;

/**
 * An ActivityException is thrown when an error occurs during database activity.
 */
public class InvalidPropertyException extends Exception
{

  /**
     *
     */
  private static final long serialVersionUID = 1L;

  /**
   * Creates a new instance of <code>ConfigurationException</code> without
   * detail message.
   */
  public InvalidPropertyException()
  {
    super();
  }

  /**
   * Constructs an instance of <code>ConfigurationException</code> with the
   * specified detail message.
   * 
   * @param s
   *          The message.
   */
  public InvalidPropertyException(String msg)
  {
    super(msg);
  }

  /**
   * Constructs an instance of <code>ConfigurationException</code> with the
   * specified detail message and throwable cause.
   * 
   * @param msg
   *          the detail message.
   * @param throwable
   *          the throwable cause.
   */
  public InvalidPropertyException(String msg, Throwable throwable)
  {
    super(msg, throwable);
  }

  /**
   * Constructs an instance of <code>ConfigurationException</code> with the
   * specified throwable cause.
   * 
   * @param throwable
   *          the throwable cause.
   */
  public InvalidPropertyException(Throwable throwable)
  {
    super(throwable);
  }
}
