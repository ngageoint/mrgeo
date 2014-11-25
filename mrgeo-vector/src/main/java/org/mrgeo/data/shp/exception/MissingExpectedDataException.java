/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.data.shp.exception;

/**
 * An MissingExpectedDataException is thrown when expected data is not returned
 * during database activity.
 */
public class MissingExpectedDataException extends ActivityException
{

  /**
     * 
     */
  private static final long serialVersionUID = 1L;

  /**
   * Creates a new instance of <code>MissingExpectedDataException</code> without
   * detail message.
   */
  public MissingExpectedDataException()
  {
    super();
  }

  /**
   * Constructs an instance of <code>MissingExpectedDataException</code> with
   * the specified detail message.
   * 
   * @param s
   *          The message.
   */
  public MissingExpectedDataException(String msg)
  {
    super(msg);
  }

  /**
   * Constructs an instance of <code>MissingExpectedDataException</code> with
   * the specified detail message and throwable cause.
   * 
   * @param msg
   *          the detail message.
   * @param throwable
   *          the throwable cause.
   */
  public MissingExpectedDataException(String msg, Throwable throwable)
  {
    super(msg, throwable);
  }

  /**
   * Constructs an instance of <code>MissingExpectedDataException</code> with
   * the specified throwable cause.
   * 
   * @param throwable
   *          the throwable cause.
   */
  public MissingExpectedDataException(Throwable throwable)
  {
    super(throwable);
  }
}
