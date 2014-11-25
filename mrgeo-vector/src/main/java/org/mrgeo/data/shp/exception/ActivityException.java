/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.data.shp.exception;

/**
 * An ActivityException is thrown when an error occurs during database activity.
 */
public class ActivityException extends Exception
{

  /**
     *
     */
  private static final long serialVersionUID = 1L;

  /**
   * Creates a new instance of <code>ActivityException</code> without detail
   * message.
   */
  public ActivityException()
  {
    super();
  }

  /**
   * Constructs an instance of <code>ActivityException</code> with the specified
   * detail message.
   * 
   * @param s
   *          The message.
   */
  public ActivityException(String msg)
  {
    super(msg);
  }

  /**
   * Constructs an instance of <code>ActivityException</code> with the specified
   * detail message and throwable cause.
   * 
   * @param msg
   *          the detail message.
   * @param throwable
   *          the throwable cause.
   */
  public ActivityException(String msg, Throwable throwable)
  {
    super(msg, throwable);
  }

  /**
   * Constructs an instance of <code>ActivityException</code> with the specified
   * throwable cause.
   * 
   * @param throwable
   *          the throwable cause.
   */
  public ActivityException(Throwable throwable)
  {
    super(throwable);
  }
}
