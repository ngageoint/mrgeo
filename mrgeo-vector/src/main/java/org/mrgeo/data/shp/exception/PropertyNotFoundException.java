/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.data.shp.exception;

/**
 * An ActivityException is thrown when an error occurs during database activity.
 */
public class PropertyNotFoundException extends Exception
{

  /**
     *
     */
  private static final long serialVersionUID = 1L;

  /**
   * Creates a new instance of <code>PropertyNotFoundException</code> without
   * detail message.
   */
  public PropertyNotFoundException()
  {
    super();
  }

  /**
   * Constructs an instance of <code>PropertyNotFoundException</code> with the
   * specified detail message.
   * 
   * @param s
   *          The message.
   */
  public PropertyNotFoundException(String msg)
  {
    super(msg);
  }

  /**
   * Constructs an instance of <code>PropertyNotFoundException</code> with the
   * specified detail message and throwable cause.
   * 
   * @param msg
   *          the detail message.
   * @param throwable
   *          the throwable cause.
   */
  public PropertyNotFoundException(String msg, Throwable throwable)
  {
    super(msg, throwable);
  }

  /**
   * Constructs an instance of <code>PropertyNotFoundException</code> with the
   * specified throwable cause.
   * 
   * @param throwable
   *          the throwable cause.
   */
  public PropertyNotFoundException(Throwable throwable)
  {
    super(throwable);
  }
}
