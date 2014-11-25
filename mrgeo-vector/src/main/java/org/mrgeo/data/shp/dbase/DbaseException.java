/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.data.shp.dbase;

public class DbaseException extends java.lang.Exception
{

  /**
     *
     */
  private static final long serialVersionUID = 1L;

  /**
   * Creates a new instance of <code>DbaseException</code> without detail
   * message.
   */
  public DbaseException()
  {
  }

  /**
   * Constructs an instance of <code>DbaseException</code> with the specified
   * detail message.
   * 
   * @param msg
   *          the detail message.
   */
  public DbaseException(String msg)
  {
    super(msg);
  }

  /**
   * Constructs an instance of <code>DbaseException</code> with the specified
   * detail message and throwable cause.
   * 
   * @param msg
   *          the detail message.
   * @param throwable
   *          the throwable cause.
   */
  public DbaseException(String msg, Throwable throwable)
  {
    super(msg, throwable);
  }

  /**
   * Constructs an instance of <code>DbaseException</code> with the specified
   * throwable cause.
   * 
   * @param throwable
   *          the throwable cause.
   */
  public DbaseException(Throwable throwable)
  {
    super(throwable);
  }
}
