/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.data.shp.exception;

/**
 * An DuplicateEntryException is thrown when a duplicate value is being updated
 * or inserted into a unique index.
 */
public class DuplicateEntryException extends ActivityException
{

  /**
     * 
     */
  private static final long serialVersionUID = 1L;

  /**
   * Creates a new instance of <code>DuplicateEntryException</code> without
   * detail message.
   */
  public DuplicateEntryException()
  {
  }

  /**
   * Constructs an instance of <code>DuplicateEntryException</code> with the
   * specified detail message.
   * 
   * @param msg
   *          the detail message.
   */
  public DuplicateEntryException(String msg)
  {
    super(msg);
  }

  /**
   * Constructs an instance of <code>DuplicateEntryException</code> with the
   * specified detail message and throwable cause.
   * 
   * @param msg
   *          the detail message.
   * @param throwable
   *          the throwable cause.
   */
  public DuplicateEntryException(String msg, Throwable throwable)
  {
    super(msg, throwable);
  }

  /**
   * Constructs an instance of <code>DuplicateEntryException</code> with the
   * specified throwable cause.
   * 
   * @param throwable
   *          the throwable cause.
   */
  public DuplicateEntryException(Throwable throwable)
  {
    super(throwable);
  }
}