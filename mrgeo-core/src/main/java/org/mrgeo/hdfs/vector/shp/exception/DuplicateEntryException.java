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

package org.mrgeo.hdfs.vector.shp.exception;

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