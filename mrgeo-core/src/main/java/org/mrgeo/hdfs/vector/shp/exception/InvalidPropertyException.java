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
 * @param s The message.
 */
public InvalidPropertyException(String msg)
{
  super(msg);
}

/**
 * Constructs an instance of <code>ConfigurationException</code> with the
 * specified detail message and throwable cause.
 *
 * @param msg       the detail message.
 * @param throwable the throwable cause.
 */
public InvalidPropertyException(String msg, Throwable throwable)
{
  super(msg, throwable);
}

/**
 * Constructs an instance of <code>ConfigurationException</code> with the
 * specified throwable cause.
 *
 * @param throwable the throwable cause.
 */
public InvalidPropertyException(Throwable throwable)
{
  super(throwable);
}
}
