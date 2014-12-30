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

package org.mrgeo.data.shp.util.jdbc;

import org.mrgeo.data.shp.util.ObjectWrapper;

import java.sql.Blob;
import java.sql.ResultSet;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;


public class DatabaseTools extends java.lang.Object
{

  public static byte[] getBytes(Object obj) throws Exception
  {
    ObjectWrapper wrapper = ObjectWrapper.wrap(obj);
    return wrapper.getBytes();
  }

  // creates a Microsoft SQL Server/MySQL compliant datetime string (e.g.
  // '1998-05-02 01:23:56.123')
  // Note: MySQL rounds to the nearest second for 'DateTime' fields.
  // returns UTC
  public static String getDateTime()
  {
    return getDateTime(null, 0);
  }

  // creates a Microsoft SQL Server/MySQL compliant datetime string (e.g.
  // '1998-05-02 01:23:56.123')
  // Note: MySQL rounds to the nearest second for 'DateTime' fields.
  public static String getDateTime(Date date)
  {
    return getDateTime(date, 0);
  }

  // creates a Microsoft SQL Server/MySQL compliant datetime string (e.g.
  // '1998-05-02 01:23:56.123')
  // Note: MySQL rounds to the nearest second for 'DateTime' fields.
  public static String getDateTime(Date date, int offset)
  {
    Calendar cal = Calendar.getInstance();
    String temp = ((offset < 0) ? "" + offset : "+" + offset);
    cal.setTimeZone(TimeZone.getTimeZone("GMT" + temp));
    if (date != null)
      cal.setTime(date);
    String stamp = cal.get(Calendar.YEAR) + "-" + (cal.get(Calendar.MONTH) + 1) + "-"
        + cal.get(Calendar.DAY_OF_MONTH) + " ";
    stamp += cal.get(Calendar.HOUR_OF_DAY) + ":" + cal.get(Calendar.MINUTE) + ":"
        + cal.get(Calendar.SECOND) + "." + cal.get(Calendar.MILLISECOND);
    return stamp;
  }

  // creates a Microsoft SQL Server/MySQL compliant datetime string (e.g.
  // '1998-05-02 01:23:56.123')
  // Note: MySQL rounds to the nearest second for 'DateTime' fields.
  public static String getDateTime(Date date, String timezone)
  {
    Calendar cal = Calendar.getInstance();
    cal.setTimeZone(TimeZone.getTimeZone(timezone));
    if (date != null)
      cal.setTime(date);
    String stamp = cal.get(Calendar.YEAR) + "-" + (cal.get(Calendar.MONTH) + 1) + "-"
        + cal.get(Calendar.DAY_OF_MONTH) + " ";
    stamp += cal.get(Calendar.HOUR_OF_DAY) + ":" + cal.get(Calendar.MINUTE) + ":"
        + cal.get(Calendar.SECOND) + "." + cal.get(Calendar.MILLISECOND);
    return stamp;
  }

  // creates a Microsoft SQL Server/MySQL compliant datetime string (e.g.
  // '1998-05-02 01:23:56.123')
  // Note: MySQL rounds to the nearest second for 'DateTime' fields.
  public static String getDateTime(int offset)
  {
    return getDateTime(null, offset);
  }

  public static Object getObject(ResultSet rs, int columnIndex) throws Exception
  {
    byte[] bytes = rs.getBytes(columnIndex);
    if (bytes == null)
      return null;
    ObjectWrapper wrapper = ObjectWrapper.unwrap(bytes);
    return wrapper.getObject();
  }

  public static Object getObject(ResultSet rs, String columnName) throws Exception
  {
    byte[] bytes = rs.getBytes(columnName);
    if (bytes == null)
      return null;
    ObjectWrapper wrapper = ObjectWrapper.unwrap(bytes);
    return wrapper.getObject();
  }

  public static Object getObjectAsBlob(ResultSet rs, int columnIndex) throws Exception
  {
    Blob blob = rs.getBlob(columnIndex);
    if (blob == null)
      return null;
    ObjectWrapper wrapper = ObjectWrapper.unwrap(blob.getBytes(1, (int) blob.length()));
    return wrapper.getObject();
  }

  public static Object getObjectAsBlob(ResultSet rs, String columnName) throws Exception
  {
    Blob blob = rs.getBlob(columnName);
    if (blob == null)
      return null;
    ObjectWrapper wrapper = ObjectWrapper.unwrap(blob.getBytes(1, (int) blob.length()));
    return wrapper.getObject();
  }

  public static void main(String[] args)
  {
    Date d = new Date();
    String s = DatabaseTools.getDateTime(d, "EST");
    System.out.println(s);
  }
}
