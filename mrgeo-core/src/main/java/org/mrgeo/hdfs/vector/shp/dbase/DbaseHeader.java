/*
 * Copyright 2009-2017. DigitalGlobe, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.mrgeo.hdfs.vector.shp.dbase;

import org.mrgeo.hdfs.vector.shp.SeekableDataInput;
import org.mrgeo.hdfs.vector.shp.util.Convert;
import org.mrgeo.hdfs.vector.shp.util.StringUtils;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Calendar;
import java.util.Vector;

public class DbaseHeader implements Cloneable
{
private Vector<DbaseField> fields;
private int format;
private int headerLength;
private int langDriverID;
private int recordCount;
private int recordLength;

DbaseHeader()
{
  fields = new Vector<>();
  format = 3; // DBASE III is default for now
  langDriverID = 27; // default, not sure what it means
}

@Override
@SuppressWarnings("squid:S00112")
// I didn't write this code, so I'm not sure why it throws the RuntimeException.  Keeping it
public Object clone()
{
  DbaseHeader hdr;
  try
  {
    hdr = (DbaseHeader) super.clone();
  }
  catch (CloneNotSupportedException e)
  {
    throw new RuntimeException("Class doesn't implement cloneable", e);
  }
  return hdr;
}

public synchronized int getColumn(String name)
{
  int col = -1;
  for (int i = 0; i < fields.size(); i++)
  {
    DbaseField field = fields.get(i);
    if (field.name.equalsIgnoreCase(name))
    {
      col = i;
      break;
    }
  }
  return col;
}

public synchronized int getRecordCount()
{
  return recordCount;
}

public synchronized int getRecordLength()
{
  return recordLength;
}

public synchronized int getHeaderLength()
{
  return headerLength;
}

public synchronized DbaseField getField(int i) throws DbaseException
{
  try
  {
    return fields.get(i);
  }
  catch (Exception e)
  {
    throw new DbaseException("Field number invalid!", e);
  }
}

public synchronized int getFieldCount()
{
  return fields.size();
}

public synchronized Vector<DbaseField> getFields()
{
  return fields;
}

public synchronized void addField(DbaseField newField)
{
  fields.add(newField);
}

public synchronized void insertField(DbaseField field, int position)
{
  fields.insertElementAt(field, position);
}


//public synchronized Vector<DbaseField> getFieldsCopy()
//{
//  return (Vector<DbaseField>) fields.clone();
//}

public synchronized void save(RandomAccessFile os) throws IOException
{
  // core data
  byte[] header = new byte[32];
  header[0] = (byte) format;
  Calendar now = Calendar.getInstance();
  header[1] = (byte) (now.get(Calendar.YEAR) - 1900); // year
  header[2] = (byte) (now.get(Calendar.MONTH) + 1); // month
  header[3] = (byte) now.get(Calendar.DAY_OF_MONTH); // day
  header[29] = (byte) langDriverID; // langDriverID
  update();
  Convert.setLEShort(header, 10, (short) recordLength);
  // write core
  os.write(header, 0, 32);
  // field headers
  for (DbaseField field : fields)
  {
    header = new byte[32];
    // set field header
    Convert.setString(header, 0, 11, field.name);
    header[11] = (byte) field.type;
    header[16] = (byte) field.length;
    header[17] = (byte) field.decimal;
    // write
    os.write(header, 0, 32);
  }
  // write end of header character
  byte[] marker = new byte[1];
  marker[0] = 13; // 0Dh
  os.write(marker, 0, 1);
}

@Override
public synchronized String toString()
{
  String s = "\n";
  s = s + StringUtils.pad("format: ", 15) + format + "\n";
  s = s + StringUtils.pad("recordCount: ", 15) + recordCount + "\n";
  s = s + StringUtils.pad("headerLength: ", 15) + headerLength + "\n";
  s = s + StringUtils.pad("recordLength: ", 15) + recordLength + "\n";
  s = s + StringUtils.pad("fieldCount: ", 15) + fields.size() + "\n";
  s = s + StringUtils.pad("langDriverID:", 15) + langDriverID + "\n\n";
  for (int i = 0; i < fields.size(); i++)
  {
    DbaseField field = fields.get(i);
    s = s + "  " + StringUtils.pad(i + ")", 5) + field + "\n";
  }
  return s;
}

protected synchronized void load(SeekableDataInput is) throws IOException, DbaseException
{
  byte[] header = new byte[32];
  is.readFully(header, 0, 32);
  // core data
  format = Convert.getByte(header, 0);
  if (format != 3 && format != 4)
  {
    throw new DbaseException("DBase 3.x/4.x are only supported!");
  }
  recordCount = Convert.getLEInteger(header, 4);
  headerLength = Convert.getLEShort(header, 8);
  recordLength = Convert.getLEShort(header, 10);
  langDriverID = Convert.getByte(header, 29);
  // fields
  int fieldCount = (headerLength - 32 - 1) / 32;
  int offset = 1; // would be zero, but there is the deleted flag character
  for (int i = 0; i < fieldCount; i++)
  {
    header = new byte[32];
    is.readFully(header, 0, 32);
    DbaseField field = new DbaseField();
    field.name = Convert.getString(header, 0, 10);
    field.type = Convert.getByte(header, 11);
    switch (field.type)
    {
    case DbaseField.CHARACTER:
      break;
    case DbaseField.DATE:
      break;
    case DbaseField.FLOAT:
      break;
    case DbaseField.LOGICAL:
      break;
    case DbaseField.NUMERIC:
      break;
    default:
      throw new DbaseException("Unsupported DBF type: " + field.type);
    }
    field.length = Convert.getByte(header, 16);
    field.decimal = Convert.getByte(header, 17);
    field.offset = offset;
    offset = offset + field.length;
    fields.add(field);
  }
  // read end of header character (keeps fis position correct)
  byte[] marker = new byte[1];
  is.readFully(marker, 0, 1); // 0Dh
}

protected synchronized int update()
{
  recordLength = 1; // deleted/un-deleted flag is a byte, so we start with 1!
  for (DbaseField field : fields)
  {
    field.offset = recordLength;
    recordLength += field.length;
  }
  return recordLength;
}

}
