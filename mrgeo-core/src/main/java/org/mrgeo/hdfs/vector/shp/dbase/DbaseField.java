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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.mrgeo.hdfs.vector.shp.util.StringUtils;

public class DbaseField implements java.io.Serializable
{
final static int BINARY = 66; // B, since D5
final static int CHARACTER = 67; // C, ever since
final static int DATE = 68; // D, ever since
final static int FLOAT = 70; // F, since D4
final static int GENERAL = 71; // G, since D5
final static int LOGICAL = 76; // L, since D3, D4*
final static int MEMO = 77; // M, ever since
final static int NUMERIC = 78; // N, since D3

private static final long serialVersionUID = 1L;

// static field types
private final static int UNKNOWN = 0; // unknown
public int decimal;
public int length;
// field data
public String name;
public int offset;
public int type;

public DbaseField()
{
}

public DbaseField(String name, int type)
{
  this.name = name.toUpperCase();
  this.type = type;
  switch (type)
  {
  case FLOAT:
    length = 13;
    decimal = 3;
    break;
  case CHARACTER:
    length = 50;
    decimal = 0;
    break;
  case NUMERIC:
    length = 13;
    decimal = 0;
    break;
  case LOGICAL:
    length = 1;
    decimal = 0;
    break;
  case DATE:
    length = 8;
    decimal = 0;
    break;
  default:
    length = 20;
    decimal = 0;
  }
}

public DbaseField(String name, int type, int length)
{
  this(name, type, length, 0);
  if (type == FLOAT)
  {
    decimal = 3;
  }
}

public DbaseField(String name, int type, int length, int decimal)
{
  this.name = name.toUpperCase();
  this.type = type;

  switch (type)
  {
  case LOGICAL:
    this.length = 1;
    this.decimal = 0;
    break;
  case DATE:
    this.length = 8;
    this.decimal = 0;
    break;
  default:
    this.length = length;
    this.decimal = decimal;
  }
}

public static int parseLiteral(String literal)
{
  if (literal == null)
  {
    return UNKNOWN;
  }
  String letter = literal.trim().toUpperCase().substring(0, 1);
  if (letter.equals("B"))
  {
    return BINARY;
  }
  else if (letter.equals("C"))
  {
    return CHARACTER;
  }
  else if (letter.equals("D"))
  {
    return DATE;
  }
  else if (letter.equals("F"))
  {
    return FLOAT;
  }
  else if (letter.equals("G"))
  {
    return GENERAL;
  }
  else if (letter.equals("L"))
  {
    return LOGICAL;
  }
  else if (letter.equals("M"))
  {
    return MEMO;
  }
  else if (letter.equals("N"))
  {
    return NUMERIC;
  }
  else
  {
    return UNKNOWN;
  }
}

private static String getTypeLiteral(int type)
{
  switch (type)
  {
  case BINARY:
    return "BINARY";
  case CHARACTER:
    return "CHARACTER";
  case DATE:
    return "DATE";
  case FLOAT:
    return "FLOAT";
  case GENERAL:
    return "GENERAL";
  case LOGICAL:
    return "LOGICAL";
  case MEMO:
    return "MEMO";
  case NUMERIC:
    return "NUMERIC";
  default:
    return "UNKNOWN";
  }
}

@Override
@SuppressFBWarnings(value = "UWF_UNWRITTEN_PUBLIC_OR_PROTECTED_FIELD", justification = "offset is a public field.  yuck!")
public String toString()
{
  return "o:" + StringUtils.pad("" + offset, 5) + "n:" + StringUtils.pad(name, 13) + "t:"
      + StringUtils.pad(getTypeLiteral(type), 12) + "l:" + StringUtils.pad("" + length, 4) + "d:"
      + StringUtils.pad("" + decimal, 4);
}
}
