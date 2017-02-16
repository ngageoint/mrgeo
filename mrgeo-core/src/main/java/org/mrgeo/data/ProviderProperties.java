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

package org.mrgeo.data;

import org.mrgeo.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

public class ProviderProperties implements Externalizable
{
private static Logger log = LoggerFactory.getLogger(ProviderProperties.class);
private String userName;
private List<String> roles;

public ProviderProperties()
{
  this("", new ArrayList<String>());
}

public ProviderProperties(String userName, List<String> roles)
{
  this.userName = userName;
  this.roles = roles;
}

public ProviderProperties(String userName, String commaDelimitedRoles)
{
  this.userName = userName;
  roles = new ArrayList<String>();
  String[] separated = commaDelimitedRoles.split(",");
  for (String r : separated)
  {
    roles.add(r);
  }
}

public static String toDelimitedString(ProviderProperties properties)
{
  if (properties == null)
  {
    return "";
  }

  return properties.toDelimitedString();

}

public static ProviderProperties fromDelimitedString(String value)
{
  String[] values = value.split("\\|\\|");
  String userName = "";
  List<String> roles = new ArrayList<String>();
  if (values.length > 0)
  {
    userName = values[0];
    for (int i = 1; i < values.length; i++)
    {
      roles.add(values[i]);
    }
  }
  return new ProviderProperties(userName, roles);
}

public String getUserName()
{
  return userName;
}

public List<String> getRoles()
{
  return roles;
}

public String toDelimitedString()
{
  return userName + "||" + StringUtils.join(roles, "||");
}

@Override
public void writeExternal(ObjectOutput out) throws IOException
{
  out.writeUTF(userName);
  out.writeInt(roles.size());
  for (String role : roles)
  {
    out.writeUTF(role);
  }
}

@Override
public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
{
  userName = in.readUTF();
  int roleCount = in.readInt();
  roles = new ArrayList<String>();
  for (int i = 0; i < roleCount; i++)
  {
    roles.add(in.readUTF());
  }
}
}
