package org.mrgeo.data;

import org.mrgeo.utils.StringUtils;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

public class ProviderProperties implements Externalizable
{
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

  public static ProviderProperties fromDelimitedString(String value)
  {
    String[] values = value.split("||");
    String userName = values[0];
    List<String> roles = new ArrayList<String>();
    for (int i=1; i < values.length; i++)
    {
      roles.add(values[i]);
    }
    return new ProviderProperties(userName, roles);
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
    for (int i=0; i < roleCount; i++)
    {
      roles.add(in.readUTF());
    }
  }
}
