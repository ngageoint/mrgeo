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

package org.mrgeo.services;

import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.mrgeo.data.DataProviderFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;

public class SecurityUtils
{
  public static Properties getProviderProperties()
  {
    Properties providerProperties = null;
    SecurityContext secCtx = SecurityContextHolder.getContext();
    if (secCtx != null)
    {
      Authentication a = secCtx.getAuthentication();
      if (a != null)
      {
        providerProperties = new Properties();
        java.util.Collection<? extends GrantedAuthority> auths = a.getAuthorities();
        String[] roles = new String[auths.size()];
        int i = 0;
        for (GrantedAuthority auth : auths)
        {
          roles[i] = auth.getAuthority();
          i++;
        }
        DataProviderFactory.setProviderProperty(
            DataProviderFactory.PROVIDER_PROPERTY_USER_NAME,
            a.getName(),
            providerProperties);
        DataProviderFactory.setProviderProperty(
            DataProviderFactory.PROVIDER_PROPERTY_USER_ROLES,
            StringUtils.join(roles, ","),
            providerProperties);
      }
    }
    return providerProperties;
  }
}
