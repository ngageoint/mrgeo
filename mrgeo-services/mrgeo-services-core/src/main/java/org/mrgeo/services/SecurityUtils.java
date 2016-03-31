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

package org.mrgeo.services;

import org.mrgeo.data.ProviderProperties;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;

import java.util.ArrayList;
import java.util.List;

public class SecurityUtils
{
  public static ProviderProperties getProviderProperties()
  {
    ProviderProperties providerProperties = null;
    SecurityContext secCtx = SecurityContextHolder.getContext();
    if (secCtx != null)
    {
      Authentication a = secCtx.getAuthentication();
      if (a != null)
      {
        java.util.Collection<? extends GrantedAuthority> auths = a.getAuthorities();
        List<String> roles = new ArrayList<String>(auths.size());
        int i = 0;
        for (GrantedAuthority auth : auths)
        {
          roles.add(auth.getAuthority());
          i++;
        }
        providerProperties = new ProviderProperties(a.getName(), roles);
      }
    }
    return providerProperties;
  }
}
