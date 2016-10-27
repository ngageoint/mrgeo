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

package org.mrgeo.services.version;

//import org.apache.commons.lang3.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

//import org.springframework.security.core.Authentication;
//import org.springframework.security.core.GrantedAuthority;
//import org.springframework.security.core.context.SecurityContext;
//import org.springframework.security.core.context.SecurityContextHolder;

/**
 * @author Steve Ingram
 *         Date: 11/18/13
 */
public class VersionService {

    private final Properties props = new Properties();

    private static final Logger log = LoggerFactory.getLogger(VersionService.class);

    public VersionService() {
        try
        {
            props.load( getClass().getClassLoader().getResourceAsStream("version.properties") );
        }
        catch (Exception e)
        {
            log.error("Version error: " + e.getMessage(), e);
        }
    }

    public String getVersionJson() {
        StringBuilder sb = new StringBuilder();

        sb.append("{");
        sb.append(" \"name\": \"").append(props.getProperty("name")).append("\",");
        sb.append(" \"version\": \"").append(props.getProperty("version")).append("\",");
        sb.append(" \"revision\": \"").append(props.getProperty("revision")).append("\",");
        sb.append(" \"build\": \"").append(props.getProperty("build")).append("\",");
        sb.append(" \"buildTime\": \"").append(props.getProperty("buildTime")).append("\",");
        String packaging = props.getProperty("packaging");
        if ( packaging != null ) {
            packaging = packaging.replaceAll("\"", "");
        } else {
            packaging = "No packaging information";
        }
        sb.append(" \"packaging\": \"").append(packaging).append("\"");
        // The following is sample code showing how to obtain authorizations for
        // the logged in user (if authentication is enabled). It is strictly here
        // as an example for testing. It was never meant to actually be returned
        // with version information in a production system.
//        SecurityContext secCtx = SecurityContextHolder.getContext();
//        if (secCtx != null)
//        {
//          Authentication a = secCtx.getAuthentication();
//          if (a != null)
//          {
//            java.util.Collection<? extends GrantedAuthority> auths = a.getAuthorities();
//            String[] roles = new String[auths.size()];
//            int i = 0;
//            for (GrantedAuthority auth : auths)
//            {
//              roles[i] = auth.getAuthority();
//              i++;
//            }
//            sb.append(" \"roles\": \"").append(StringUtils.join(roles, ",")).append("\"");
//          }
//        }
        sb.append(" }");
        return sb.toString();
    }
}
