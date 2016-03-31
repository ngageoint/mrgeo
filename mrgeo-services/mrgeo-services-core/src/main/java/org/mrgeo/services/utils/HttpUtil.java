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

package org.mrgeo.services.utils;

import javax.servlet.http.HttpServletRequest;

/**
 * A class for working with HTTP requests that might have
 * been forwarded through a proxy.
 */
public class HttpUtil {

    /**
     * Gets the scheme (http or https) of a request that might have been
     * forwarded through a proxy.  It looks for the 'X-Forwarded-SSL' header,
     * and if that header has a value of 'on', it considers the scheme to be
     * 'https'.  Otherwise, it is standard 'http'.
     *
     * @param request the request to examine
     * @return a string representing the request scheme (http or https)
     */
    public static String getSchemeFromHeaders( HttpServletRequest request ) {

        String scheme = request.getScheme();
        String headerValue = request.getHeader( "X-Forwarded-SSL" );
        if ( "on".equals( headerValue ) ) {
            scheme = "https";
        }
        return scheme;
    }

    /**
     * Updates the scheme portion of a supplied URL string based on any headers
     * that might have been provided in the supplied request object.
     *
     * @param url the URL to modify
     * @param request the request to examine
     * @return a potentially modified URL string
     */
    public static String updateSchemeFromHeaders( String url, HttpServletRequest request ) {
        String scheme = getSchemeFromHeaders( request );

        return url.replaceFirst( "http:", scheme + ":" );
    }

    /**
     * Reconstructs the base context URL for the request, using
     * {@link #getSchemeFromHeaders( HttpServletRequest )} to determine
     * the scheme.
     *
     * @param request the request to examine
     * @return the reconstructed base URL for the request's context
     */
    public static String getBaseContextUrl( HttpServletRequest request ) {
        String scheme = getSchemeFromHeaders( request );

        int standardPort = ( scheme.equals( "https" ) ) ? 443 : 80;
        int port = request.getServerPort();

        // coming over a proxy we might lose the ssl port, too,
        // so fake like it came over the standard ssl port
        if ( scheme.equals( "https" ) && ( port == 80 ) ) {
            port = 443;
        }

        return  scheme + "://" +
                request.getServerName() +
                (( port != standardPort ) ? ":" + port : "" ) +
                request.getContextPath();
    }
}
