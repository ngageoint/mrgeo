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

package org.mrgeo.data.vector.pg;

public class PgDbSettings
{
  private String url;
  private String ssl;
  private String username;
  private String password;
  private String query;
  private String countQuery;
  private String mbrQuery;
  private String geomColumnLabel;
  private String wktColumnLabel;

  public PgDbSettings(final String url,
                      final String username,
                      final String password,
                      final String query,
                      final String countQuery,
                      final String mbrQuery,
                      final String geomColumnLabel,
                      final String wktColumnLabel,
                      final String ssl)
  {
    this.url = url;
    this.ssl = ssl;
    this.username = username;
    this.password = password;
    this.query = query;
    this.countQuery = countQuery;
    this.mbrQuery = mbrQuery;
    this.geomColumnLabel = geomColumnLabel;
    this.wktColumnLabel = wktColumnLabel;
  }

  public String getUrl() {return url; }

  public String getSsl() { return ssl; }

  public String getUsername() { return username; }

  public String getPassword() { return password; }

  public String getQuery() { return query; }

  public String getCountQuery() { return countQuery; }

  public String getMBRQuery() { return mbrQuery; }

  public String getGeomColumnLabel() { return geomColumnLabel; }

  public String getWktColumnLabel() { return wktColumnLabel; }
}
