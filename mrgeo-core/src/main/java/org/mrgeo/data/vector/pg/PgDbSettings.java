package org.mrgeo.data.vector.pg;

public class PgDbSettings
{
  private String url;
  private String ssl;
  private String username;
  private String password;
  private String query;
  private String countQuery;
  private String geomColumnLabel;

  public PgDbSettings(final String url,
                      final String username,
                      final String password,
                      final String query,
                      final String countQuery,
                      final String geomColumnLabel,
                      final String ssl)
  {
    this.url = url;
    this.ssl = ssl;
    this.username = username;
    this.password = password;
    this.query = query;
    this.countQuery = countQuery;
    this.geomColumnLabel = geomColumnLabel;
  }

  public String getUrl() {
    return url;
  }

  public String getSsl() {
    return ssl;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public String getQuery() {
    return query;
  }

  public String getCountQuery() {
    return countQuery;
  }

  public String getGeomColumnLabel() {
    return geomColumnLabel;
  }
}
