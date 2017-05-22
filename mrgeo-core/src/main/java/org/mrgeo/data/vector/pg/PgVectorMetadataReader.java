package org.mrgeo.data.vector.pg;

import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.mrgeo.data.vector.VectorMetadata;
import org.mrgeo.data.vector.VectorMetadataReader;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.geometry.GeometryFactory;
import org.mrgeo.utils.tms.Bounds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;

public class PgVectorMetadataReader implements VectorMetadataReader
{
  private static Logger log = LoggerFactory.getLogger(PgVectorMetadataReader.class);
  private VectorMetadata metadata;
  private PgVectorDataProvider dataProvider;

  public PgVectorMetadataReader(PgVectorDataProvider provider)
  {
    this.dataProvider = provider;
  }

  @Override
  public VectorMetadata read() throws IOException
  {
    if (metadata == null)
    {
      try {
        metadata = loadMetadata();
      } catch (SQLException e) {
        throw new IOException(e);
      }
    }
    return metadata;
  }

  @Override
  public VectorMetadata reload() throws IOException
  {
    return null;
  }

  @SuppressFBWarnings(value = {"SQL_INJECTION_JDBC", "SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING"}, justification = "User supplied queries are a requirement")
  private VectorMetadata loadMetadata() throws SQLException, IOException
  {
    VectorMetadata metadata = new VectorMetadata();
    PgDbSettings dbSettings = dataProvider.parseResourceName();
    try(Connection conn = PgVectorDataProvider.getDbConnection(dbSettings)) {
      try (Statement st = conn.prepareStatement(dbSettings.getQuery(),
              ResultSet.TYPE_FORWARD_ONLY,
              ResultSet.CONCUR_READ_ONLY)) {
        try (ResultSet rs = ((PreparedStatement) st).executeQuery()) {
          ResultSetMetaData dbMetadata = rs.getMetaData();
          for (int c=1; c < dbMetadata.getColumnCount(); c++) {
            metadata.addAttribute(dbMetadata.getColumnLabel(c));
          }
        }
      }
      // Now get the minimum bounding rectangle
      String mbrQuery = dbSettings.getMBRQuery();
      if (mbrQuery == null || mbrQuery.isEmpty()) {
        // Look for the first occurrence of SELECT ... FROM and replace it with
        // SELECT ST_AsText(ST_Extent(geom)) FROM. Make sure to match case insensitively,
        // but the non-replaced portion of the string must retain its case (hence the
        // use of (?i) to inline the case insensitive match).
        mbrQuery = dbSettings.getQuery().replaceFirst("(?i)SELECT .* FROM",
                "SELECT ST_AsText(ST_Extent(" + dbSettings.getGeomColumnLabel() + ")) FROM");
      }
      try (Statement st = conn.prepareStatement(mbrQuery,
              ResultSet.TYPE_FORWARD_ONLY,
              ResultSet.CONCUR_READ_ONLY)) {
        try (ResultSet rs = ((PreparedStatement) st).executeQuery()) {
          if (rs.next()) {
            String mbrWkt = rs.getString(1);
            WKTReader wktReader = new WKTReader();
            Geometry geom = null;
            try {
              geom = GeometryFactory.fromJTS(wktReader.read(mbrWkt));
              if (geom != null) {
                Bounds mbr = geom.getBounds();
                metadata.setBounds(mbr);
              }
              else {
                log.warn("Unable to convert WKT returned from Postgres to MrGeo geometry: " + mbrWkt);
              }
            } catch (ParseException e) {
              log.warn("Unable to parse WKT returned form Postgres: " + mbrWkt);
            }
          }
        }
      }
    }
    return metadata;
  }
}
