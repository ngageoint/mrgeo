package org.mrgeo.data.vector.pg;

import org.apache.hadoop.mapreduce.InputFormat;
import org.mrgeo.data.vector.FeatureIdWritable;
import org.mrgeo.data.vector.VectorInputFormatContext;
import org.mrgeo.data.vector.VectorInputFormatProvider;
import org.mrgeo.geometry.Geometry;

public class PgVectorInputFormatProvider extends VectorInputFormatProvider
{
//  private PgVectorDataProvider dataProvider;
  private PgDbSettings dbSettings;

  public PgVectorInputFormatProvider(VectorInputFormatContext context,
                                     PgVectorDataProvider dataProvider,
                                     PgDbSettings dbSettings)
  {
    super(context);
//    this.dataProvider = dataProvider;
    this.dbSettings = dbSettings;
  }

  @Override
  public InputFormat<FeatureIdWritable, Geometry> getInputFormat(String input)
  {
    return new PgVectorInputFormat(dbSettings);
  }
}
