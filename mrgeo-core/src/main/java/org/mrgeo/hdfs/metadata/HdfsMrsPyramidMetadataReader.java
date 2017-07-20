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

package org.mrgeo.hdfs.metadata;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Predicates;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mrgeo.data.image.MrsPyramidMetadataReader;
import org.mrgeo.data.image.MrsPyramidMetadataReaderContext;
import org.mrgeo.hdfs.image.HdfsMrsImageDataProvider;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.image.MrsPyramidMetadata;
import org.reflections.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Set;

/**
 */
public class HdfsMrsPyramidMetadataReader implements MrsPyramidMetadataReader
{
private static final Logger log = LoggerFactory.getLogger(HdfsMrsPyramidMetadataReader.class);
private final HdfsMrsImageDataProvider dataProvider;
private MrsPyramidMetadata metadata;
private Configuration conf;
//private final MrsPyramidMetadataReaderContext context;

/**
 * Constructor
 *
 * @param dataProvider MrsImagePyramidDataProvider DataProvider (image pyramid) providing this metadata
 * @param context      MrsPyramidMetadataReaderContext Additional context data for loading the metadata (currently not used)
 */
public HdfsMrsPyramidMetadataReader(HdfsMrsImageDataProvider dataProvider,
    Configuration conf,
    MrsPyramidMetadataReaderContext context)
{
  this.dataProvider = dataProvider;
  this.conf = conf;
  //this.context = context;
}

/**
 * Return the MrsPyramidMetadate for the supplied HDFS resource
 */
@Override
public MrsPyramidMetadata read() throws IOException
{
  if (dataProvider == null)
  {
    throw new IOException("DataProvider not set!");
  }

  String name = dataProvider.getResourceName();
  if (name == null || name.length() == 0)
  {
    throw new IOException("Can not load metadata, resource name is empty!");
  }

  if (metadata == null)
  {
    metadata = loadMetadata();
  }

  return metadata;
}

/**
 * Reload the metadata.  Uses the existing object and sets all the parameters (getters/setters) to the
 * new values.  This allows anyone holding a reference to an existing metadata object to see the
 * updated values without having to reload the metadata themselves.
 */
@SuppressWarnings({"unchecked", "squid:S1166"}) // Exception caught and handled
@Override
public MrsPyramidMetadata reload() throws IOException
{

  if (metadata == null)
  {
    return read();
  }

  if (dataProvider == null)
  {
    throw new IOException("DataProvider not set!");
  }

  String name = dataProvider.getResourceName();
  if (name == null || name.length() == 0)
  {
    throw new IOException("Can not load metadata, resource name is empty!");
  }

  MrsPyramidMetadata copy = loadMetadata();

  Set<Method> getters = ReflectionUtils.getAllMethods(MrsPyramidMetadata.class,
      Predicates.<Method>and(
          Predicates.<AnnotatedElement>not(ReflectionUtils.withAnnotation(JsonIgnore.class)),
          ReflectionUtils.withModifier(Modifier.PUBLIC),
          ReflectionUtils.withPrefix("get"),
          ReflectionUtils.withParametersCount(0)));

  Set<Method> setters = ReflectionUtils.getAllMethods(MrsPyramidMetadata.class,
      Predicates.<Method>and(
          Predicates.<AnnotatedElement>not(ReflectionUtils.withAnnotation(JsonIgnore.class)),
          ReflectionUtils.withModifier(Modifier.PUBLIC),
          ReflectionUtils.withPrefix("set"),
          ReflectionUtils.withParametersCount(1)));


  //    System.out.println("getters");
  //    for (Method m: getters)
  //    {
  //      System.out.println("  " + m.getName());
  //    }
  //    System.out.println();
  //
  //    System.out.println("setters");
  //    for (Method m: setters)
  //    {
  //      System.out.println("  " + m.getName());
  //    }
  //    System.out.println();

  for (Method getter : getters)
  {
    String gettername = getter.getName();
    String settername = gettername.replaceFirst("get", "set");

    for (Method setter : setters)
    {
      if (setter.getName().equals(settername))
      {
        //          System.out.println("found setter: " + setter.getName() + " for " + getter.getName() );
        try
        {
          setter.invoke(metadata, getter.invoke(copy, new Object[]{}));
        }
        catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException ignore)
        {
        }
        break;
      }
    }
  }

  return metadata;
}

/**
 * Check for existence and load the metadata
 */
private MrsPyramidMetadata loadMetadata() throws IOException
{
  Path metapath = new Path(dataProvider.getResourcePath(true), HdfsMrsImageDataProvider.METADATA);
  FileSystem fs = HadoopFileUtils.getFileSystem(conf, metapath);

  // metadata file exists at this level
  if (fs.exists(metapath))
  {
    // load the file from HDFS
    log.debug("Physically loading image metadata from " + metapath.toString());
    try (InputStream is = HadoopFileUtils.open(conf, metapath))
    {
      // load the metadata from the input stream
      MrsPyramidMetadata meta = MrsPyramidMetadata.load(is);

      // set the fully qualified path for the metadata file
      //Path fullPath = metapath.makeQualified(fs);
      //meta.setPyramid(fullPath.getParent().toString());

      meta.setPyramid(dataProvider.getResourceName());

      return meta;
    }
  }

  throw new IOException("No metadata file found! (resource name: " + dataProvider.getResourceName() + ")");
}

}