package org.mrgeo.hdfs.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.data.image.ImageInputFormatContext;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConfigurationBuilder
{
// Key names from ImageInputFormatContext
private static final String imageInputFormatContextClassName = ImageInputFormatContext.class.getSimpleName();
private static final String ZOOM_LEVEL = imageInputFormatContextClassName + ".zoomLevel";
private static final String TILE_SIZE = imageInputFormatContextClassName + ".tileSize";
private static final String INPUT = imageInputFormatContextClassName + ".input";
private static final String BOUNDS = imageInputFormatContextClassName + ".bounds";
private static final String PROVIDER_PROPERTY_KEY = imageInputFormatContextClassName + "provProps";

// Key names from FileOutputFormat
private static final String FILE_OUTPT_FORMAT_COMPRESS = "mapreduce.output.fileoutputformat.compress";
private static final String FILE_OUTPUT_COMPRESSION_TYPE = "mapreduce.output.fileoutputformat.compress.type";
private static final String FILE_OUTPUT_COMPRESSION_CODEC = "mapreduce.output.fileoutputformat.compress.codec";
private static final String FILE_OUTPUT_PATH = "mapreduce.output.fileoutputformat.outputdir";

private final Configuration configuration;
private int zoomLevel;
private int tileSize;
private String boundsString;
private boolean compressOutput;
private String outputCompressionType;
private String outputCompressionCodec;
private String outputFilePath;

public ConfigurationBuilder()
{
  this.configuration = mock(Configuration.class);
}

public ConfigurationBuilder zoomLevel(int zoomLevel)
{
  this.zoomLevel = zoomLevel;

  return this;
}

public ConfigurationBuilder tileSize(int tileSize)
{
  this.tileSize = tileSize;

  return this;
}

public ConfigurationBuilder boundsString(String boundsString)
{
  this.boundsString = boundsString;

  return this;
}

public ConfigurationBuilder compressOutput(boolean compressOutput)
{
  this.compressOutput = compressOutput;

  return this;
}

public ConfigurationBuilder outputCompressionType(String outputCompressionType)
{
  this.outputCompressionType = outputCompressionType;

  return this;
}

public ConfigurationBuilder ouputCompressionCodec(String outputCompressionCodec)
{
  this.outputCompressionCodec = outputCompressionCodec;

  return this;
}

public ConfigurationBuilder outputFilePath(String outputFilePath)
{
  this.outputFilePath = outputFilePath;

  return this;
}

public Configuration build()
{
  when(configuration.getInt(ZOOM_LEVEL, 1)).thenReturn(zoomLevel);
  when(configuration.getInt(TILE_SIZE, MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT_INT)).thenReturn(tileSize);
  when(configuration.get(BOUNDS)).thenReturn(boundsString);
  when(configuration.getBoolean(FILE_OUTPT_FORMAT_COMPRESS, false)).thenReturn(compressOutput);
  when(configuration.get(FILE_OUTPUT_COMPRESSION_TYPE, CompressionType.RECORD.toString()))
      .thenReturn(outputCompressionType);
  when(configuration.get(FILE_OUTPUT_COMPRESSION_CODEC)).thenReturn(outputCompressionCodec);
  when(configuration.get(FILE_OUTPUT_PATH)).thenReturn(outputFilePath);

//        when(configuration.getClassByName(anyString())).thenAnswer(new Answer<Class>() {
//
//            @Override
//            public Class answer(InvocationOnMock invocationOnMock) throws Throwable {
//                return Class.forName(invocationOnMock.getArguments()[0].toString());
//            }
//        });

  return configuration;
}
}
