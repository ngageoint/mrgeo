package org.mrgeo.cmd.vectorinfo;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.mrgeo.cmd.Command;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderNotFound;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.vector.VectorDataProvider;
import org.mrgeo.data.vector.VectorMetadata;
import org.mrgeo.data.vector.VectorMetadataReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class VectorInfo extends Command
{
  private static Logger log = LoggerFactory.getLogger(VectorInfo.class);

  private boolean verbose = false;
  private boolean debug = false;
  private Configuration config;

  @Override
  public String getUsage() { return "vectorinfo <vector data source>"; }

  @Override
  public void addOptions(Options options)
  {
    // No additional options
  }

  @Override
  @SuppressWarnings("squid:S1166") // DataProviderNotFound exception caught and message printed
  public int run(final CommandLine line, final Configuration conf,
                 final ProviderProperties providerProperties) throws ParseException
  {
    this.config = conf;
    debug = line.hasOption("d");
    verbose = debug || line.hasOption("v");

    String input = null;
    for (final String arg : line.getArgs())
    {
      input = arg;
      break;
    }

    if (input == null)
    {
      throw new ParseException("Missing input vector data source");
    }

    try {
      VectorDataProvider dp = DataProviderFactory.getVectorDataProvider(
              input,
              DataProviderFactory.AccessMode.READ,
              providerProperties);
      VectorMetadataReader reader = dp.getMetadataReader();
      if (reader != null) {
        VectorMetadata metadata = reader.read();
        System.out.println("Bounds: " + metadata.getBounds().toString());
        System.out.println("Attributes:");
        for (String attr: metadata.getAttributes()) {
          System.out.println("  " + attr);
        }
        return 0;
      }
    } catch (DataProviderNotFound e) {
      log.error("Unable to open " + input, e);
    } catch (IOException e) {
      log.error("Unable to read metadata for " + input, e);
    }
    return -1;
  }
}
