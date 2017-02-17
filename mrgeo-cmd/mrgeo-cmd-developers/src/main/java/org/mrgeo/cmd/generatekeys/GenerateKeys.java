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

package org.mrgeo.cmd.generatekeys;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.mrgeo.cmd.Command;
import org.mrgeo.cmd.OptionsParser;
import org.mrgeo.data.ProviderProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

public class GenerateKeys extends Command
{
private static final Logger log = LoggerFactory.getLogger(GenerateKeys.class);

//  public static void generateKeyFile(String file, String table, Connector connector) throws TableNotFoundException, IOException {
//    Scanner scan;
//    PrintWriter out = new PrintWriter(new FileWriter(file));
//
//    scan = connector.createScanner(table, new Authorizations());
//    for(Entry<Key, Value> entry : scan) {
//      long value = ByteBuffer.wrap(entry.getKey().getRow().getBytes()).getLong();
//      out.println(value);
//    }
//    out.close();
//  }

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "File() - name is generated in code")
public static List<Long> getRandom(String randomKeyFileDir, String allKeyFile, int num, long min, long max, Random r)
    throws IOException
{
  Path randomKeyFilePath = new Path(randomKeyFileDir, String.format("random_keys_%d.txt", num));
  File randomKeyFile = new File(randomKeyFilePath.toUri().getPath());
  List<Long> randomKeys;
  if (randomKeyFile.exists())
  {
    log.info("Using existing random keys file " + randomKeyFile.getCanonicalPath());
    randomKeys = GenerateKeys.readRandomKeys(randomKeyFile);
  }
  else
  {
    log.info("Generating new random keys file " + randomKeyFile.getCanonicalPath());
    randomKeys = GenerateKeys.getRandom(allKeyFile, num, min, max, r);
    GenerateKeys.writeRandomKeys(randomKeys, randomKeyFile);
  }
  return randomKeys;
}

public static List<Long> getSequential(String keyFile, String[] splits, int num) throws IOException
{
  log.info("getSequential: Start generating keys");
  List<Long> keys = null;
  List<Long> allKeys = getAllKeys(keyFile, num * 1000);

  for (int i = 0; i < splits.length; i++)
  {
    keys = findSequentialRange(allKeys, num, Long.parseLong(splits[i]),
        (i < splits.length - 1) ? Long.parseLong(splits[i + 1]) : Long.MAX_VALUE);
    if (keys != null)
    {
      break;
    }
  }
  if (keys == null)
  {
    throw new IOException("Unable to find a range of size " + num);
  }
  log.info("getSequential: Finished generating keys");
  return keys;
}

public static List<Long> getSequential(String keyFile, int num, long start, long end) throws IOException
{
  log.info("getSequential: Start generating keys");
  List<Long> keys = null;
  List<Long> allKeys = getAllKeys(keyFile, num * 1000);

  keys = findSequentialRange(allKeys, num, start, end);

  if (keys == null)
  {
    throw new IOException(String.format("Unable to find a range of size %d", num));
  }

  log.info("getSequential: Finished generating keys");
  return keys;
}

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "File() - name is generated in code")
public static List<Long> getAllKeys(String file, int limit) throws NumberFormatException, IOException
{
  log.info("Start getting all keys");
  ArrayList<Long> keys = new ArrayList<Long>();
  try (BufferedReader br = new BufferedReader(new FileReader(new File(file))))
  {
    String line;

    int num = 0;
    while ((line = br.readLine()) != null && num++ < limit)
    {
      keys.add(Long.valueOf(line));
    }
  }
  log.info("End getting all keys");
  return keys;
}

private static List<Long> getRandom(String file, int num, long min, long max, Random r) throws IOException
{
  log.info("getRandom: Start generating keys");

  List<Long> allKeysList;
  allKeysList = getAllKeys(file, num * 1000);
  int indexMin = -1, indexMax = -1;
  for (int i = 0; i < allKeysList.size(); i++)
  {

    // initialize indexMin to first entry >= min
    if (allKeysList.get(i) >= min && indexMin == -1)
    {
      indexMin = i;
    }
    // initialize indexMax to last entry <= max
    if (allKeysList.get(i) <= max)
    {
      indexMax = i;
    }

  }

  List<Integer> indices = findRandomIndices(num, indexMin, indexMax, r);

  List<Long> keysList = new ArrayList<Long>();
  for (Integer index : indices)
  {
    keysList.add(allKeysList.get(index));
  }

  log.info("getRandom: Finished generating keys");
  return keysList;
}

private static List<Integer> findRandomIndices(int num, int indexMin, int indexMax, Random r)
{
  log.info("Start generating random indices");

  int range = (indexMax - indexMin) + 1;
  log.info(String.format("Using index range %d %d", indexMin, indexMax));
  TreeSet<Integer> indices = new TreeSet<Integer>();

  while (indices.size() < num)
  {
    int index = r.nextInt(range) + indexMin;
    if (!indices.contains(index))
    {
      indices.add(index);
    }
  }

  List<Integer> indicesList = new ArrayList<Integer>();
  for (Integer index : indices)
  {
    indicesList.add(index);
  }
  for (Integer index : indicesList)
  {
    log.info("Index = " + index);
  }
  log.info("End generating random indices");

  return indicesList;
}

private static List<Long> findSequentialRange(List<Long> allKeys, int num, long min, long max)
{
  // returns a vector if it finds num elements in the range (min,max], otherwise null

  System.out.println(String.format("findSequentialRange: %d,%d,%d", min, max, num));
  ArrayList<Long> result = new ArrayList<Long>(num);
  for (Long key : allKeys)
  {
    if (result.size() < num && key > min && key <= max)
    {
      result.add(key);
    }
  }
  if (result.size() == num)
  {
    return result;
  }
  return null;
}

private static List<Long> readRandomKeys(File file) throws FileNotFoundException
{
  List<Long> keys = new ArrayList<Long>();
  java.util.Scanner in = new java.util.Scanner(new BufferedReader(new FileReader(file)));
  while (in.hasNextLine())
  {
    keys.add(Long.valueOf(in.nextLine()));
  }
  in.close();
  return keys;
}

private static void writeRandomKeys(List<Long> keys, File file) throws FileNotFoundException
{
  PrintWriter writer = new PrintWriter(file);
  for (Long key : keys)
  {
    writer.println(key);
  }
  writer.close();
}

@SuppressFBWarnings(value = "PREDICTABLE_RANDOM", justification = "Just getting random tileids")
@Override
public int run(String[] args, Configuration conf, ProviderProperties providerProperties)
{
  try
  {
    OptionsParser parser = new OptionsParser(args);

    //  List<Long> keys = getSequential(parser.getOptionValue("keyfile"), 100, 27000, 28000);
    //  for(Long key : keys)
    //    System.out.println(key);
    //  System.out.println("---------");

    String keyFile = parser.getOptionValue("keyfile");
    long minTile = Long.parseLong(parser.getOptionValue("mintile"));
    long maxTile = Long.parseLong(parser.getOptionValue("maxtile"));
    List<Long> keys2 =
        getRandom(keyFile, Integer.parseInt(parser.getOptionValue("tiles")), minTile, maxTile, new Random());
    for (Long key : keys2)
    {
      System.out.println(key);
      if (key < minTile || key > maxTile)
      {
        throw new IOException(String.format("Error: %d is not in range [%d,%d]", key, minTile, maxTile));
      }
    }

    return 0;
  }
  catch (ParseException | IOException e)
  {
    log.error("Exception thrown", e);
  }

  return -1;
}
}
