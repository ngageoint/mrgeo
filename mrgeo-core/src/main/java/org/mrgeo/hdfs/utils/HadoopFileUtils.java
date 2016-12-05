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

package org.mrgeo.hdfs.utils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.mrgeo.utils.HadoopUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.MessageFormat;

public class HadoopFileUtils
{
private static final Logger log = LoggerFactory.getLogger(HadoopFileUtils.class);

private HadoopFileUtils()
{
}

public static void cleanDirectory(final Path dir) throws IOException
{
  cleanDirectory(dir, true);
}

public static void cleanDirectory(final String dir) throws IOException
{
  cleanDirectory(HadoopUtils.createConfiguration(), dir);
}

public static void cleanDirectory(final Configuration conf, final String dir) throws IOException
{
  cleanDirectory(conf, dir, true);
}

public static void cleanDirectory(final String dir, final boolean recursive) throws IOException
{
  cleanDirectory(HadoopUtils.createConfiguration(), dir, recursive);
}

public static void cleanDirectory(final Configuration conf, final String dir, final boolean recursive)
    throws IOException
{
  cleanDirectory(conf, new Path(dir), recursive);
}

public static void cleanDirectory(final Path dir, final boolean recursive) throws IOException
{
  cleanDirectory(HadoopUtils.createConfiguration(), dir, recursive);
}

public static void cleanDirectory(final Configuration conf, final Path dir, final boolean recursive)
    throws IOException
{
  final FileSystem fs = getFileSystem(conf, dir);

  cleanDirectory(fs, dir);
  if (recursive)
  {
    final FileStatus files[] = fs.listStatus(dir);
    for (final FileStatus file : files)
    {
      if (file.isDir())
      {
        cleanDirectory(file.getPath(), recursive);
      }
    }
  }
}

public static void copyFileToHdfs(final String fromFile, final String toFile) throws IOException
{
  copyFileToHdfs(fromFile, toFile, true);
}

@SuppressWarnings("squid:S2095") // hadoop FileSystem cannot be closed, or else subsequent uses will fail
public static void copyFileToHdfs(final String fromFile, final String toFile,
    final boolean overwrite) throws IOException
{
  final Path toPath = new Path(toFile);
  final Path fromPath = new Path(fromFile);
  final FileSystem srcFS = HadoopFileUtils.getFileSystem(toPath);
  final FileSystem dstFS = HadoopFileUtils.getFileSystem(fromPath);

  final Configuration conf = HadoopUtils.createConfiguration();
  InputStream in = null;
  OutputStream out = null;
  try
  {
    in = srcFS.open(fromPath);
    out = dstFS.create(toPath, overwrite);

    IOUtils.copyBytes(in, out, conf, true);
  }
  catch (final IOException e)
  {
    IOUtils.closeStream(out);
    IOUtils.closeStream(in);
    throw e;
  }
}

public static void copyToHdfs(final Path fromDir, final Path toDir, final String fileName)
    throws IOException
{
  final FileSystem fs = getFileSystem(toDir);
  fs.mkdirs(toDir);
  fs.copyFromLocalFile(false, true, new Path(fromDir, fileName), new Path(toDir, fileName));
}

public static void copyToHdfs(final Path fromDir, final Path toDir, final String fileName,
    final boolean overwrite) throws IOException
{
  if (overwrite)
  {
    delete(new Path(toDir, fileName));
  }
  copyToHdfs(fromDir, toDir, fileName);
}

public static void copyToHdfs(final String fromDir, final Path toDir, final String fileName)
    throws IOException
{
  copyToHdfs(new Path(fromDir), toDir, fileName);
}

public static void copyToHdfs(final String fromDir, final String toDir, final String fileName)
    throws IOException
{
  copyToHdfs(new Path(fromDir), new Path(toDir), fileName);
}

public static void copyToHdfs(final String fromDir, final Path toDir, final String fileName,
    final boolean overwrite) throws IOException
{
  if (overwrite)
  {
    delete(new Path(toDir, fileName));
  }
  copyToHdfs(fromDir, toDir, fileName);
}

@SuppressWarnings("squid:S2095") // hadoop FileSystem cannot be closed, or else subsequent uses will fail
public static void copyToHdfs(final String fromDir, final String toDir) throws IOException
{
  final Path toPath = new Path(toDir);
  final Path fromPath = new Path(fromDir);
  final FileSystem fs = HadoopFileUtils.getFileSystem(toPath);
  fs.mkdirs(toPath);
  fs.copyFromLocalFile(false, true, fromPath, toPath);
}

public static void copyToHdfs(final String fromDir, final String toDir, final boolean overwrite)
    throws IOException
{
  if (overwrite)
  {
    delete(toDir);
  }
  copyToHdfs(fromDir, toDir);
}

public static Path createJobTmp() throws IOException
{
  return createJobTmp(HadoopUtils.createConfiguration());
}

public static Path createJobTmp(Configuration conf) throws IOException
{
  return createUniqueTmp(conf);
}

public static Path createUniqueTmp() throws IOException
{
  return createUniqueTmp(HadoopUtils.createConfiguration());
}

/**
 * Creates a unique temp directory and returns the path.
 *
 * @return
 * @throws IOException
 */
@SuppressWarnings("squid:S2095") // hadoop FileSystem cannot be closed, or else subsequent uses will fail
public static Path createUniqueTmp(Configuration conf) throws IOException
{
  // create a corresponding tmp directory for the job
  final Path tmp = createUniqueTmpPath(conf);

  final FileSystem fs = HadoopFileUtils.getFileSystem(tmp);
  if (!fs.exists(tmp))
  {
    fs.mkdirs(tmp);
  }

  return tmp;
}

/**
 * Creates a unique path but doesn't create a directory.
 *
 * @return A new unique temporary path
 * @throws IOException
 */
public static Path createUniqueTmpPath() throws IOException
{
  return createUniqueTmpPath(HadoopUtils.createConfiguration());
}

public static Path createUniqueTmpPath(Configuration conf) throws IOException
{
  return new Path(getTempDir(conf), HadoopUtils.createRandomString(40));
}

public static void create(final Path path) throws IOException
{
  create(HadoopUtils.createConfiguration(), path);
}

public static void create(final Path path, final String mode) throws IOException
{
  create(HadoopUtils.createConfiguration(), path, mode);
}

public static void create(final Configuration conf, final Path path) throws IOException
{
  create(conf, path, null);
}

@SuppressWarnings("squid:S2095") // hadoop FileSystem cannot be closed, or else subsequent uses will fail
public static void create(final Configuration conf, final Path path, final String mode) throws IOException
{
  final FileSystem fs = HadoopFileUtils.getFileSystem(conf, path);

  if (!fs.exists(path))
  {
    if (mode == null)
    {
      fs.mkdirs(path);
    }
    else
    {
      FsPermission p = new FsPermission(mode);
      fs.mkdirs(path, p);
    }
  }
}

public static void create(final String path, final String mode) throws IOException
{
  create(HadoopUtils.createConfiguration(), path, mode);
}

public static void create(final Configuration conf, final String path, final String mode) throws IOException
{
  create(conf, new Path(path), mode);
}

public static void create(final String path) throws IOException
{
  create(HadoopUtils.createConfiguration(), path);
}

public static void create(final Configuration conf, final String path) throws IOException
{
  create(conf, new Path(path));
}

public static void delete(final Path path) throws IOException
{
  delete(HadoopUtils.createConfiguration(), path);
}

/**
 * Deletes the specified path. If the scheme is s3 or s3n, then it will wait
 * until the path is gone to return or else throw an IOException indicating
 * that the path still exists. This is because s3 operates under eventual
 * consistency so deletes are not guarantted to happen right away.
 *
 * @param conf
 * @param path
 * @throws IOException
 */
public static void delete(final Configuration conf, final Path path) throws IOException
{
  final FileSystem fs = getFileSystem(conf, path);
  if (fs.exists(path))
  {
    log.info("Deleting path " + path.toString());
    if (fs.delete(path, true) == false)
    {
      throw new IOException("Error deleting directory " + path.toString());
    }
    Path qualifiedPath = path.makeQualified(fs);
    URI pathUri = qualifiedPath.toUri();
    String scheme = pathUri.getScheme().toLowerCase();
    if ("s3".equals(scheme) || "s3n".equals(scheme))
    {
      boolean stillExists = fs.exists(path);
      int sleepIndex = 0;
      // Wait for S3 to finish the deletion in phases - initially checking
      // more frequently and then less frequently as time goes by.
      int[][] waitPhases = {{60, 1}, {120, 2}, {60, 15}};
      while (sleepIndex < waitPhases.length)
      {
        int waitCount = 0;
        log.info("Sleep index " + sleepIndex);
        while (stillExists && waitCount < waitPhases[sleepIndex][0])
        {
          waitCount++;
          log.info("Waiting " + waitPhases[sleepIndex][1] + " seconds " + path.toString() + " to be deleted");
          try
          {
            Thread.sleep(waitPhases[sleepIndex][1] * 1000L);
          }
          catch (InterruptedException e)
          {
            log.warn("While waiting for " + path.toString() + " to be deleted", e);
          }
          stillExists = fs.exists(path);
          log.info("After waiting exists = " + stillExists);
        }
        sleepIndex++;
      }
      if (stillExists)
      {
        throw new IOException(path.toString() + " was not deleted within the waiting period");
      }
    }
  }
  else
  {
    log.info("Path already does not exist " + path.toString());
  }
}

public static void delete(final String path) throws IOException
{
  delete(HadoopUtils.createConfiguration(), path);
}

public static void delete(final Configuration conf, final String path) throws IOException
{
  delete(conf, new Path(path));
}

public static boolean exists(final Configuration conf, final Path path) throws IOException
{
  final FileSystem fs = getFileSystem(conf, path);
  return fs.exists(path);
}

public static boolean exists(final Configuration conf, final String path) throws IOException
{
  return exists(conf, new Path(path));
}

public static boolean exists(final Path path) throws IOException
{
  return exists(HadoopUtils.createConfiguration(), path);
}

public static boolean exists(final String path) throws IOException
{
  return exists(HadoopUtils.createConfiguration(), new Path(path));
}

public static void get(final Configuration conf, final Path fromDir, final Path toDir,
    final String fileName) throws IOException
{
  final FileSystem fs = getFileSystem(conf, fromDir);

  final FileSystem fsTo = toDir.getFileSystem(conf);
  fsTo.mkdirs(toDir);

  fs.copyToLocalFile(new Path(fromDir, fileName), new Path(toDir, fileName));
}

public static void get(final Path fromDir, final Path toDir, final String fileName)
    throws IOException
{
  get(HadoopUtils.createConfiguration(), fromDir, toDir, fileName);
}

/**
 * Returns the default file system. If the caller already has an instance of Configuration, they
 * should call getFileSystem(conf) instead to save creating a new Hadoop configuration.
 *
 * @return
 * @throws
 */
public static FileSystem getFileSystem()
{
  try
  {
    return FileSystem.get(HadoopUtils.createConfiguration());
  }
  catch (IOException e)
  {
    log.error("Error getting file system", e);
  }
  return null;
}

public static FileSystem getFileSystem(final Configuration conf) throws IOException
{
  return FileSystem.get(conf);
}

public static FileSystem getFileSystem(final Configuration conf, final Path path)
    throws IOException
{
  //return FileSystem.newInstance(path.toUri(), conf);
  return FileSystem.get(path.toUri(), conf);
}

/**
 * Returns the file system for the specified path. If the caller already has an instance of
 * Configuration, they should call getFileSystem(conf) instead to save creating a new Hadoop
 * configuration.
 *
 * @param path
 * @return
 * @throws IOException
 */
public static FileSystem getFileSystem(final Path path) throws IOException
{
  return getFileSystem(HadoopUtils.createConfiguration(), path);
}

public static long getPathSize(final Configuration conf, final Path p) throws IOException
{
  long result = 0;

  final FileSystem fs = getFileSystem(conf, p);

  final FileStatus status = fs.getFileStatus(p);
  if (status.isDir())
  {
    final FileStatus[] list = fs.listStatus(p);
    for (final FileStatus l : list)
    {
      result += getPathSize(conf, l.getPath());
    }
  }
  else
  {
    result = status.getLen();
  }
  return result;
}

/**
 * Returns a tmp directory, if the tmp directory doesn't exist it is created.
 *
 * @return
 * @throws IOException
 */
public static Path getTempDir() throws IOException
{
  return getTempDir(HadoopUtils.createConfiguration());
}

/**
 * Returns a tmp directory, if the tmp directory doesn't exist it is created.
 *
 * @return
 * @throws IOException
 */
public static Path getTempDir(final Configuration conf) throws IOException
{
  final FileSystem fs = getFileSystem(conf);
  Path parent;
  parent = fs.getHomeDirectory();
  final Path tmp = new Path(parent, "tmp");
  if (!fs.exists(tmp))
  {
    fs.mkdirs(tmp);
  }
  return tmp;
}

/**
 * Moves a file in DFS
 *
 * @param inputPath  input path
 * @param outputPath output path
 * @throws IOException if unable to move file
 */
public static void move(final Configuration conf, final Path inputPath, final Path outputPath)
    throws IOException
{
  final FileSystem fs = getFileSystem(conf, inputPath);

  if (!fs.rename(inputPath, outputPath))
  {
    throw new IOException("Cannot rename " + inputPath.toString() + " to " +
        outputPath.toString());
  }
  if (!fs.exists(outputPath))
  {
    throw new IOException("Output path does not exist: " + outputPath.toString());
  }
}

public static void
move(final Configuration conf, final String inputPath, final String outputPath)
    throws IOException
{
  move(conf, new Path(inputPath), new Path(outputPath));
}

/**
 * For each subdirectory below srcParent, move that subdirectory below targetParent. If a
 * sub-directory already exists under the targetParent, delete it and then perform the move. This
 * function does not moves files below the parent, just directories.
 *
 * @param conf
 * @param targetParent
 * @throws IOException
 */
public static void moveChildDirectories(final Configuration conf, final Path srcParent,
    final Path targetParent) throws IOException
{
  // We want to move each of the child directories of the output path
  // to the actual target directory (toPath). If any of the subdirs
  // already exists under the target path, we need to delete it first,
  // then perform the move.

  final FileSystem fs = getFileSystem(conf, srcParent);
  if (!fs.exists(targetParent))
  {
    fs.mkdirs(targetParent);
  }
  final FileStatus[] children = fs.listStatus(srcParent);
  for (final FileStatus stat : children)
  {
    if (stat.isDir())
    {
      final Path srcPath = stat.getPath();
      final String name = srcPath.getName();
      final Path target = new Path(targetParent, name);
      if (fs.exists(target))
      {
        fs.delete(target, true);
      }
      if (fs.rename(srcPath, targetParent) == false)
      {
        final String msg = MessageFormat.format(
            "Error moving temporary file {0} to output path {1}.", srcPath.toString(), target
                .toString());
        throw new IOException(msg);
      }
    }
  }
}

public static InputStream open(final Configuration conf, final Path path) throws IOException
{
  final FileSystem fs = getFileSystem(conf, path);

  if (fs.exists(path))
  {
    return fs.open(path, 131072);
  }

  throw new FileNotFoundException("File not found: " + path.toUri().toString());
}

public static InputStream open(final Path path) throws IOException
{
  return open(HadoopUtils.createConfiguration(), path);
}

/**
 * Returns a DFS path without the file system in the URL (not sure if there is a better way to do
 * this).
 *
 * @param path DFS path
 * @return e.g. for input: hdfs://localhost:9000/mrgeo/images/test-1, returns /mrgeo/images/test-1
 */
public static Path unqualifyPath(final Path path)
{
  return new Path(path.toUri().getPath());
}

public static String unqualifyPath(final String path)
{
  return new Path((new Path(path)).toUri().getPath()).toString();
}

public static Path resolveName(final String input) throws IOException, URISyntaxException
{
  return resolveName(input, true);
}

public static Path resolveName(final String input, boolean checkForExistance) throws IOException, URISyntaxException
{
  return resolveName(HadoopUtils.createConfiguration(), input, checkForExistance);
}

@SuppressWarnings("squid:S1166") // Exception caught and handled
@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "method only makes complete URI out of the name")
public static Path resolveName(final Configuration conf, final String input,
    boolean checkForExistance) throws IOException, URISyntaxException
{
  // It could be either HDFS or local file system
  File f = new File(input);
  if (f.exists())
  {
    try
    {
      return new Path(new URI("file://" + input));
    }
    catch (URISyntaxException ignored)
    {
      // The URI is invalid, so let's continue to try to open it in HDFS
    }
  }
  Path p = new Path(new URI(input));
  if (!checkForExistance || exists(conf, p))
  {
    return p;
  }
  throw new IOException("Cannot find: " + input);
}

private static void cleanDirectory(final FileSystem fs, final Path dir) throws IOException
{
  final Path temp = new Path(dir, "_temporary");
  if (fs.exists(temp))
  {
    fs.delete(temp, true);
  }

  final Path success = new Path(dir, "_SUCCESS");
  if (fs.exists(success))
  {
    fs.delete(success, true);
  }
}
}
