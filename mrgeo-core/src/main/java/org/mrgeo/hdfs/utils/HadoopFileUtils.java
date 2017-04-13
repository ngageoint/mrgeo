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

package org.mrgeo.hdfs.utils;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.utils.HadoopUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.UriBuilder;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.text.MessageFormat;
import java.util.Scanner;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
    if ("s3".equals(scheme) || "s3a".equals(scheme) || "s3n".equals(scheme))
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

private static long copyFileFromS3(AmazonS3 s3Client, URI uri, File localFile,
                                   FileOutputStream fos) throws IOException
{
  S3Object object = null;
  try {
    log.debug("Reading from bucket " + uri.getHost() + " and key " + uri.getPath());
    // The AWS key is relative, so we eliminate the leading slash
    object = s3Client.getObject(
            new GetObjectRequest(uri.getHost(), uri.getPath().substring(1)));
  }
  catch(com.amazonaws.AmazonServiceException e) {
    log.error("Got AmazonServiceException while getting " + uri.toString() + ": " + e.getMessage(), e);
    throw new IOException(e);
  }
  catch(com.amazonaws.AmazonClientException e) {
    log.error("Got AmazonClientException while getting " + uri.toString() + ": " + e.getMessage(), e);
    throw new IOException(e);
  }

  log.debug("In copyFileFromS3 - after force mkdir");
  InputStream objectData = object.getObjectContent();
  try {
    long byteCount = org.apache.commons.io.IOUtils.copyLarge(objectData, fos);
    return byteCount;
  }
  finally {
    objectData.close();
    log.debug("Length of local " + localFile.getAbsolutePath() + " is " + localFile.length());
  }
}

public static class SequenceFileReaderWrapper {
  private Configuration conf;
  private Path path;
  private SequenceFile.Reader reader;
  private Path localPath;
  private S3CacheEntry cacheEntry;

  public SequenceFileReaderWrapper(Path path, Configuration conf) throws IOException
  {
    this.path = path;
    this.conf = conf;
  }

  public String toString()
  {
    return "SequenceFileReaderWrapper for " +
            ((localPath == null) ? "null" : localPath.toString()) +
            " in lieu of " + path.toString();
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "File() - name is gotten from mrgeo.conf")
  public SequenceFile.Reader getReader() throws IOException
  {
    if (reader == null) {
      FileSystem fs = path.getFileSystem(conf);
      Path qualifiedPath = path.makeQualified(fs);
      URI pathUri = qualifiedPath.toUri();
      String scheme = pathUri.getScheme().toLowerCase();
      if ("s3".equals(scheme) || "s3a".equals(scheme) || "s3n".equals(scheme)) {
        S3Cache localS3Cache = getS3Cache();
        File tmpFile = new File(cacheDir, pathUri.getPath().substring(1));
        // Copy the file from S3 to the local drive and return a reader for that file
        Path tryLocalPath = new Path("file://" + tmpFile.getParentFile().getAbsolutePath());
        cacheEntry = localS3Cache.getEntry(path.toString(), tryLocalPath, tmpFile, null);
        if (cacheEntry != null) {
          cacheEntry.readLock();
          localPath = cacheEntry.getLocalPath();
        }
        else {
          // The file(s) do not exist locally, so bring them down.
          localPath = new Path("file://" + tmpFile.getParentFile().getAbsolutePath());
          // Add a placeholder entry to the cache so that other threads can see that
          // S3 files are being copied for this resource. Once the copy is done,
          // this placeholder will be replaced with a real cache entry. Set the
          // local file objects to null to prevent deleting them wher the placeholder
          // entry is replaced later.
          cacheEntry = localS3Cache.createEntry(localPath, tmpFile);
          boolean writeLocked = cacheEntry.tryWriteLock();
          if (writeLocked) {
            try {
              // Add the entry to the cache even before the files are copied so that if
              // another thread tries to copy the same map file from S3, it will get an
              // existing cache entry and wait on the lock to be released (which happens
              // after the files have been copied - below).
              s3Cache.addEntry(path.toString(), cacheEntry);
              AmazonS3 s3Client = new AmazonS3Client(new DefaultAWSCredentialsProviderChain());
              // Copy the index and data files locally
              long fileSize = copyFileFromS3(s3Client, pathUri, tmpFile, cacheEntry.getPrimaryFileOutputStream());
              log.debug("Copied data from " + pathUri.toString() + " to " + tmpFile.getAbsolutePath() + ": " + fileSize);
              log.debug("SequenceFile " + pathUri.toString() + " size is " + fileSize);
            } finally {
              // Release the write lock on the original cache entry to allow locks
              // to readers waiting for the write to complete.
              log.debug("Downgrading from writeLock to readLock from thread " + Thread.currentThread().getId() + " on cacheEntry " + cacheEntry.toString());
              cacheEntry.releaseWriteLock();
              cacheEntry.readLock();
            }
          }
          else {
            // Another process is trying to fetch the same file from S3, so we need
            // attempt a read lock, which will wait until the write is done.
            cacheEntry.readLock();
          }
        }
        log.debug("Opening local reader to " + localPath.toString());
        reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(localPath));
      } else {
        log.debug("Not caching locally: " + path.toString());
        localPath = null;
        reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));
      }
    }
    return reader;
  }

  public void close() throws IOException
  {
    if (reader != null) {
      log.debug("Closing: " + path.toString());
      reader.close();
      reader = null;
      // Inform the file cache that we are done with the file
      if (cacheEntry != null) {
        cacheEntry.releaseReadLock();
      }
    }
  }
}

public static class MapFileReaderWrapper
{
  private Configuration conf;
  private Path path;
  private MapFile.Reader reader;
  private Path localPath;
  private S3CacheEntry cacheEntry;

  public MapFileReaderWrapper(Path path, Configuration conf)
  {
    this.path = path;
    this.conf = conf;
  }

  public MapFileReaderWrapper(MapFile.Reader reader, Path path)
  {
    this.reader = reader;
    this.path = path;
  }

  public String toString()
  {
    return "MapFileReaderWrapper for " +
            ((localPath == null) ? "null" : localPath.toString()) +
            " in lieu of " + path.toString();
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "File() - name is generated in code")
  public MapFile.Reader getReader() throws IOException
  {
    if (reader == null) {
      FileSystem fs = path.getFileSystem(conf);
      Path qualifiedPath = path.makeQualified(fs);
      URI pathUri = qualifiedPath.toUri();
      URI indexUri = UriBuilder.fromUri(pathUri).path("index").build();
      URI dataUri = UriBuilder.fromUri(pathUri).path("data").build();
      String scheme = pathUri.getScheme().toLowerCase();
      if ("s3".equals(scheme) || "s3a".equals(scheme) || "s3n".equals(scheme)) {
        S3Cache localS3Cache = getS3Cache();
        log.debug("cacheDir = " + cacheDir.getAbsolutePath());
        File tmpIndexFile = new File(cacheDir, indexUri.getPath().substring(1));
        File tmpDataFile = new File(cacheDir, dataUri.getPath().substring(1));
        // Copy the file from S3 to the local drive if it is not already there
        // and return a reader for that file. Use locking to prevent multiple
        // processes from copying the same file at the same time.
        Path tryLocalPath = new Path("file://" + tmpIndexFile.getParentFile().getAbsolutePath());
        log.debug("Checking if cache entry exists at " + tmpIndexFile.getAbsolutePath());
        cacheEntry = localS3Cache.getEntry(path.toString(), tryLocalPath, tmpIndexFile, tmpDataFile);
        if (cacheEntry != null) {
          cacheEntry.readLock();
          localPath = cacheEntry.getLocalPath();
        }
        else {
          // The file(s) do not exist locally, so bring them down.
          localPath = new Path("file://" + tmpIndexFile.getParentFile().getAbsolutePath());
          // Add a placeholder entry to the cache so that other threads can see that
          // S3 files are being copied for this resource. Once the copy is done,
          // this placeholder will be replaced with a real cache entry. Set the
          // local file objects to null to prevent deleting them wher the placeholder
          // entry is replaced later.
          cacheEntry = localS3Cache.createEntry(localPath, tmpIndexFile, tmpDataFile);
          boolean writeLocked = cacheEntry.tryWriteLock();
          if (writeLocked) {
            try {
              // Add the entry to the cache even before the files are copied so that if
              // another thread tries to copy the same map file from S3, it will get an
              // existing cache entry and wait on the lock to be released (which happens
              // after the files have been copied - below).
              localS3Cache.addEntry(path.toString(), cacheEntry);
              AmazonS3 s3Client = new AmazonS3Client(new DefaultAWSCredentialsProviderChain());
              // Copy the index and data files locally. Do the index file last since it also
              // acts as the lock file for both and we don't want to release the lock until
              // all the files are copied.
              FileUtils.forceMkdir(tmpDataFile.getParentFile());
              FileOutputStream fosData = new FileOutputStream(tmpDataFile);
              try {
                long dataSize = copyFileFromS3(s3Client, dataUri, tmpDataFile, fosData);
                log.debug("Copied data from " + dataUri.toString() + " to " +
                        tmpDataFile.getAbsolutePath() + ": " + dataSize);
              }
              finally {
                fosData.close();
              }
              long indexSize = copyFileFromS3(s3Client, indexUri, tmpIndexFile,
                      cacheEntry.getPrimaryFileOutputStream());
              log.debug("Copied data from " + indexUri.toString() + " to " +
                      tmpIndexFile.getAbsolutePath() + ": " + indexSize);
              log.debug("Index " + indexUri.toString() + " size is " + indexSize);
            } finally {
              // Release the write lock on the original cache entry to allow locks
              // to readers waiting for the write to complete.
              log.debug("Downgrading from writeLock to readLock from thread " +
                      Thread.currentThread().getId() + " on cacheEntry " + cacheEntry.toString());
              cacheEntry.releaseWriteLock();
              cacheEntry.readLock();
            }
          }
          else {
            // Another process is trying to fetch the same file from S3, so we need
            // attempt a read lock, which will wait until the write is done.
            cacheEntry.readLock();
          }
        }
        log.debug("Opening local reader to " + localPath.toString());
        reader = new MapFile.Reader(localPath, conf);
      } else {
        log.debug("Not caching locally: " + path.toString());
        localPath = null;
        reader = new MapFile.Reader(path, conf);
      }
    }
    return reader;
  }

  public void close() throws IOException
  {
    if (reader != null) {
      log.debug("Closing: " + path.toString());
      reader.close();
      reader = null;
      // Inform the file cache that we are done with the file
      if (cacheEntry != null) {
        cacheEntry.releaseReadLock();
      }
    }
  }
}

public interface S3CacheEntry
{
  void readLock() throws IOException;
  void writeLock() throws IOException;
  boolean tryWriteLock() throws IOException;
  void releaseReadLock() throws IOException;
  void releaseWriteLock() throws IOException;
  Path getLocalPath();
  void cleanup() throws IOException;
  FileOutputStream getPrimaryFileOutputStream() throws IOException;
}

@SuppressFBWarnings(value = {"SE_BAD_FIELD", "SE_NO_SERIALVERSIONID"}, justification = "Class is never serialized")
public static class S3MemoryCacheEntry implements S3CacheEntry
{
  private Path localPath;
  private File primaryFile;
  private File secondaryFile;
  private ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
  private FileOutputStream fos;

  S3MemoryCacheEntry(Path localPath, File primaryFile)
  {
    this.localPath = localPath;
    this.primaryFile = primaryFile;
  }

  S3MemoryCacheEntry(Path localPath, File primaryFile, File secondaryFile)
  {
    this.localPath = localPath;
    this.primaryFile = primaryFile;
    this.secondaryFile = secondaryFile;
  }

  @Override
  public FileOutputStream getPrimaryFileOutputStream() throws IOException
  {
    if (fos == null) {
      FileUtils.forceMkdir(primaryFile.getParentFile());
      fos = new FileOutputStream(primaryFile);
    }
    return fos;
  }

  @Override
  public Path getLocalPath() { return localPath; }

  @Override
  public void cleanup() throws IOException
  {
    writeLock();
    try {
      if (primaryFile != null) {
        log.debug("Deleting cached file " + primaryFile.getAbsolutePath());
        boolean file1Deleted = primaryFile.delete();
        if (!file1Deleted) {
          log.error("Unable to delete local cache file " + primaryFile.getAbsolutePath());
        }
        primaryFile = null;
      }
      if (secondaryFile != null) {
        log.debug("Deleting cached file " + secondaryFile.getAbsolutePath());
        boolean file2Deleted = secondaryFile.delete();
        if (!file2Deleted) {
          log.error("Unable to delete local cache file " + secondaryFile.getAbsolutePath());
        }
        secondaryFile = null;
      }
    }
    finally {
      releaseWriteLock();
    }
  }

  @Override
  public void readLock() throws IOException
  {
    log.debug("Lock readLock lock from thread " + Thread.currentThread().getId() +
            " on cacheEntry " + this.toString());
    rwLock.readLock().lock();
  }

  @Override
  public void writeLock() throws IOException
  {
    log.debug("Lock writeLock lock from thread " + Thread.currentThread().getId() +
            " on cacheEntry " + this.toString());
    rwLock.writeLock().lock();
  }

  @Override
  public boolean tryWriteLock() throws IOException
  {
    log.debug("Lock writeLock lock from thread " + Thread.currentThread().getId() +
            " on cacheEntry " + this.toString());
    return rwLock.writeLock().tryLock();
  }

  @Override
  public void releaseReadLock()
  {
    log.debug("Lock readLock unlock from thread " + Thread.currentThread().getId() +
            " on cacheEntry " + this.toString());
    rwLock.readLock().unlock();
  }

  @Override
  public void releaseWriteLock() throws IOException
  {
    log.debug("Lock writeLock unlock from thread " + Thread.currentThread().getId() +
            " on cacheEntry " + this.toString());
    rwLock.writeLock().unlock();
    // The lock is memory-based, but we want to make sure to close the file output stream
    // when we release the lock so we don't retain open files.
    if (fos != null) {
      fos.close();
      fos = null;
    }
  }
}

public static class S3FileCacheEntry implements S3CacheEntry
{
  private Path localPath;
  private File primaryFile;
  private File secondaryFile;
  private FileLock readFileLock;
  private FileLock writeFileLock;
  private FileOutputStream fos;
  private FileInputStream fis;
  private FileChannel readChannel;
  private FileChannel writeChannel;

  S3FileCacheEntry(Path localPath, File localFile)
  {
    this.localPath = localPath;
    this.primaryFile = localFile;
  }

  S3FileCacheEntry(Path localPath, File primaryFile, File secondaryFile)
  {
    this.localPath = localPath;
    this.primaryFile = primaryFile;
    this.secondaryFile = secondaryFile;
  }

  public FileOutputStream getPrimaryFileOutputStream() throws IOException
  {
    return fos;
  }

  @Override
  public void readLock() throws IOException
  {
    initReadChannel();
    log.debug("readLock from thread " + Thread.currentThread().getId() + " on entry " +
            primaryFile.getAbsolutePath());
    readFileLock = readChannel.lock(0, Long.MAX_VALUE, true);
    log.debug("  readLock from thread " + Thread.currentThread().getId() + " done ");
    // Some operating systems do not support shared locks, so report that here. But
    // keep the lock active - the system will just run slower.
    if (!readFileLock.isShared()) {
      log.warn("Unable to create a shared lock on " + getLocalPath().toString());
    }
  }

  @Override
  public void writeLock() throws IOException
  {
    initWriteChannel();
    log.debug("writeLock from thread " + Thread.currentThread().getId() + " on entry " +
            primaryFile.getAbsolutePath());
    writeFileLock = writeChannel.lock(0, Long.MAX_VALUE, false);
    log.debug("  done writeLock from thread " + Thread.currentThread().getId() + " on entry " +
            primaryFile.getAbsolutePath());
  }

  @Override
  public boolean tryWriteLock() throws IOException
  {
    initWriteChannel();
    log.debug("tryWriteLock from thread " + Thread.currentThread().getId() + " on entry " +
            primaryFile.getAbsolutePath());
    writeFileLock = writeChannel.tryLock(0, Long.MAX_VALUE, false);
    log.debug("  done tryWriteLock from thread " + Thread.currentThread().getId() + " on entry " +
            primaryFile.getAbsolutePath());
    return (writeFileLock != null);
  }

  @Override
  public void releaseReadLock() throws IOException
  {
    if (readChannel != null) {
      log.debug("releaseReadLock from thread " + Thread.currentThread().getId() + " on entry " +
              primaryFile.getAbsolutePath());
      readChannel.close();
      readChannel = null;
      log.debug("  done releaseReadLock from thread " + Thread.currentThread().getId() +
              " on entry " + primaryFile.getAbsolutePath());
    }
    if (fis != null) {
      fis.close();
      fis = null;
    }
  }

  @Override
  public void releaseWriteLock() throws IOException
  {
    if (writeChannel != null) {
      log.debug("releaseWriteLock from thread " + Thread.currentThread().getId() + " on entry " +
              primaryFile.getAbsolutePath());
      writeChannel.close();
      writeChannel = null;
      log.debug("  done releaseWriteLock from thread " + Thread.currentThread().getId() + " on entry " + primaryFile.getAbsolutePath());
    }
    if (fos != null) {
      fos.close();
      fos = null;
    }
  }

  @Override
  public Path getLocalPath() { return localPath; }

  @Override
  public void cleanup() throws IOException
  {
    writeLock();
    try {
      if (secondaryFile != null) {
        log.debug("Deleting cached file " + secondaryFile.getAbsolutePath());
        boolean file2Deleted = secondaryFile.delete();
        if (!file2Deleted) {
          log.error("Unable to delete local cache file " + secondaryFile.getAbsolutePath());
        }
        secondaryFile = null;
      }
      if (primaryFile != null) {
        log.debug("Deleting cached file " + primaryFile.getAbsolutePath());
        boolean file1Deleted = primaryFile.delete();
        if (!file1Deleted) {
          log.error("Unable to delete local cache file " + primaryFile.getAbsolutePath());
        }
        primaryFile = null;
      }
    }
    finally {
      releaseWriteLock();
      releaseReadLock();
    }
  }

  private void initWriteChannel() throws IOException
  {
    if (writeChannel == null) {
      FileUtils.forceMkdir(primaryFile.getParentFile());
      fos = new FileOutputStream(primaryFile);
      writeChannel = fos.getChannel();
    }
  }

  private void initReadChannel() throws IOException
  {
    if (readChannel == null) {
      fis = new FileInputStream(primaryFile);
      readChannel = fis.getChannel();
    }
  }

  @SuppressFBWarnings(value = "OS_OPEN_STREAM", justification = "stream is closed when lock is released")
  public static void run(String[] args)
  {
    Path lockPath = new Path(args[0]);
    File file1 = new File(args[1]);
    File file2 = new File(args[2]);

    Scanner scanner = new Scanner(System.in);
    System.out.print("Command (twl, wl, rwl, rl, rrl, rwlrl, wf, quit): ");
    String line = scanner.nextLine();
    S3FileCacheEntry entry = null;
    while (!line.equalsIgnoreCase("quit")) {
      if (entry ==null) {
        entry = new S3FileCacheEntry(lockPath, file1, file2);
      }
      if (line.equalsIgnoreCase("rl")) {
        try {
          entry.readLock();
          System.out.println("result: success");
        } catch (Throwable e) {
          System.out.println("Error from readLock: " + e);
        }
      } else if (line.equalsIgnoreCase("rrl")) {
        try {
          entry.releaseReadLock();
          entry = null;
          System.out.println("result: success");
        } catch (Throwable e) {
          System.out.println("Error from releaseReadLock: " + e);
        }
      } else if (line.equalsIgnoreCase("twl")) {
        try {
          System.out.println("result: " + entry.tryWriteLock());
        } catch (Throwable e) {
          System.out.println("Error from tryWriteLock: " + e);
        }
      } else if (line.equalsIgnoreCase("wl")) {
        try {
          entry.writeLock();
          System.out.println("result: success");
        } catch (Throwable e) {
          System.out.println("Error from writeLock: " + e);
        }
      } else if (line.equalsIgnoreCase("rwl")) {
        try {
          entry.releaseWriteLock();
          entry = null;
          System.out.println("result: success");
        } catch (Throwable e) {
          System.out.println("Error from releaseWriteLock: " + e);
        }
      } else if (line.equalsIgnoreCase("rwlrl")) {
        try {
          entry.releaseWriteLock();
          System.out.println("released write lock, now getting read lock");
          entry.readLock();
          System.out.println("result: success");
        } catch (Throwable e) {
          System.out.println("Error from releaseWriteLock: " + e);
        }
      } else if (line.equalsIgnoreCase("wf")) {
        try {
          FileOutputStream fos = entry.getPrimaryFileOutputStream();
          PrintWriter pw = new PrintWriter(fos);
          pw.println("blah");
          pw.flush();
          // Do not close the stream here. That is done when the write lock is released.
        } catch (Throwable e) {
          System.out.println("Error writing lock file: " + e);
        }
      }
      System.out.print("Command (twl, wl, rwl, rl, rrl, rwlrl, wf, quit): ");
      line = scanner.nextLine();
    }
  }
}

public interface S3Cache
{
  void init() throws IOException;
  S3CacheEntry getEntry(String key, Path localPath, File primaryFile, File secondaryFile);
  void addEntry(String key, S3CacheEntry entry);
  // For now, we only need an entry with either 1 or 2 local files since we only
  // need support for sequence files and map files at the moment. If we ever need
  // more, then change the following two methods to a single signature that takes
  // a variable number of local files. For now, it avoids the overhead of storing
  // an array for each entry.
  S3CacheEntry createEntry(Path localPath, File primaryFile);
  S3CacheEntry createEntry(Path localPath, File primaryFile, File secondaryFile);
}

public static class CacheCleanupHook extends Thread
{
  private File cleanupDir;

  CacheCleanupHook(File cleanupDir)
  {
    this.cleanupDir = cleanupDir;
  }

  @Override
  public void run()
  {
    try {
      FileUtils.cleanDirectory(cleanupDir);
      System.out.println("In shutdown, deleting " + cleanupDir.getAbsolutePath());
      log.debug("In shutdown, deleting " + cleanupDir.getAbsolutePath());
      FileUtils.deleteDirectory(cleanupDir);
    } catch (IOException e) {
      log.debug("Unable to clean MrGeo S3 cache directory " + e.getMessage());
    }
  }
}

public static class CacheCleanupOnElementRemoval implements RemovalListener<String, S3CacheEntry>
{
  @Override
  public void onRemoval(RemovalNotification<String, S3CacheEntry> notification) {
    log.debug("S3 cache removal key: " + notification.getKey() + " with cause " + notification.getCause().toString() + " from thread " + Thread.currentThread().getId());
    log.debug("S3 cache removal value: " + notification.getValue().toString() + " from thread " + Thread.currentThread().getId());
    try {
      notification.getValue().cleanup();
    } catch (IOException e) {
      log.error("Got exception while cleaning up a cache entry for " + notification.getKey(), e);
    }
  }
}

public static class S3MemoryCache implements S3Cache
{
  private Cache<String, S3CacheEntry> s3FileCache;

  @Override
  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "File() - name is gotten from mrgeo.conf")
  public void init() throws IOException
  {
    if (cacheDir == null) {
      // For the memory-based cache approach, the locally cached S3 files
      // must be cleaned up when the process exits. Also, the local cache
      // dir is in a temp subdir so it does not conflict with other running
      // processes that use this same cache approach (e.g. multiple instances
      // of MrGeo web services running on a single machine)
      String strTmpDir = MrGeoProperties.getInstance().getProperty("s3.cache.dir", "/tmp/mrgeo");
      FileUtils.forceMkdir(new File(strTmpDir));
      java.nio.file.Path tmpPath = java.nio.file.Files.createTempDirectory(java.nio.file.Paths.get(strTmpDir), "mrgeo");
      cacheDir = tmpPath.toFile();
    }
    if (s3FileCache == null) {
      long s3CacheSize = 200L;
      String strCacheSize = MrGeoProperties.getInstance().getProperty("s3.cache.size.elements");
      if (strCacheSize != null && !strCacheSize.isEmpty()) {
        s3CacheSize = Long.parseLong(strCacheSize);
      }
      log.debug("cache size " + s3CacheSize);
      String strLocker = MrGeoProperties.getInstance().getProperty("s3.cache.locker", "file");
      useFileLocker = strLocker.equalsIgnoreCase("file");
      Runtime.getRuntime().addShutdownHook(new CacheCleanupHook(cacheDir));
      s3FileCache = CacheBuilder
              .newBuilder().maximumSize(s3CacheSize)
              .removalListener(new CacheCleanupOnElementRemoval()).build();
    }
  }

  @Override
  public S3CacheEntry getEntry(final String key, final Path localPath, final File primaryFile,
                               final File secondaryFile)
  {
    return s3FileCache.getIfPresent(key);
  }

  @Override
  public void addEntry(final String key, final S3CacheEntry entry)
  {
    s3FileCache.put(key, entry);
  }

  @Override
  public S3CacheEntry createEntry(final Path localPath, final File primaryFile)
  {
    return new S3MemoryCacheEntry(localPath, primaryFile);
  }

  @Override
  public S3CacheEntry createEntry(Path localPath, File primaryFile, File secondaryFile)
  {
    return new S3MemoryCacheEntry(localPath, primaryFile, secondaryFile);
  }
}

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "File() - name is configured by an admin")
public static class S3FileCache implements S3Cache
{
  @Override
  public void init() throws IOException
  {
    if (cacheDir == null) {
      // When using a file-based S3 local cache, all processes use the same
      // file-based cache and file locking to coordinate access. There is no need
      // the delete the cache directory when the process exits because it will
      // automatically re-use the cache contents the next time it starts up.
      String strTmpDir = MrGeoProperties.getInstance().getProperty("s3.cache.dir", "/tmp/mrgeo");
      FileUtils.forceMkdir(new File(strTmpDir));
      cacheDir = new File(strTmpDir);
    }
  }

  @Override
  public S3CacheEntry getEntry(String key, Path localPath, File primaryFile, File secondaryFile)
  {
    if (primaryFile.exists()) {
      log.debug("Found existing entry for " + key);
      return new S3FileCacheEntry(localPath, primaryFile, secondaryFile);
    }
    log.debug("Did not find existing entry for " + key);
    return null;
  }

  @Override
  public void addEntry(String key, S3CacheEntry entry)
  {
    // Nothing to do
  }

  @Override
  public S3CacheEntry createEntry(Path localPath, File primaryFile)
  {
    return new S3FileCacheEntry(localPath, primaryFile);
  }

  @Override
  public S3CacheEntry createEntry(Path localPath, File primaryFile, File secondaryFile)
  {
    return new S3FileCacheEntry(localPath, primaryFile, secondaryFile);
  }
}

private static S3Cache s3Cache;
private static File cacheDir;
private static boolean useFileLocker;

private static synchronized S3Cache getS3Cache() throws IOException
{
  if (s3Cache == null) {
    String strLocker = MrGeoProperties.getInstance().getProperty("s3.cache.locker", "file");
    useFileLocker = strLocker.equalsIgnoreCase("file");
    s3Cache = (useFileLocker) ? new S3FileCache() : new S3MemoryCache();
    s3Cache.init();
  }
  return s3Cache;
}

public static void main(String[] args)
{
  S3FileCacheEntry.run(args);
}
}
