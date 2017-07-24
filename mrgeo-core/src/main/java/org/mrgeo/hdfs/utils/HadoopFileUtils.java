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
import org.mrgeo.utils.HadoopUtils;
import org.mrgeo.utils.S3Utils;
import org.mrgeo.utils.S3Utils.S3Cache;
import org.mrgeo.utils.S3Utils.S3CacheEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.UriBuilder;
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

public static void cleanDirectory(Path dir) throws IOException
{
  cleanDirectory(dir, true);
}

public static void cleanDirectory(String dir) throws IOException
{
  cleanDirectory(HadoopUtils.createConfiguration(), dir);
}

public static void cleanDirectory(Configuration conf, String dir) throws IOException
{
  cleanDirectory(conf, dir, true);
}

public static void cleanDirectory(String dir, boolean recursive) throws IOException
{
  cleanDirectory(HadoopUtils.createConfiguration(), dir, recursive);
}

public static void cleanDirectory(Configuration conf, String dir, boolean recursive)
    throws IOException
{
  cleanDirectory(conf, new Path(dir), recursive);
}

public static void cleanDirectory(Path dir, boolean recursive) throws IOException
{
  cleanDirectory(HadoopUtils.createConfiguration(), dir, recursive);
}

public static void cleanDirectory(Configuration conf, Path dir, boolean recursive)
    throws IOException
{
  FileSystem fs = getFileSystem(conf, dir);

  cleanDirectory(fs, dir);
  if (recursive)
  {
    FileStatus files[] = fs.listStatus(dir);
    for (FileStatus file : files)
    {
      if (file.isDir())
      {
        cleanDirectory(file.getPath(), recursive);
      }
    }
  }
}

public static void copyFileToHdfs(String fromFile, String toFile) throws IOException
{
  copyFileToHdfs(fromFile, toFile, true);
}

@SuppressWarnings("squid:S2095") // hadoop FileSystem cannot be closed, or else subsequent uses will fail
public static void copyFileToHdfs(String fromFile, String toFile,
    boolean overwrite) throws IOException
{
  Path toPath = new Path(toFile);
  Path fromPath = new Path(fromFile);
  FileSystem srcFS = getFileSystem(toPath);
  FileSystem dstFS = getFileSystem(fromPath);

  Configuration conf = HadoopUtils.createConfiguration();
  InputStream in = null;
  OutputStream out = null;
  try
  {
    in = srcFS.open(fromPath);
    out = dstFS.create(toPath, overwrite);

    IOUtils.copyBytes(in, out, conf, true);
  }
  catch (IOException e)
  {
    IOUtils.closeStream(out);
    IOUtils.closeStream(in);
    throw e;
  }
}

public static void copyToHdfs(Path fromDir, Path toDir, String fileName)
    throws IOException
{
  FileSystem fs = getFileSystem(toDir);
  fs.mkdirs(toDir);
  fs.copyFromLocalFile(false, true, new Path(fromDir, fileName), new Path(toDir, fileName));
}

public static void copyToHdfs(Path fromDir, Path toDir, String fileName,
    boolean overwrite) throws IOException
{
  if (overwrite)
  {
    delete(new Path(toDir, fileName));
  }
  copyToHdfs(fromDir, toDir, fileName);
}

public static void copyToHdfs(String fromDir, Path toDir, String fileName)
    throws IOException
{
  copyToHdfs(new Path(fromDir), toDir, fileName);
}

public static void copyToHdfs(String fromDir, String toDir, String fileName)
    throws IOException
{
  copyToHdfs(new Path(fromDir), new Path(toDir), fileName);
}

public static void copyToHdfs(String fromDir, Path toDir, String fileName,
    boolean overwrite) throws IOException
{
  if (overwrite)
  {
    delete(new Path(toDir, fileName));
  }
  copyToHdfs(fromDir, toDir, fileName);
}

@SuppressWarnings("squid:S2095") // hadoop FileSystem cannot be closed, or else subsequent uses will fail
public static void copyToHdfs(String fromDir, String toDir) throws IOException
{
  Path toPath = new Path(toDir);
  Path fromPath = new Path(fromDir);
  FileSystem fs = getFileSystem(toPath);
  fs.mkdirs(toPath);
  fs.copyFromLocalFile(false, true, fromPath, toPath);
}

public static void copyToHdfs(String fromDir, String toDir, boolean overwrite)
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
  Path tmp = createUniqueTmpPath(conf);

  FileSystem fs = getFileSystem(tmp);
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

public static void create(Path path) throws IOException
{
  create(HadoopUtils.createConfiguration(), path);
}

public static void create(Path path, String mode) throws IOException
{
  create(HadoopUtils.createConfiguration(), path, mode);
}

public static void create(Configuration conf, Path path) throws IOException
{
  create(conf, path, null);
}

@SuppressWarnings("squid:S2095") // hadoop FileSystem cannot be closed, or else subsequent uses will fail
public static void create(Configuration conf, Path path, String mode) throws IOException
{
  FileSystem fs = getFileSystem(conf, path);

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

public static void create(String path, String mode) throws IOException
{
  create(HadoopUtils.createConfiguration(), path, mode);
}

public static void create(Configuration conf, String path, String mode) throws IOException
{
  create(conf, new Path(path), mode);
}

public static void create(String path) throws IOException
{
  create(HadoopUtils.createConfiguration(), path);
}

public static void create(Configuration conf, String path) throws IOException
{
  create(conf, new Path(path));
}

public static void delete(Path path) throws IOException
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
public static void delete(Configuration conf, Path path) throws IOException
{
  FileSystem fs = getFileSystem(conf, path);
  if (fs.exists(path))
  {
    log.info("Deleting path " + path);
    if (!fs.delete(path, true))
    {
      throw new IOException("Error deleting directory " + path);
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
          log.info("Waiting " + waitPhases[sleepIndex][1] + " seconds " + path + " to be deleted");
          try
          {
            Thread.sleep(waitPhases[sleepIndex][1] * 1000L);
          }
          catch (InterruptedException e)
          {
            log.warn("While waiting for " + path + " to be deleted", e);
          }
          stillExists = fs.exists(path);
          log.info("After waiting exists = " + stillExists);
        }
        sleepIndex++;
      }
      if (stillExists)
      {
        throw new IOException(path + " was not deleted within the waiting period");
      }
    }
  }
  else
  {
    log.info("Path already does not exist " + path);
  }
}

public static void delete(String path) throws IOException
{
  delete(HadoopUtils.createConfiguration(), path);
}

public static void delete(Configuration conf, String path) throws IOException
{
  delete(conf, new Path(path));
}

public static boolean exists(Configuration conf, Path path) throws IOException
{
  FileSystem fs = getFileSystem(conf, path);
  return fs.exists(path);
}

public static boolean exists(Configuration conf, String path) throws IOException
{
  return exists(conf, new Path(path));
}

public static boolean exists(Path path) throws IOException
{
  return exists(HadoopUtils.createConfiguration(), path);
}

public static boolean exists(String path) throws IOException
{
  return exists(HadoopUtils.createConfiguration(), new Path(path));
}

public static void get(Configuration conf, Path fromDir, Path toDir,
    String fileName) throws IOException
{
  FileSystem fs = getFileSystem(conf, fromDir);

  FileSystem fsTo = toDir.getFileSystem(conf);
  fsTo.mkdirs(toDir);

  fs.copyToLocalFile(new Path(fromDir, fileName), new Path(toDir, fileName));
}

public static void get(Path fromDir, Path toDir, String fileName)
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

public static FileSystem getFileSystem(Configuration conf) throws IOException
{
  return FileSystem.get(conf);
}

public static FileSystem getFileSystem(Configuration conf, Path path)
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
public static FileSystem getFileSystem(Path path) throws IOException
{
  return getFileSystem(HadoopUtils.createConfiguration(), path);
}

public static long getPathSize(Configuration conf, Path p) throws IOException
{
  long result = 0;

  FileSystem fs = getFileSystem(conf, p);

  FileStatus status = fs.getFileStatus(p);
  if (status.isDir())
  {
    FileStatus[] list = fs.listStatus(p);
    for (FileStatus l : list)
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
public static Path getTempDir(Configuration conf) throws IOException
{
  FileSystem fs = getFileSystem(conf);
  Path parent;
  parent = fs.getHomeDirectory();
  Path tmp = new Path(parent, "tmp");
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
public static void move(Configuration conf, Path inputPath, Path outputPath)
    throws IOException
{
  FileSystem fs = getFileSystem(conf, inputPath);

  if (!fs.rename(inputPath, outputPath))
  {
    throw new IOException("Cannot rename " + inputPath + " to " +
        outputPath);
  }
  if (!fs.exists(outputPath))
  {
    throw new IOException("Output path does not exist: " + outputPath);
  }
}

public static void
move(Configuration conf, String inputPath, String outputPath)
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
public static void moveChildDirectories(Configuration conf, Path srcParent,
    Path targetParent) throws IOException
{
  // We want to move each of the child directories of the output path
  // to the actual target directory (toPath). If any of the subdirs
  // already exists under the target path, we need to delete it first,
  // then perform the move.

  FileSystem fs = getFileSystem(conf, srcParent);
  if (!fs.exists(targetParent))
  {
    fs.mkdirs(targetParent);
  }
  FileStatus[] children = fs.listStatus(srcParent);
  for (FileStatus stat : children)
  {
    if (stat.isDir())
    {
      Path srcPath = stat.getPath();
      String name = srcPath.getName();
      Path target = new Path(targetParent, name);
      if (fs.exists(target))
      {
        fs.delete(target, true);
      }
      if (!fs.rename(srcPath, targetParent))
      {
        String msg = MessageFormat.format(
            "Error moving temporary file {0} to output path {1}.", srcPath.toString(), target
                .toString());
        throw new IOException(msg);
      }
    }
  }
}

public static InputStream open(Configuration conf, Path path) throws IOException
{
  FileSystem fs = getFileSystem(conf, path);

  if (fs.exists(path))
  {
    return fs.open(path, 131072);
  }

  throw new FileNotFoundException("File not found: " + path.toUri());
}

public static InputStream open(Path path) throws IOException
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
public static Path unqualifyPath(Path path)
{
  return new Path(path.toUri().getPath());
}

public static String unqualifyPath(String path)
{
  return new Path((new Path(path)).toUri().getPath()).toString();
}

public static Path resolveName(String input) throws IOException, URISyntaxException
{
  return resolveName(input, true);
}

public static Path resolveName(String input, boolean checkForExistance) throws IOException, URISyntaxException
{
  return resolveName(HadoopUtils.createConfiguration(), input, checkForExistance);
}

@SuppressWarnings("squid:S1166") // Exception caught and handled
@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "method only makes complete URI out of the name")
public static Path resolveName(Configuration conf, String input,
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

private static void cleanDirectory(FileSystem fs, Path dir) throws IOException
{
  Path temp = new Path(dir, "_temporary");
  if (fs.exists(temp))
  {
    fs.delete(temp, true);
  }

  Path success = new Path(dir, "_SUCCESS");
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
    log.error("Got AmazonServiceException while getting " + uri + ": " + e.getMessage(), e);
    throw new IOException(e);
  }
  catch(com.amazonaws.AmazonClientException e) {
    log.error("Got AmazonClientException while getting " + uri + ": " + e.getMessage(), e);
    throw new IOException(e);
  }

  try (InputStream objectData = object.getObjectContent())
  {
    long byteCount = org.apache.commons.io.IOUtils.copyLarge(objectData, fos);
    return byteCount;
  }
  finally
  {
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
            " in lieu of " + path;
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
        S3Cache localS3Cache = S3Utils.getS3Cache();
        S3Cache s3Cache = S3Utils.getS3Cache();
        File tmpBucketDir = new File(S3Utils.getCacheDir(), pathUri.getHost());
        File tmpFile = new File(tmpBucketDir, pathUri.getPath().substring(1));
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
              log.debug("Copied data from " + pathUri + " to " + tmpFile.getAbsolutePath() + ": " + fileSize);
              log.debug("SequenceFile " + pathUri + " size is " + fileSize);
            } finally {
              // Release the write lock on the original cache entry to allow locks
              // to readers waiting for the write to complete.
              log.debug("Downgrading from writeLock to readLock from thread " + Thread.currentThread().getId() + " on cacheEntry " +
                  cacheEntry);
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
        log.debug("Opening local reader to " + localPath);
        reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(localPath));
      } else {
        log.debug("Not caching locally: " + path);
        localPath = null;
        reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));
      }
    }
    return reader;
  }

  public void close() throws IOException
  {
    if (reader != null) {
      log.debug("Closing: " + path);
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
            " in lieu of " + path;
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
        S3Cache localS3Cache = S3Utils.getS3Cache();
        File cacheDir = S3Utils.getCacheDir();
        log.debug("cacheDir = " + cacheDir.getAbsolutePath());
        File tmpBucketDir = new File(S3Utils.getCacheDir(), pathUri.getHost());
        File tmpIndexFile = new File(tmpBucketDir, indexUri.getPath().substring(1));
        File tmpDataFile = new File(tmpBucketDir, dataUri.getPath().substring(1));
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
              try (FileOutputStream fosData = new FileOutputStream(tmpDataFile))
              {
                long dataSize = copyFileFromS3(s3Client, dataUri, tmpDataFile, fosData);
                log.debug("Copied data from " + dataUri + " to " +
                    tmpDataFile.getAbsolutePath() + ": " + dataSize);
              }
              long indexSize = copyFileFromS3(s3Client, indexUri, tmpIndexFile,
                      cacheEntry.getPrimaryFileOutputStream());
              log.debug("Copied data from " + indexUri + " to " +
                      tmpIndexFile.getAbsolutePath() + ": " + indexSize);
              log.debug("Index " + indexUri + " size is " + indexSize);
            } finally {
              // Release the write lock on the original cache entry to allow locks
              // to readers waiting for the write to complete.
              log.debug("Downgrading from writeLock to readLock from thread " +
                      Thread.currentThread().getId() + " on cacheEntry " + cacheEntry);
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
        log.debug("Opening local reader to " + localPath);
        reader = new MapFile.Reader(localPath, conf);
      } else {
        log.debug("Not caching locally: " + path);
        localPath = null;
        reader = new MapFile.Reader(path, conf);
      }
    }
    return reader;
  }

  public void close() throws IOException
  {
    if (reader != null) {
      log.debug("Closing: " + path);
      reader.close();
      reader = null;
      // Inform the file cache that we are done with the file
      if (cacheEntry != null) {
        cacheEntry.releaseReadLock();
      }
    }
  }
}
}
