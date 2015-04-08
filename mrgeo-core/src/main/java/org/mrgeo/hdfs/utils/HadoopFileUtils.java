/*
 * Copyright 2009-2014 DigitalGlobe, Inc.
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
 */

package org.mrgeo.hdfs.utils;

import com.sun.media.jai.codec.SeekableStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.mrgeo.utils.HadoopUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.MessageFormat;

public class HadoopFileUtils
{
  public static class CompressedSeekableStream extends SeekableStream
  {
    final private InputStream input;
    final private SeekableStream seekable;

    public CompressedSeekableStream(final InputStream input)
    {
      this.input = input;
      this.seekable = SeekableStream.wrapInputStream(this.input, true);
    }

    @Override
    public int available() throws IOException
    {
      return seekable.available();
    }

    @Override
    public boolean canSeekBackwards()
    {
      return seekable.canSeekBackwards();
    }

    @Override
    public void close() throws IOException
    {
      super.close();
      seekable.close();
      input.close();
    }

    @Override
    public long getFilePointer() throws IOException
    {
      return seekable.getFilePointer();
    }

    @Override
    public synchronized void mark(final int readLimit)
    {
      seekable.mark(readLimit);
    }

    @Override
    public boolean markSupported()
    {
      return seekable.markSupported();
    }

    @Override
    public int read() throws IOException
    {
      return seekable.read();
    }

    @Override
    public int read(final byte[] b) throws IOException
    {
      return seekable.read(b);
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException
    {
      return seekable.read(b, off, len);
    }

    @Override
    public synchronized void reset() throws IOException
    {
      seekable.reset();
    }

    @Override
    public void seek(final long pos) throws IOException
    {
      seekable.seek(pos);
    }

    @Override
    public long skip(final long n) throws IOException
    {
      return seekable.skip(n);
    }

    @Override
    public int skipBytes(final int n) throws IOException
    {
      return seekable.skipBytes(n);
    }

    @Override
    protected void finalize() throws Throwable
    {
      super.finalize();
    }
  }

  private static final Logger log = LoggerFactory.getLogger(HadoopFileUtils.class);

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

  public static void cleanDirectory(final Configuration conf, final String dir, final boolean recursive) throws IOException
  {
    cleanDirectory(new Path(dir), recursive);
  }

  public static void cleanDirectory(final Path dir, final boolean recursive) throws IOException
  {
    cleanDirectory(HadoopUtils.createConfiguration(), dir, recursive);
  }

  public static void cleanDirectory(final Configuration conf, final Path dir, final boolean recursive) throws IOException
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

  public static void copyToHdfs(final String fromDir, final Path toDir, final String fileName,
    final boolean overwrite) throws IOException
    {
    if (overwrite)
    {
      delete(new Path(toDir, fileName));
    }
    copyToHdfs(fromDir, toDir, fileName);
    }

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
    return createUniqueTmp();
  }

  /**
   * Creates a unique temp directory and returns the path.
   *
   * @return
   * @throws IOException
   */
  public static Path createUniqueTmp() throws IOException
  {
    // final String base = baseName.replace(" ", "_");

    // create a corresponding tmp directory for the job
    final Path tmp = createUniqueTmpPath(); // new Path(createUniqueTmpPath(), base);

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
    return new Path(getTempDir(), HadoopUtils.createRandomString(40));
  }

  public static void create(final Path path) throws IOException
  {
    create(HadoopUtils.createConfiguration(), path);
  }

  public static void create(final Configuration conf, final Path path) throws IOException
  {
    final FileSystem fs = HadoopFileUtils.getFileSystem(conf, path);
    if (!fs.exists(path))
    {
      fs.mkdirs(path);
    }
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

  public static void delete(final Configuration conf, final Path path) throws IOException
  {
    final FileSystem fs = getFileSystem(conf, path);
    if (fs.exists(path))
    {
      if (fs.delete(path, true) == false)
      {
        throw new IOException("Error deleting directory " + path.toString());
      }
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
    return FileSystem.get(path.toUri(), conf);
    //    FileSystem fs = null;
    //    final String pstr = path.toString();
    //    // if this is a hadoop archive
    //    if (pstr.startsWith("har://"))
    //    {
    //      @SuppressWarnings("resource")
    //      final HarFileSystem hfs = new HarFileSystem();
    //      // I get a null pointer exception if I don't include this initialize
    //      // step -- odd.
    //      hfs.initialize(path.toUri(), conf);
    //      fs = hfs;
    //    }
    //    else if (pstr.endsWith(".har"))
    //    {
    //      fs = path.getFileSystem(conf);
    //      final Path p = new Path("har://" + path.makeQualified(fs).toString().replace("://", "-") +
    //        "/");
    //      @SuppressWarnings("resource")
    //      final HarFileSystem hfs = new HarFileSystem();
    //      // I get a null pointer exception if I don't include this initialize step -- odd.
    //      hfs.initialize(p.toUri(), conf);
    //      fs = hfs;
    //    }
    //    else
    //    {
    //      // NOTE: the bowels of path.getFileSystem() can throw and catch an IOException
    //      // that is printed in log level DEBUG. This just means the file does not
    //      // exist and getFileSystem() will return the DFS filesystem.
    //      fs = path.getFileSystem(conf);
    //    }
    //    return fs;
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
    if (fs.exists(tmp) == false)
    {
      fs.mkdirs(tmp);
    }
    return tmp;
  }

  /**
   * Moves a file in DFS
   *
   * @param inputPath
   *          input path
   * @param outputPath
   *          output path
   * @throws IOException
   *           if unable to move file
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
      final InputStream stream = fs.open(path, 131072); // give open a 128K buffer

      // see if were compressed
      final CompressionCodecFactory factory = new CompressionCodecFactory(conf);
      final CompressionCodec codec = factory.getCodec(path);

      if (codec != null)
      {
        return new CompressedSeekableStream(codec.createInputStream(stream));
      }

      return stream;
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
   * @param path
   *          DFS path
   * @return e.g. for input: hdfs://localhost:9000/mrgeo/images/test-1, returns /mrgeo/images/test-1
   */
  public static Path unqualifyPath(final Path path)
  {
    return new Path(path.toUri().getPath().toString());
  }

  public static String unqualifyPath(final String path)
  {
    return new Path((new Path(path)).toUri().getPath().toString()).toString();
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

  public static Path resolveName(final String input) throws IOException, URISyntaxException
  {
    return resolveName(input, true);
  }
  
  public static Path resolveName(final String input, boolean checkForExistance) throws IOException, URISyntaxException
  {
    return resolveName(HadoopUtils.createConfiguration(), input, checkForExistance);
  }

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
      catch (URISyntaxException e)
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

}
