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

package org.mrgeo.utils;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.data.image.MrsPyramidMetadataReader;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.image.MrsPyramidMetadata;
import org.mrgeo.utils.tms.Bounds;
import org.mrgeo.utils.tms.TMSUtils;
import org.mrgeo.utils.tms.Tile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class S3Utils
{
  private static final Logger log = LoggerFactory.getLogger(S3Utils.class);
  private static S3Cache s3Cache;
  private static File cacheDir;
  private static boolean useFileLocker;

  public interface S3CacheEntry {
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
  private static class S3MemoryCacheEntry implements S3CacheEntry {
    private Path localPath;
    private File primaryFile;
    private File secondaryFile;
    private ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private FileOutputStream fos;

    S3MemoryCacheEntry(Path localPath, File primaryFile) {
      this.localPath = localPath;
      this.primaryFile = primaryFile;
    }

    S3MemoryCacheEntry(Path localPath, File primaryFile, File secondaryFile) {
      this.localPath = localPath;
      this.primaryFile = primaryFile;
      this.secondaryFile = secondaryFile;
    }

    @Override
    public FileOutputStream getPrimaryFileOutputStream() throws IOException {
      if (fos == null) {
        org.apache.commons.io.FileUtils.forceMkdir(primaryFile.getParentFile());
        fos = new FileOutputStream(primaryFile);
      }
      return fos;
    }

    @Override
    public Path getLocalPath() {
      return localPath;
    }

    @Override
    public void cleanup() throws IOException {
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
      } finally {
        releaseWriteLock();
      }
    }

    @Override
    public void readLock() throws IOException {
      log.debug("Lock readLock lock from thread " + Thread.currentThread().getId() +
              " on cacheEntry " + toString());
      rwLock.readLock().lock();
    }

    @Override
    public void writeLock() throws IOException {
      log.debug("Lock writeLock lock from thread " + Thread.currentThread().getId() +
              " on cacheEntry " + toString());
      rwLock.writeLock().lock();
    }

    @Override
    public boolean tryWriteLock() throws IOException {
      log.debug("Lock writeLock lock from thread " + Thread.currentThread().getId() +
              " on cacheEntry " + toString());
      return rwLock.writeLock().tryLock();
    }

    @Override
    public void releaseReadLock() {
      log.debug("Lock readLock unlock from thread " + Thread.currentThread().getId() +
              " on cacheEntry " + toString());
      rwLock.readLock().unlock();
    }

    @Override
    public void releaseWriteLock() throws IOException {
      log.debug("Lock writeLock unlock from thread " + Thread.currentThread().getId() +
              " on cacheEntry " + toString());
      rwLock.writeLock().unlock();
      // The lock is memory-based, but we want to make sure to close the file output stream
      // when we release the lock so we don't retain open files.
      if (fos != null) {
        fos.close();
        fos = null;
      }
    }
  }

  private static class S3FileCacheEntry implements S3CacheEntry {
    private Path localPath;
    private File primaryFile;
    private File secondaryFile;
    private FileLock readFileLock;
    private FileLock writeFileLock;
    private FileOutputStream fos;
    private FileInputStream fis;
    private FileChannel readChannel;
    private FileChannel writeChannel;

    S3FileCacheEntry(Path localPath, File localFile) {
      this.localPath = localPath;
      primaryFile = localFile;
    }

    S3FileCacheEntry(Path localPath, File primaryFile, File secondaryFile) {
      this.localPath = localPath;
      this.primaryFile = primaryFile;
      this.secondaryFile = secondaryFile;
    }

    public FileOutputStream getPrimaryFileOutputStream() throws IOException {
      return fos;
    }

    @Override
    public void readLock() throws IOException {
      initReadChannel();
      log.debug("readLock from thread " + Thread.currentThread().getId() + " on entry " +
              primaryFile.getAbsolutePath());
      readFileLock = readChannel.lock(0, Long.MAX_VALUE, true);
      log.debug("  readLock from thread " + Thread.currentThread().getId() + " done ");
      // Some operating systems do not support shared locks, so report that here. But
      // keep the lock active - the system will just run slower.
      if (!readFileLock.isShared()) {
        log.warn("Unable to create a shared lock on " + getLocalPath());
      }
    }

    @Override
    public void writeLock() throws IOException {
      initWriteChannel();
      log.debug("writeLock from thread " + Thread.currentThread().getId() + " on entry " +
              primaryFile.getAbsolutePath());
      writeFileLock = writeChannel.lock(0, Long.MAX_VALUE, false);
      log.debug("  done writeLock from thread " + Thread.currentThread().getId() + " on entry " +
              primaryFile.getAbsolutePath());
    }

    @Override
    public boolean tryWriteLock() throws IOException {
      initWriteChannel();
      log.debug("tryWriteLock from thread " + Thread.currentThread().getId() + " on entry " +
              primaryFile.getAbsolutePath());
      writeFileLock = writeChannel.tryLock(0, Long.MAX_VALUE, false);
      log.debug("  done tryWriteLock from thread " + Thread.currentThread().getId() + " on entry " +
              primaryFile.getAbsolutePath());
      return (writeFileLock != null);
    }

    @Override
    public void releaseReadLock() throws IOException {
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
    public void releaseWriteLock() throws IOException {
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
    public Path getLocalPath() {
      return localPath;
    }

    @Override
    public void cleanup() throws IOException {
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
      } finally {
        releaseWriteLock();
        releaseReadLock();
      }
    }

    private void initWriteChannel() throws IOException {
      if (writeChannel == null) {
        org.apache.commons.io.FileUtils.forceMkdir(primaryFile.getParentFile());
        fos = new FileOutputStream(primaryFile);
        writeChannel = fos.getChannel();
      }
    }

    private void initReadChannel() throws IOException {
      if (readChannel == null) {
        fis = new FileInputStream(primaryFile);
        readChannel = fis.getChannel();
      }
    }

    @SuppressFBWarnings(value = "OS_OPEN_STREAM", justification = "stream is closed when lock is released")
    public static void run(String[] args) {
      Path lockPath = new Path(args[0]);
      File file1 = new File(args[1]);
      File file2 = new File(args[2]);

      Scanner scanner = new Scanner(System.in);
      System.out.print("Command (twl, wl, rwl, rl, rrl, rwlrl, wf, quit): ");
      String line = scanner.nextLine();
      S3FileCacheEntry entry = null;
      while (!line.equalsIgnoreCase("quit")) {
        if (entry == null) {
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

  public interface S3Cache {
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

  private static class CacheCleanupHook extends Thread {
    private File cleanupDir;

    CacheCleanupHook(File cleanupDir) {
      this.cleanupDir = cleanupDir;
    }

    @Override
    public void run() {
      try {
        org.apache.commons.io.FileUtils.cleanDirectory(cleanupDir);
        log.debug("In shutdown, deleting " + cleanupDir.getAbsolutePath());
        org.apache.commons.io.FileUtils.deleteDirectory(cleanupDir);
      } catch (IOException e) {
        log.debug("Unable to clean MrGeo S3 cache directory " + e.getMessage());
      }
    }
  }

  private static class CacheCleanupOnElementRemoval implements RemovalListener<String, S3CacheEntry> {
    @Override
    public void onRemoval(RemovalNotification<String, S3CacheEntry> notification) {
      log.debug("S3 cache removal key: " + notification.getKey() + " with cause " + notification.getCause() + " from thread " + Thread.currentThread().getId());
      log.debug("S3 cache removal value: " + notification.getValue() + " from thread " + Thread.currentThread().getId());
      try {
        notification.getValue().cleanup();
      } catch (IOException e) {
        log.error("Got exception while cleaning up a cache entry for " + notification.getKey(), e);
      }
    }
  }

  private static class S3MemoryCache implements S3Cache {
    private Cache<String, S3CacheEntry> s3FileCache;

    @Override
    @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "File() - name is gotten from mrgeo.conf")
    public void init() throws IOException {
      if (cacheDir == null) {
        // For the memory-based cache approach, the locally cached S3 files
        // must be cleaned up when the process exits. Also, the local cache
        // dir is in a temp subdir so it does not conflict with other running
        // processes that use this same cache approach (e.g. multiple instances
        // of MrGeo web services running on a single machine)
        String strTmpDir = MrGeoProperties.getInstance().getProperty("s3.cache.dir", "/tmp/mrgeo");
        org.apache.commons.io.FileUtils.forceMkdir(new File(strTmpDir));
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
    public S3CacheEntry getEntry(String key, Path localPath, File primaryFile,
                                 File secondaryFile) {
      return s3FileCache.getIfPresent(key);
    }

    @Override
    public void addEntry(String key, S3CacheEntry entry) {
      s3FileCache.put(key, entry);
    }

    @Override
    public S3CacheEntry createEntry(Path localPath, File primaryFile) {
      return new S3MemoryCacheEntry(localPath, primaryFile);
    }

    @Override
    public S3CacheEntry createEntry(Path localPath, File primaryFile, File secondaryFile) {
      return new S3MemoryCacheEntry(localPath, primaryFile, secondaryFile);
    }
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "File() - name is configured by an admin")
  private static class S3FileCache implements S3Cache {
    @Override
    public void init() throws IOException {
      if (cacheDir == null) {
        // When using a file-based S3 local cache, all processes use the same
        // file-based cache and file locking to coordinate access. There is no need
        // the delete the cache directory when the process exits because it will
        // automatically re-use the cache contents the next time it starts up.
        String strTmpDir = MrGeoProperties.getInstance().getProperty("s3.cache.dir", "/tmp/mrgeo");
        org.apache.commons.io.FileUtils.forceMkdir(new File(strTmpDir));
        cacheDir = new File(strTmpDir);
      }
    }

    @Override
    public S3CacheEntry getEntry(String key, Path localPath, File primaryFile, File secondaryFile) {
      if (primaryFile.exists()) {
        log.debug("Found existing entry for " + key);
        return new S3FileCacheEntry(localPath, primaryFile, secondaryFile);
      }
      log.debug("Did not find existing entry for " + key);
      return null;
    }

    @Override
    public void addEntry(String key, S3CacheEntry entry) {
      // Nothing to do
    }

    @Override
    public S3CacheEntry createEntry(Path localPath, File primaryFile) {
      return new S3FileCacheEntry(localPath, primaryFile);
    }

    @Override
    public S3CacheEntry createEntry(Path localPath, File primaryFile, File secondaryFile) {
      return new S3FileCacheEntry(localPath, primaryFile, secondaryFile);
    }
  }

  public static synchronized S3Cache getS3Cache() throws IOException {
    if (s3Cache == null) {
      String strLocker = MrGeoProperties.getInstance().getProperty("s3.cache.locker", "file");
      useFileLocker = strLocker.equalsIgnoreCase("file");
      s3Cache = (useFileLocker) ? new S3FileCache() : new S3MemoryCache();
      s3Cache.init();
    }
    return s3Cache;
  }

  public static File getCacheDir() throws IOException
  {
    // Initialize the cacheDir if needed
    getS3Cache();
    return cacheDir;
  }


  private interface MrGeoImageCallback
  {
    void foundImage(java.nio.file.Path imagePath, long imageSize, FileTime lastAccess);
  }

  /**
   * MrGeo images are split into partitions. Each partition is a Hadoop MapFile which
   * is a directory containing two files named "index" and "data". When performing
   * cache cleaning, we want to make sure that we remove both files and the parent
   * directory.
   */
  private static class MrGeoImageCacheFileVisitor extends SimpleFileVisitor<java.nio.file.Path>
  {
    private BasicFileAttributes indexAttributes;
    private BasicFileAttributes dataAttributes;
    private long partitionSize;
    private FileTime lastAccessTime = FileTime.from(Instant.now());
    private MrGeoImageCallback callback;

    public MrGeoImageCacheFileVisitor(MrGeoImageCallback callback)
    {
      this.callback = callback;
    }

    @Override
    public FileVisitResult preVisitDirectory(java.nio.file.Path dir, BasicFileAttributes attrs) throws IOException
    {
      partitionSize = 0L;
      lastAccessTime = FileTime.from(Instant.now());
      return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFile(java.nio.file.Path file, BasicFileAttributes attrs) throws IOException
    {
      if (file != null) {
        java.nio.file.Path nameOnly = file.getFileName();
        if (nameOnly != null) {
          String fileName = nameOnly.toString();
          if (fileName.equals("index")) {
            indexAttributes = attrs;
            if (attrs.lastAccessTime().compareTo(lastAccessTime) < 0) {
              lastAccessTime = attrs.lastAccessTime();
            }
          } else if (fileName.equals("data")) {
            dataAttributes = attrs;
            if (attrs.lastAccessTime().compareTo(lastAccessTime) < 0) {
              lastAccessTime = attrs.lastAccessTime();
            }
          }
        }
      }
      return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult postVisitDirectory(java.nio.file.Path dir, IOException exc) throws IOException
    {
      if (indexAttributes != null && dataAttributes != null) {
        partitionSize = indexAttributes.size() + dataAttributes.size();
        callback.foundImage(dir, partitionSize, lastAccessTime);
        indexAttributes = null;
        dataAttributes = null;
        lastAccessTime = FileTime.from(Instant.now());
        partitionSize = 0L;
      }
      return FileVisitResult.CONTINUE;
    }
  }

  private static class MrGeoImageEntry
  {
    private java.nio.file.Path imagePath;
    private long imageSize;
    private FileTime lastAccess;

    public MrGeoImageEntry(java.nio.file.Path imagePath, long imageSize, FileTime lastAccess)
    {
      this.imagePath = imagePath;
      this.imageSize = imageSize;
      this.lastAccess = lastAccess;
    }
  }

  private static class MrGeoImageCollection implements MrGeoImageCallback
  {
    private List<MrGeoImageEntry> images;

    public MrGeoImageCollection(List<MrGeoImageEntry> images)
    {
      this.images = images;
    }

    @Override
    public void foundImage(java.nio.file.Path imagePath, long imageSize, FileTime lastAccess) {
      images.add(new MrGeoImageEntry(imagePath, imageSize, lastAccess));
    }
  }

  public static void cleanCache(long maxCacheSize, int age, int ageField, Bounds bbox,
                                int minZoom, int maxZoom, boolean dryrun,
                                int defaultTileSize,
                                Configuration conf,
                                ProviderProperties providerProperties) throws IOException
  {
    // The following call figures out the cacheDir
    File useCacheDir = getCacheDir();
    java.nio.file.Path startPath = Paths.get(useCacheDir.getAbsolutePath());
    // Scan all entries and sort by last access time.
    List<MrGeoImageEntry> imageList = new ArrayList<>(500);
    MrGeoImageCollection images = new MrGeoImageCollection(imageList);
    Files.walkFileTree(startPath, new MrGeoImageCacheFileVisitor(images));
    // Sort in ascending order by lastAccess then descending order by size.
    imageList.sort(new Comparator<MrGeoImageEntry>() {
      @Override
      public int compare(MrGeoImageEntry image1, MrGeoImageEntry image2) {
        int result = image1.lastAccess.compareTo(image2.lastAccess);
        if (result == 0) {
          if (image1.imageSize > image2.imageSize) {
            result = -1;
          }
          else if (image1.imageSize < image2.imageSize) {
            result = 1;
          }
        }
        return result;
      }
    });
    if (imageList.size() > 0) {
      long totalSize = 0L;
      // Compute the total size of all images.
      for (MrGeoImageEntry entry : imageList) {
        totalSize += entry.imageSize;
      }
      if (totalSize > 0) {
        long newSize = totalSize;
        java.util.Calendar c = GregorianCalendar.getInstance();
        if (age > 0) {
          c.add(ageField, -age);
        }
        Iterator<MrGeoImageEntry> iter = imageList.iterator();
        long deleteCount = 0L;
        Map<String, Integer> tileSizes = new HashMap<>();
        while (iter.hasNext()) {
          MrGeoImageEntry entry = iter.next();
          boolean deleteFile = false;
          // Get the zoom level of the current file
          int zoomLevel = -1;
          java.nio.file.Path zoomPath = entry.imagePath.getParent();
          if (zoomPath != null) {
            java.nio.file.Path zoomName = zoomPath.getFileName();
            if (zoomName != null) {
              try {
                zoomLevel = Integer.parseInt(zoomName.toString());
              }
              catch(NumberFormatException nfe) {
                log.error("Unable to parse zoom level from " + zoomName);
              }
            }
          }
          try {
//              System.out.println(entry.lastAccess.toString() + " " + entry.imagePath.toString() +
//                      " with size " + entry.imageSize +
//                      ". New cache size is " + newSize);
//              if (newSize > maxCacheSize) {
//                System.out.println("  DELETE");
//                newSize -= entry.imageSize;
//              }
            if (maxCacheSize > 0) {
              if (newSize > maxCacheSize) {
                deleteFile = true;
                newSize -= entry.imageSize;
              }
              else {
                // We are below the size threshold, so bail out
                break;
              }
            }
            else if (minZoom > 0) {
              deleteFile = (minZoom <= zoomLevel && zoomLevel <= maxZoom);
            }
            else if (age > 0) {
              deleteFile = (entry.lastAccess.toMillis() < c.getTimeInMillis());
            }
            else if (bbox != null) {
              // Have to read the tiles from the partition and see if any of them
              // intersect the specified bbox.
              Path indexPath = new Path("file://" + entry.imagePath, "index");
              int tilesize = defaultTileSize;
              try {
                // Get the image name
                java.nio.file.Path imagePath = zoomPath.getParent();
                java.nio.file.Path cachePath = Paths.get(cacheDir.toString());
                java.nio.file.Path relativeImagePath = cachePath.relativize(imagePath);
                if (relativeImagePath != null) {
                  String strRelativeImagePath = relativeImagePath.toString();
                  if (tileSizes.containsKey(strRelativeImagePath)) {
                    tilesize = tileSizes.get(strRelativeImagePath);
                  }
                  else {
                    MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(
                            "s3://" + strRelativeImagePath,
                            AccessMode.READ, providerProperties);
                    MrsPyramidMetadataReader metadataReader = dp.getMetadataReader();
                    if (metadataReader != null) {
                      MrsPyramidMetadata metadata = metadataReader.read();
                      tileSizes.put(strRelativeImagePath, tilesize);
                      tilesize = metadata.getTilesize();
                    }
                  }
                }
                SequenceFile.Reader indexReader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(indexPath));
                TileIdWritable key = (TileIdWritable)indexReader.getKeyClass().newInstance();
                boolean hasKey = indexReader.next(key);
                while (hasKey && !deleteFile) {
                  Tile tile = TMSUtils.tileid(key.get(), zoomLevel);
                  Bounds tb = TMSUtils.tileBounds(tile.tx, tile.ty, zoomLevel, tilesize);
                  deleteFile = bbox.intersects(tb);
                  hasKey = indexReader.next(key);
                }
              } catch (IllegalAccessException | InstantiationException e) {
                log.error("Unable to check the bounds of " + indexPath, e);
              }
            }

            if (deleteFile) {
              deleteCount++;
              if (dryrun) {
                System.out.println("Would delete S3 cached file at " + entry.imagePath);
              } else {
                org.mrgeo.utils.FileUtils.deleteDir(entry.imagePath.toFile(), true);
                System.out.println("Deleted S3 cached file at " + entry.imagePath);
              }
            }
          }
          catch(IOException e) {
            log.warn("Unable to clean cached S3 image at " + entry.imagePath, e);
          }
        }
        System.out.println(((dryrun) ? "Would have deleted " : "Deleted ") + deleteCount + " cache entries");
      }
    }
  }

  /**
   * This is only meant for testing S3 caching from the command line.
   * @param args
   */
  public static void main(String[] args) {
    S3FileCacheEntry.run(args);
  }
}
