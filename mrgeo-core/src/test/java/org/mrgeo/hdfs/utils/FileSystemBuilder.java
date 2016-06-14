package org.mrgeo.hdfs.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import static org.mockito.Mockito.mock;

/**
 * Created by ericwood on 6/10/16.
 */
public class FileSystemBuilder {

    private final FileSystem fileSystem;

    public FileSystemBuilder() {
        this.fileSystem = mock(FileSystem.class);
    }

    public FileSystem build() {
        return fileSystem;
    }
}
