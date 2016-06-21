package org.mrgeo.hdfs.utils;

import org.mrgeo.hdfs.tile.FileSplit;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by ericwood on 6/17/16.
 */
public class MrGeoFileSplitBuilder {
    private FileSplit fileSplit;
    private List<FileSplit.FileSplitInfo> splits = new ArrayList<>();

    public MrGeoFileSplitBuilder() {
        this.fileSplit = mock(FileSplit.class);
    }

    public MrGeoFileSplitBuilder split(FileSplit.FileSplitInfo split) {
        this.splits.add(split);

        return this;
    }

    public FileSplit build() {
        FileSplit.FileSplitInfo[] splitsArray = new FileSplit.FileSplitInfo[splits.size()];
        when(fileSplit.getSplits()).thenReturn(splits.toArray(splitsArray));
        return fileSplit;
    }
}
