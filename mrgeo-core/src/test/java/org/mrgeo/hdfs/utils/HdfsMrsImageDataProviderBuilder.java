package org.mrgeo.hdfs.utils;

import org.apache.hadoop.fs.Path;
import org.mrgeo.data.image.MrsPyramidMetadataReader;
import org.mrgeo.hdfs.image.HdfsMrsImageDataProvider;
import org.mrgeo.image.MrsPyramidMetadata;

import java.io.IOException;

import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by ericwood on 6/16/16.
 */
public class HdfsMrsImageDataProviderBuilder {
    private HdfsMrsImageDataProvider imageDataProvider;
//    private MrsPyramidMetadataReader metadataReader;
    private MrsPyramidMetadata pyramidMetadata;
    private Path resourcePath;

    public HdfsMrsImageDataProviderBuilder() {
        // RETURNS_DEEP_STUBS allows us to avoid mocking intermediate objects (e.g. MetadataReader)
        this.imageDataProvider = mock(HdfsMrsImageDataProvider.class, RETURNS_DEEP_STUBS);
//        this.metadataReader = mock(MrsPyramidMetadataReader.class);
    }

    public HdfsMrsImageDataProviderBuilder pyramidMetadata(MrsPyramidMetadata pyramidMetadata) {
        this.pyramidMetadata = pyramidMetadata;

        return this;
    }

    public HdfsMrsImageDataProviderBuilder resourcePath(Path resourcePath) {
        this.resourcePath = resourcePath;

        return this;
    }

    public HdfsMrsImageDataProvider build() throws IOException {
//        when(metadataReader.read()).thenReturn(pyramidMetadata);
//        when(imageDataProvider.getMetadataReader()).thenReturn(metadataReader);
        when(imageDataProvider.getMetadataReader().read()).thenReturn(pyramidMetadata);
        when(imageDataProvider.getResourcePath(anyBoolean())).thenReturn(resourcePath);

        return imageDataProvider;
    }
}
