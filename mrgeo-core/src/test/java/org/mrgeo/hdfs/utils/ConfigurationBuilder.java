package org.mrgeo.hdfs.utils;

import org.apache.hadoop.conf.Configuration;

import static org.mockito.Mockito.mock;

/**
 * Created by ericwood on 6/10/16.
 */
public class ConfigurationBuilder {

    private final Configuration configuration;

    public ConfigurationBuilder() {
        this.configuration = mock(Configuration.class);
    }

    public Configuration build() {
        return configuration;
    }
}
