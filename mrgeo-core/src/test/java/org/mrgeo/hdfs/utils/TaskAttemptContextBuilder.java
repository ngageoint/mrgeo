package org.mrgeo.hdfs.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import static org.mockito.Mockito.*;

/**
 * Created by ericwood on 6/10/16.
 */
public class TaskAttemptContextBuilder {

    private TaskAttemptContext taskAttemptContext;
    private Configuration configuration;

    public TaskAttemptContextBuilder() {
        taskAttemptContext = mock(org.apache.hadoop.mapreduce.TaskAttemptContext.class);
    }

    public TaskAttemptContextBuilder configuration(Configuration configuration) {
        this.configuration = configuration;

        return this;
    }

    public TaskAttemptContext build() {
        when(taskAttemptContext.getConfiguration()).thenReturn(configuration);

        return taskAttemptContext;
    }
}
