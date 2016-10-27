package org.mrgeo.hdfs.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by ericwood on 6/10/16.
 */
public class TaskAttemptContextBuilder {

    private TaskAttemptContext taskAttemptContext;
    private Configuration configuration;
    private Class outputKeyClass;
    private Class outputValueClass;

    public TaskAttemptContextBuilder() {
        taskAttemptContext = mock(org.apache.hadoop.mapreduce.TaskAttemptContext.class);
    }

    public TaskAttemptContextBuilder configuration(Configuration configuration) {
        this.configuration = configuration;

        return this;
    }

    public TaskAttemptContextBuilder outputKeyClass(Class<? extends WritableComparable> outputKeyClass) {
        this.outputKeyClass = outputKeyClass;

        return this;
    }

    public TaskAttemptContextBuilder outputValueClass(Class<? extends Writable> outputValueClass) {
        this.outputValueClass = outputValueClass;

        return this;
    }

    public TaskAttemptContext build() {
        when(taskAttemptContext.getConfiguration()).thenReturn(configuration);
        when(taskAttemptContext.getOutputKeyClass()).thenReturn(outputKeyClass);
        when(taskAttemptContext.getOutputValueClass()).thenReturn(outputValueClass);

        return taskAttemptContext;
    }
}
