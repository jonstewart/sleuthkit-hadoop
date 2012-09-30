package com.lightboxtechnologies.spectrum;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

import org.sleuthkit.hadoop.core.SKMapper;

public class ImportMetadataMapper
                   extends SKMapper<Object, Text, LongWritable, JsonWritable> {
  @Override
  protected void map(Object key, Text value, Context context)
                                     throws IOException, InterruptedException { 

  }
}
