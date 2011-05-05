/*
src/com/lightboxtechnologies/spectrum/JsonImport.java

Created by Jon Stewart on 2010-03-23.
Copyright (c) 2010 Lightbox Technologies, Inc. All rights reserved.
*/

package com.lightboxtechnologies.spectrum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.util.GenericOptionsParser;

import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ExtractData {
  protected ExtractData() {}

  public static final Log LOG = LogFactory.getLog(ExtractData.class.getName());

  public static void main(String[] args) throws Exception {
    final Configuration conf = new Configuration();
    final String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    if (otherArgs.length != 4) {
      System.err.println("Usage: ExtractData <table> <extents_file> <evidence file> <store_path>");
      System.exit(2);
    }

    final Job job = new Job(conf, "ExtractData");
    job.setJarByClass(ExtractData.class);
    job.setMapperClass(ExtractMapper.class);
    job.setNumReduceTasks(1);
//    job.setReducer
    
    job.setInputFormatClass(RawFileInputFormat.class);
    RawFileInputFormat.addInputPath(job, new Path(otherArgs[2]));
    
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    conf.setInt("mapred.job.reuse.jvm.num.tasks", -1);
    conf.set(FsEntryHBaseOutputFormat.ENTRY_TABLE, otherArgs[0]);
    conf.set("com.lbt.storepath", otherArgs[3]);
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
    
    final URI extents = new Path(otherArgs[1]).toUri();
    LOG.info("extents file is " + extents);
    
    DistributedCache.addCacheFile(new Path(otherArgs[1]).toUri(), conf);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
