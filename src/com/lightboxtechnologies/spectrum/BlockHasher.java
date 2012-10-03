/*
src/com/lightboxtechnologies/spectrum/JsonImport.java

Copyright 2011, Lightbox Technologies, Inc

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.lightboxtechnologies.spectrum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MD5Hash;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.sleuthkit.hadoop.core.SKJobFactory;

public class BlockHasher extends Configured implements Tool {
  public static final Log LOG = LogFactory.getLog(BlockHasher.class.getName());


  public int run(String[] args) throws Exception {
    if (args.length != 3) {
      System.err.println("Usage: BlockHasher <imageID> <image> <output>");
      return 2;
    }

    final String imageID = args[0];
    final String image = args[1];
    final String output = args[2];

    Configuration conf = getConf();

    final Job job = SKJobFactory.createJobFromConf(
      imageID, image, "BlockHasher", conf
    );
    job.setJarByClass(BlockHasher.class);
    job.setMapperClass(BlockHashMapper.class);
    // job.setReducerClass(Reducer.class);
    job.setNumReduceTasks(0);

    // job ctor copies the Configuration we pass it, get the real one
    conf = job.getConfiguration();

    conf.setLong("timestamp", System.currentTimeMillis());

    job.setInputFormatClass(RawFileInputFormat.class);
    RawFileInputFormat.addInputPath(job, new Path(image));

    job.setOutputFormatClass(TextOutputFormat.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(MD5Hash.class);
    FileOutputFormat.setOutputPath(job, new Path(output));

    conf.setInt("mapred.job.reuse.jvm.num.tasks", -1);
    
    return job.waitForCompletion(true) ? 0: 1;
  }

  public static void main(String[] args) throws Exception {
    System.exit(
      ToolRunner.run(HBaseConfiguration.create(), new BlockHasher(), args)
    );
  }
}
