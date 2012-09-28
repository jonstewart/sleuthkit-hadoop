/*
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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.sleuthkit.hadoop.core.SKJobFactory;

public class JsonImport extends Configured implements Tool {
  public int run(String[] args) throws ClassNotFoundException, InterruptedException, IOException {
    if (args.length != 3) {
      System.err.println("Usage: JsonImport <in> <image_hash> <friendly_name>");
      return 1;
    }

    final String jsonPath = args[0];
    final String imageHash = args[1];
    final String friendlyName = args[2];

    final Configuration conf = getConf();
    conf.set(HBaseTables.ENTRIES_TBL_VAR, HBaseTables.ENTRIES_TBL);

    final Job job = SKJobFactory.createJobFromConf(imageHash, friendlyName, "JsonImport", conf);
    job.setJarByClass(JsonImport.class);
    job.setMapperClass(FsEntryMapLoader.class);
    job.setNumReduceTasks(0);
    job.setOutputFormatClass(FsEntryHBaseOutputFormat.class);
    FileInputFormat.addInputPath(job, new Path(jsonPath));

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    System.exit(
      ToolRunner.run(HBaseConfiguration.create(), new JsonImport(), args)
    );
  }
}
