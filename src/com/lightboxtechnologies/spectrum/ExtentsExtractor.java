/*
src/com/lightboxtechnologies/spectrum/ExtentsExtractor.java

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

import org.apache.commons.codec.DecoderException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.sleuthkit.hadoop.core.SKJobFactory;

public class ExtentsExtractor extends Configured implements Tool {
  public int run(String[] args) throws ClassNotFoundException, DecoderException, InterruptedException, IOException {
    if (args.length != 3) {
      System.err.println("Usage: ExtentsExtractor <imageID> <friendlyName> <sequenceFileNameHDFS>");
      return 1;
    }

    final String imageID = args[0];
    final String friendlyName = args[1];
    final String outDir = args[2];

    final Configuration conf = getConf();
    final Job job = SKJobFactory.createJobFromConf(
      imageID, friendlyName, "ExtentsExtractor", conf
    );
    
    job.setJarByClass(ExtentsExtractor.class);
    job.setMapperClass(ExtentsExtractorMapper.class);

    job.setNumReduceTasks(1);
    job.setReducerClass(Reducer.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(JsonWritable.class);
    job.setInputFormatClass(FsEntryHBaseInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    SequenceFileOutputFormat.setOutputPath(job, new Path(outDir));

    FsEntryHBaseInputFormat.setupJob(job, imageID);

    System.out.println("Spinning off ExtentsExtractor Job...");
    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main (String[] args) throws Exception {
    System.exit(
      ToolRunner.run(new Configuration(), new ExtentsExtractor(), args)
    );
  }
}
