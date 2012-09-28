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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import org.sleuthkit.hadoop.core.SKJobFactory;

public class ExtentsExtractor {
  public static void reportUsageAndExit() {
    System.err.println("Usage: ExtentsExtractor <imageID> <friendlyName> <sequenceFileNameHDFS>");
    System.exit(-1);
  }

  public static int run(String imageID, String friendlyName, String outDir) throws Exception {
    final Job job = SKJobFactory.createJob(imageID, friendlyName, "ExtentsExtractor");
    
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
    job.waitForCompletion(true);
    return 0;
  }

  public static void main (String[] argv) throws Exception {
    run(argv[0], argv[1], argv[2]);
  }
}
