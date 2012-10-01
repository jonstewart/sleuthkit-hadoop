package com.lightboxtechnologies.spectrum;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.sleuthkit.hadoop.core.SKJobFactory;

public class ImportMetadata extends Configured implements Tool {
  public int run(String[] args)
             throws ClassNotFoundException, InterruptedException, IOException {
    if (args.length != 4) {
      System.err.println("Usage: ImportMetadata <pathToLocalJsonInputFile> <imageID> <friendlyName> <pathToHDFSSequenceFileDirectory>");
      return 1;
    }

    final String jsonPath = args[0];
    final String imageID = args[1];
    final String friendlyName = args[2];
    final String outDir = args[3];

    final Configuration conf = getConf();

    final Job job = SKJobFactory.createJobFromConf(
      imageID, friendlyName, "ImportMetadata", conf
    );
    job.setJarByClass(ImportMetadata.class);
    job.setMapperClass(ImportMetadataMapper.class);
    job.setNumReduceTasks(1);
    job.setReducerClass(Reducer.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(JsonWritable.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(jsonPath));
    SequenceFileOutputFormat.setOutputPath(job, new Path(outDir));

    HBaseTables.summon(
      conf, HBaseTables.ENTRIES_TBL_B, HBaseTables.ENTRIES_COLFAM_B
    );

    System.out.println("Spinning off ImportMetadata Job...");
    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    System.exit(
      ToolRunner.run(HBaseConfiguration.create(), new ImportMetadata(), args)
    );
  }
}
