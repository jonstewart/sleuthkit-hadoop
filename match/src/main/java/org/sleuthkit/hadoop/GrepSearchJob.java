/*
   Copyright 2011 Basis Technology Corp.

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

package org.sleuthkit.hadoop;


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;

/* Extracts matching regexes from input files and counts them. */
public class GrepSearchJob {
    private GrepSearchJob() {}


    public static final String INPUT_DIR =  "hdfs://localhost/texaspete/text/";
    public static final String OUTPUT_DIR = "hdfs://localhost/texaspete/grepped/";


    public int run(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Grep <regex> [<group>]");
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        Job job = new Job();
        job.setJarByClass(GrepSearchJob.class);

        try {

            job.setJobName("sleuthkit-grep-search");

            FileSystem fs = FileSystem.get(job.getConfiguration());
            fs.delete(new Path(OUTPUT_DIR), true);
            Path inFile = new Path(args[0]);
            FSDataInputStream in = fs.open(inFile);

            // Read the regex file, set a property on the configuration object
            // to store them in a place accessible by all of the child jobs.

            byte[] bytes = new byte[1024];

            StringBuilder b = new StringBuilder();
            int i = in.read(bytes);
            while ( i != -1) {
                b.append(new String(bytes).substring(0, i));
                i = in.read(bytes);
            }
            System.out.println("regexes are: " + b.toString());
            String regexes = b.toString();

            job.getConfiguration().set("mapred.mapper.regex", regexes);

            FileInputFormat.setInputPaths(job, new Path(INPUT_DIR));

            FileOutputFormat.setOutputPath(job, new Path(OUTPUT_DIR));

            job.setMapperClass(GrepSearchJob.GrepMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);


            // We could set a combiner here to improve performance on distributed
            // systems.
            //grepJob.setCombinerClass(SetReducer.class);

            job.setReducerClass(GrepSearchJob.SetReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(ArrayWritable.class);


            job.setInputFormatClass(SequenceFileInputFormat.class);

            job.setOutputFormatClass(SequenceFileOutputFormat.class);


            System.out.println("About to run the job...");

            // Possibly, we may want to base some sorting code off of this later.
            //JobClient.runJob(grepJob);

            //JobConf sortJob = new JobConf(GrepSearchJob.class);
            //sortJob.setJobName("grep-sort");

            //FileInputFormat.setInputPaths(sortJob, tempDir);
            //sortJob.setInputFormat(SequenceFileInputFormat.class);

            //sortJob.setMapperClass(InverseMapper.class);

            //sortJob.setNumReduceTasks(1);                 // write a single file
            //FileOutputFormat.setOutputPath(sortJob, new Path(OUTPUT_DIR));
            //sortJob.setOutputKeyComparatorClass           // sort by decreasing freq
            //(LongWritable.DecreasingComparator.class);

            //JobClient.runJob(sortJob);
            return job.waitForCompletion(true) ? 0 : 1;
        } catch (Exception ex) {
            ex.printStackTrace();
            return 2;
        }
    }

    public static void main(String[] args) throws Exception {
        new GrepSearchJob().run(args);
        //int res = ToolRunner.run(new Configuration(), new GrepSearchJob(), args);
        System.exit(0);
    }

    // Maps regex matches to an output file.
    public static class GrepMapper 
    extends Mapper<Text, Text, Text, Text> {

        private List<Pattern> patterns = new ArrayList<Pattern>();

        @Override
        public void setup(Context ctx) {
            String[] regexlist = ctx.getConfiguration().get("mapred.mapper.regex").split("\n");
            System.out.print("Found Regexes: " + regexlist.length);

            for (String item : regexlist) {
                if ("".equals(item)) continue; // don't add empty regexes
                try {
                    patterns.add(Pattern.compile(item));
                } catch (Exception ex) {
                    // not much to do...
                }
            }
        }

        @Override
        public void map(Text key, Text value, Context context)
        throws IOException {
            String text = value.toString();
            //System.out.println("Mapping : " + key.toString() + " value: " + value.toString());
            for (Pattern item : patterns) {
                Matcher matcher = item.matcher(text);
                while (matcher.find()) {
                    try {
                        context.write(key, new Text(matcher.group()));
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

            }
        }
    }

    // Reduces a class using a java set to remove duplicates.
    public static class SetReducer extends Reducer<Text, Text, Text, ArrayWritable>{
        @Override
        public void reduce(Text key, Iterable<Text> values,
                Context ctx)
        throws IOException {

            Set<String> s = new HashSet<String>();

            for (Text t : values) {
                s.add(t.toString());
            }
            String[] str = new String[1];
            try {
                ctx.write(key, new ArrayWritable(s.toArray(str)));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}