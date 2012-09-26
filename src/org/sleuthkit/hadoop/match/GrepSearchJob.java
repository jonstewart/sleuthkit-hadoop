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

package org.sleuthkit.hadoop.match;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;

import com.lightboxtechnologies.spectrum.FsEntry;
import com.lightboxtechnologies.spectrum.FsEntryHBaseInputFormat;
import com.lightboxtechnologies.spectrum.FsEntryHBaseOutputFormat;

import org.sleuthkit.hadoop.core.JobNames;
import org.sleuthkit.hadoop.core.SKJobFactory;

/** Extracts matching regexes from FsEntry instances generated from
 * the file table for a given image. The data is placed back into the
 * table through FsEntryHBaseOutputFormat.
 */
public class GrepSearchJob {
    public GrepSearchJob() {}

    public int run(String[] args) throws Exception {
        if (args.length < 1) {
            // TODO: We don't support groups yet. We could add them.
            System.out.println("Grep <table> <deviceID> <regexFile> <friendlyName> [<group>]");
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }
        return runPipeline(args[0], args[1], args[2], args[3]);
    }
    
    // Kicks off a mapreduce job that does the following:
    // 1. Scan the HBASE table for files on the drive specified by the
    // device ID.
    // 2. Run the java regexes from the given regexFile on that file.
    public static int runPipeline(String table, String deviceID, String regexFile, String friendlyName) {
        
        try {
            Job job = SKJobFactory.createJob(deviceID, friendlyName, JobNames.GREP_SEARCH);
            job.setJarByClass(GrepSearchJob.class);

            FileSystem fs = FileSystem.get(job.getConfiguration());
            //fs.delete(new Path(outputdir), true);
            Path inFile = new Path(regexFile);
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
            
            
            job.setMapperClass(GrepMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(FsEntry.class);

            // we are not reducing.
            job.setNumReduceTasks(0);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(FsEntry.class);

            job.setInputFormatClass(FsEntryHBaseInputFormat.class);
            FsEntryHBaseInputFormat.setupJob(job, deviceID);
            job.setOutputFormatClass(FsEntryHBaseOutputFormat.class);
            
            System.out.println("About to run the job...");
            
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
}
