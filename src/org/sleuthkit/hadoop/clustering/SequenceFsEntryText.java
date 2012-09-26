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

package org.sleuthkit.hadoop.clustering;

import java.io.IOException;

import org.apache.commons.codec.DecoderException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lightboxtechnologies.spectrum.FsEntryHBaseInputFormat;

import org.sleuthkit.hadoop.core.HBaseConstants;
import org.sleuthkit.hadoop.core.JobNames;
import org.sleuthkit.hadoop.core.SKJobFactory;

/** Contains methods which will output file data to sequence files for use
 *  with mahout. Only outputs file data for files which also have grep search
 *  matches; the idea is to cluster results which might be interesting in
 *  order to give us a better idea of which files most likely actually *are*
 *  interesting.
 */
public class SequenceFsEntryText {
    public static final Logger LOG = LoggerFactory.getLogger(SequenceFsEntryText.class);

    public static enum WrittenDocumentCount { DOCUMENTS };

    public static final String GREP_MATCHES_TO_SEARCH = "org.sleuthkit.grepsearchfield";

    public static void main(String[] argv) throws Exception {
        runPipeline(argv[0], argv[1], argv[2]);
    }

    /** Runs a mapreduce task which will iterate over the HBase entries table
     * using FSEntry. It will output files on the hdd with the identifier
     * id that have grep matches to one or more sequence files in outDir.
     */
    public static boolean runPipeline(String outDir, String id, String friendlyName) {
        try {
            Job job = SKJobFactory.createJob(id, friendlyName, JobNames.GREP_MATCHED_FILES_OUT);
            job.setJarByClass(SequenceFsEntryText.class);
            job.setMapperClass(SequenceFsEntryTextMapper.class);

            // We don't need a combiner or a reducer for this job. We aren't
            // writing anything out either.
            job.setNumReduceTasks(0);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setInputFormatClass(FsEntryHBaseInputFormat.class);
            FsEntryHBaseInputFormat.setupJob(job, id);

            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            SequenceFileOutputFormat.setOutputPath(job, new Path(outDir));

            // we want to search the default grep results. If there are any, then
            // dump the text to a file.
            job.getConfiguration().set(GREP_MATCHES_TO_SEARCH, HBaseConstants.GREP_RESULTS);

            job.waitForCompletion(true);

            long filesWritten = job.getCounters().findCounter(WrittenDocumentCount.DOCUMENTS).getValue();

            // If we wrote some files, return 0. Else, return 1. 
            return filesWritten != 0 ? true : false;
        } catch (IOException ex) {
            LOG.error("IO Exception while writing documents to sequence files.", ex);
        } catch (DecoderException ex) {
            LOG.error("Decoder Exception while setting up FsEntryHBaseInputFormat.", ex);
        } catch (InterruptedException ex) {
            LOG.error("InterruptedException while running job.", ex);
        } catch (ClassNotFoundException ex) {
            LOG.error("ClassNotFoundException while spinning off job.", ex);
        }
        return false;
    }
}
