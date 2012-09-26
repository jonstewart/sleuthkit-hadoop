
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


package org.sleuthkit.hadoop.pipeline;

import org.sleuthkit.hadoop.clustering.ClusterDocumentsJob;
import org.sleuthkit.hadoop.clustering.SequenceFsEntryText;
import org.sleuthkit.hadoop.clustering.TokenizeAndVectorizeDocuments;
import org.sleuthkit.hadoop.match.GrepReportGenerator;
import org.sleuthkit.hadoop.match.GrepSearchJob;
import org.sleuthkit.hadoop.scoring.CrossImageScorerJob;
import org.sleuthkit.hadoop.textextraction.FSEntryTikaTextExtractor;

import com.lightboxtechnologies.spectrum.HBaseTables;
import com.lightboxtechnologies.spectrum.HDFSArchiver;

/** Runs all of the post-ingest tasks on the hadoop cloud, one after another. 
 * TODO: we could parallelize much of the things that happen here, as although
 * they run in sequence, many of them are not dependent on each other to run. */
public class Pipeline {
  // A file containing lines of text, each of which represents a regex.
  public static final String GREP_KEYWORDS = "/texaspete/regexes";

  public static void main(String[] argv) throws Exception {
    if (argv.length != 2) {
      System.err.println("Usage: Pipeline image_id friendly_name");
      System.exit(1);
    }
    final String imageID = argv[0];
    final String friendlyName = argv[1];
    final String prefix = "/texaspete/data/" + imageID;
    final String seqDumpDirectory = prefix + "/text";
    final String tokenDumpDirectory = prefix + "/tokens";
    final String vectorDumpDirectory = prefix + "/vectors";
    final String clusterDumpDirectory = prefix + "/clusters";
    final String dictionaryDumpDirectory = prefix + "/vectors/dictionary.file-0";

    FSEntryTikaTextExtractor.runPipeline(HBaseTables.ENTRIES_TBL, imageID, friendlyName);
    GrepSearchJob.runPipeline(HBaseTables.ENTRIES_TBL, imageID, GREP_KEYWORDS, friendlyName);

    // This will allow us to only cluster if we have documents that are written out to sequence files.
    // In other words, clustering will only take place if there are things TO cluster.
    boolean filesToSequence = (SequenceFsEntryText.runPipeline(seqDumpDirectory, imageID, friendlyName));
    if (filesToSequence) {
      TokenizeAndVectorizeDocuments.runPipeline(seqDumpDirectory, tokenDumpDirectory, vectorDumpDirectory);
      ClusterDocumentsJob.runPipeline(vectorDumpDirectory + "/tfidf-vectors/", clusterDumpDirectory, dictionaryDumpDirectory, .65, .65, imageID, friendlyName, prefix);

    }
    GrepReportGenerator.runPipeline(GREP_KEYWORDS, imageID, friendlyName, prefix);
    
    CrossImageScorerJob.runPipeline(prefix, imageID, friendlyName);

    HDFSArchiver.runPipeline(prefix + "/reports", prefix + "/reports.zip");
  }
}
