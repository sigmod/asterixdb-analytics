/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.pregelix.example.asterixdb;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.api.util.ConservativeCheckpointHook;
import edu.uci.ics.pregelix.example.PageRankVertex;
import edu.uci.ics.pregelix.example.PageRankVertex.SimplePageRankVertexOutputFormat;
import edu.uci.ics.pregelix.example.converter.VLongIdInputVertexConverter;
import edu.uci.ics.pregelix.example.converter.VLongIdOutputVertexConverter;
import edu.uci.ics.pregelix.example.data.VLongNormalizedKeyComputer;
import edu.uci.ics.pregelix.example.inputformat.TextPageRankInputFormat;

public class JobGenerator {
    private static String OUTPUT_BASE = "src/test/resources/jobs-asterixdb/";
    private static String DATAVERSE = "graph";
    private static String INPUT_DATASET = "webmap";
    private static String OUTPUT_DATASET = "ranks";

    private static void generatePageRankJob(String jobName, String outputPath) throws IOException {
        PregelixJob job = new PregelixJob(jobName);
        job.setVertexClass(PageRankVertex.class);
        job.setVertexInputFormatClass(TextPageRankInputFormat.class);
        job.setVertexOutputFormatClass(SimplePageRankVertexOutputFormat.class);
        job.setMessageCombinerClass(PageRankVertex.SimpleSumCombiner.class);
        job.setNoramlizedKeyComputerClass(VLongNormalizedKeyComputer.class);
        job.setFixedVertexValueSize(true);
        job.getConfiguration().setLong(PregelixJob.NUM_VERTICE, 20);
        job.setCheckpointHook(ConservativeCheckpointHook.class);
        job.setGroupByAlgorithm(false);
        job.setGroupByMemoryLimit(3);
        job.setFrameSize(1024);

        //lets the job use asterixdb for data source and data sink.
        job.setUseAsterixDBDataSink(true);
        job.setUseAsterixDBDataSink(true);
        job.setAsterixDBInputDataverse(DATAVERSE);
        job.setAsterixDBInputDataset(INPUT_DATASET);
        job.setAsterixDBInputConverterClass(VLongIdInputVertexConverter.class);
        job.setAsterixDBOutputDataverse(DATAVERSE);
        job.setAsterixDBOutputDataset(OUTPUT_DATASET);
        job.setAsterixDBOutputConverterClass(VLongIdOutputVertexConverter.class);

        // writes a job file.
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
    }

    public static void genPageRank() throws IOException {
        generatePageRankJob("pagerank", OUTPUT_BASE + "pagerank.xml");
    }

    public static void main(String[] args) throws IOException {
        genPageRank();
    }
}
