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

package edu.uci.ics.pregelix.benchmark.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Map.Entry;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.MapMutableEdge;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class TextCCInputFormat extends TextVertexInputFormat<VLongWritable, VLongWritable, NullWritable> {

    @Override
    public TextVertexReader createVertexReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new TextVertexReaderFromEachLine() {
            StringTokenizer items;

            @Override
            protected VLongWritable getId(Text line) throws IOException {
                items = new StringTokenizer(line.toString());
                return new VLongWritable(Long.parseLong(items.nextToken()));
            }

            @Override
            protected VLongWritable getValue(Text line) throws IOException {
                return null;
            }

            @Override
            protected Iterable<Edge<VLongWritable, NullWritable>> getEdges(Text line) throws IOException {
                List<Edge<VLongWritable, NullWritable>> edges = new ArrayList<Edge<VLongWritable, NullWritable>>();
                Map<VLongWritable, NullWritable> edgeMap = new HashMap<VLongWritable, NullWritable>();
                while (items.hasMoreTokens()) {
                    edgeMap.put(new VLongWritable(Long.parseLong(items.nextToken())), null);
                }
                for (Entry<VLongWritable, NullWritable> entry : edgeMap.entrySet()) {
                    MapMutableEdge<VLongWritable, NullWritable> edge = new MapMutableEdge<VLongWritable, NullWritable>();
                    edge.setEntry(entry);
                    edge.setValue(null);
                    edges.add(edge);
                }
                return edges;
            }

        };
    }

}
