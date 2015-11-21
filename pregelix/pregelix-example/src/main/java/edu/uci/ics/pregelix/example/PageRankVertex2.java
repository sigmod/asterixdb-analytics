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

package edu.uci.ics.pregelix.example;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.pregelix.api.graph.MessageCombiner;
import edu.uci.ics.pregelix.api.graph.MsgList;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.example.PageRankVertex2.StateWritable;
import edu.uci.ics.pregelix.example.client.Client;

/**
 * The basic Pregel PageRank implementation for a specific AsterixDB input.
 */
public class PageRankVertex2 extends Vertex<VLongWritable, StateWritable, FloatWritable, DoubleWritable> {

    public static final String ITERATIONS = "HyracksPageRankVertex.iteration";
    private final DoubleWritable outputValue = new DoubleWritable();
    private final StateWritable tmpVertexValue = new StateWritable();
    private int maxIteration = -1;

    /**
     * Test whether combiner is called by summing up the messages.
     */
    public static class SimpleSumCombiner extends MessageCombiner<VLongWritable, DoubleWritable, DoubleWritable> {
        private double sum = 0.0;
        private final DoubleWritable agg = new DoubleWritable();
        private MsgList<DoubleWritable> msgList;

        @SuppressWarnings({ "rawtypes", "unchecked" })
        @Override
        public void init(MsgList msgList) {
            sum = 0.0;
            this.msgList = msgList;
        }

        @Override
        public void stepPartial(VLongWritable vertexIndex, DoubleWritable msg) throws HyracksDataException {
            sum += msg.get();
        }

        @Override
        public DoubleWritable finishPartial() {
            agg.set(sum);
            return agg;
        }

        @Override
        public void stepFinal(VLongWritable vertexIndex, DoubleWritable partialAggregate) throws HyracksDataException {
            sum += partialAggregate.get();
        }

        @Override
        public MsgList<DoubleWritable> finishFinal() {
            agg.set(sum);
            msgList.clear();
            msgList.add(agg);
            return msgList;
        }

        @Override
        public void setPartialCombineState(DoubleWritable combineState) {
            sum = combineState.get();
        }

        @Override
        public void stepPartial2(VLongWritable vertexIndex, DoubleWritable partialAggregate)
                throws HyracksDataException {
            sum += partialAggregate.get();
        }

        @Override
        public DoubleWritable finishPartial2() {
            agg.set(sum);
            return agg;
        }
    }

    @Override
    public void configure(Configuration conf) {
        maxIteration = conf.getInt(ITERATIONS, 10);
    }

    @Override
    public void compute(Iterator<DoubleWritable> msgIterator) {
        if (getSuperstep() == 1) {
            tmpVertexValue.setValue(1.0 / getNumVertices());
            setVertexValue(tmpVertexValue);
        }
        if (getSuperstep() >= 2 && getSuperstep() <= maxIteration) {
            double sum = 0;
            while (msgIterator.hasNext()) {
                sum += msgIterator.next().get();
            }
            tmpVertexValue.setValue(0.15 / getNumVertices() + 0.85 * sum);
            setVertexValue(tmpVertexValue);
        }

        if (getSuperstep() >= 1 && getSuperstep() < maxIteration) {
            long edges = getNumOutEdges();
            outputValue.set(getVertexValue().getValue() / edges);
            sendMsgToAllEdges(outputValue);
        } else {
            voteToHalt();
        }
    }

    @Override
    public String toString() {
        return getVertexId() + " " + getVertexValue();
    }

    public static class StateWritable implements Writable {

        private DoubleWritable value = new DoubleWritable();
        private Text identifier = new Text();

        public double getValue() {
            return value.get();
        }

        public String getIdentifier(String name) {
            return identifier.toString();
        }

        public void setValue(double v) {
            value.set(v);
        }

        public void setIdentifier(String name) {
            identifier.set(name);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            value.write(out);
            identifier.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            value.readFields(in);
            identifier.readFields(in);
        }
    }

    public static void main(String[] args) throws Exception {
        PregelixJob job = new PregelixJob(PageRankVertex2.class.getSimpleName());
        job.setVertexClass(PageRankVertex2.class);
        job.setMessageCombinerClass(PageRankVertex2.SimpleSumCombiner.class);
        job.setFixedVertexValueSize(true);
        job.setSkipCombinerKey(true);
        Client.run(args, job);
    }

}
