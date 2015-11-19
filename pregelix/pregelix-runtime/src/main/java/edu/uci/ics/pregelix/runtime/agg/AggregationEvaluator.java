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

package edu.uci.ics.pregelix.runtime.agg;

import java.io.DataInput;
import java.io.DataInputStream;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluator;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import edu.uci.ics.pregelix.api.graph.MessageCombiner;
import edu.uci.ics.pregelix.api.graph.MsgList;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.pregelix.dataflow.base.IConfigurationFactory;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class AggregationEvaluator implements IAggregateEvaluator {
    private final Configuration conf;
    private final boolean isFinalStage;
    private final boolean partialAggAsInput;
    private MessageCombiner combiner;
    private WritableComparable key;
    private Writable value;
    private Writable combinedResult;
    private MsgList msgList = new MsgList();

    private final ByteBufferInputStream keyInputStream = new ByteBufferInputStream();
    private final ByteBufferInputStream valueInputStream = new ByteBufferInputStream();
    private final DataInput keyInput = new DataInputStream(keyInputStream);
    private final DataInput valueInput = new DataInputStream(valueInputStream);
    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    private boolean skipKey = false;

    public AggregationEvaluator(IHyracksTaskContext ctx, IConfigurationFactory confFactory, boolean isFinalStage,
            boolean partialAggAsInput) throws HyracksDataException {
        this.conf = confFactory.createConfiguration(ctx);
        this.isFinalStage = isFinalStage;
        this.partialAggAsInput = partialAggAsInput;
        msgList.setConf(this.conf);

        combiner = BspUtils.createMessageCombiner(conf);
        key = BspUtils.createVertexIndex(conf);
        value = !partialAggAsInput ? BspUtils.createMessageValue(conf) : BspUtils.createPartialCombineValue(conf);
        skipKey = BspUtils.getSkipCombinerKey(conf);
    }

    @Override
    public void init() throws AlgebricksException {
        combiner.init(msgList);
        resultStorage.reset();
    }

    @Override
    public void step(IFrameTupleReference tuple) throws AlgebricksException {
        try {
            readKeyValue(tuple);
            if (!isFinalStage) {
                if (!partialAggAsInput) {
                    combiner.stepPartial(key, value);
                } else {
                    combiner.stepPartial2(key, value);
                }
            } else {
                combiner.stepFinal(key, value);
            }
        } catch (Exception e) {
            throw new AlgebricksException(e);
        }
    }

    @Override
    public void finishPartial(IPointable result) throws AlgebricksException {
        finishInternal(result);
    }

    @Override
    public void finish(IPointable result) throws AlgebricksException {
        finishInternal(result);
    }

    private void finishInternal(IPointable result) throws AlgebricksException {
        try {
            if (!isFinalStage) {
                if (!partialAggAsInput) {
                    combinedResult = combiner.finishPartial();
                } else {
                    combinedResult = combiner.finishPartial2();
                }
            } else {
                combinedResult = combiner.finishFinal();
            }
            // Writes combined result.
            combinedResult.write(resultStorage.getDataOutput());
            result.set(resultStorage);
        } catch (Exception e) {
            throw new AlgebricksException(e);
        }
    }

    private void readKeyValue(IFrameTupleReference tuple) throws HyracksDataException {
        FrameTupleReference ftr = (FrameTupleReference) tuple;
        IFrameTupleAccessor fta = ftr.getFrameTupleAccessor();
        ByteBuffer buffer = fta.getBuffer();
        int tIndex = ftr.getTupleIndex();

        int keyStart = fta.getFieldSlotsLength() + fta.getTupleStartOffset(tIndex) + fta.getFieldStartOffset(tIndex, 0);
        int valueStart = fta.getFieldSlotsLength() + fta.getTupleStartOffset(tIndex)
                + fta.getFieldStartOffset(tIndex, 1);

        keyInputStream.setByteBuffer(buffer, keyStart);
        valueInputStream.setByteBuffer(buffer, valueStart);

        try {
            //read key if necessary
            if (!skipKey) {
                key.readFields(keyInput);
            }
            //read value
            value.readFields(valueInput);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

}
