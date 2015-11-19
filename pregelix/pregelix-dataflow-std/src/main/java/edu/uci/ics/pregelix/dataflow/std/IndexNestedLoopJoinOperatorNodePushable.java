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
package edu.uci.ics.pregelix.dataflow.std;

import java.io.DataOutput;
import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.api.IIndexAccessor;
import org.apache.hyracks.storage.am.common.api.IIndexCursor;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.dataflow.AbstractTreeIndexOperatorDescriptor;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelper;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.common.ophelpers.MultiComparator;
import org.apache.hyracks.storage.am.common.tuples.PermutingFrameTupleReference;

public class IndexNestedLoopJoinOperatorNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {
    private IndexDataflowHelper treeIndexOpHelper;
    private FrameTupleAccessor accessor;

    private IFrame writeFrame;
    private FrameTupleAppender appender;
    private ArrayTupleBuilder tb;
    private DataOutput dos;

    private ITreeIndex index;
    private PermutingFrameTupleReference lowKey;
    private PermutingFrameTupleReference highKey;
    private boolean lowKeyInclusive;
    private boolean highKeyInclusive;
    private RangePredicate rangePred;
    private MultiComparator lowKeySearchCmp;
    private MultiComparator highKeySearchCmp;
    private IIndexCursor cursor;
    protected IIndexAccessor indexAccessor;

    private RecordDescriptor recDesc;
    private final RecordDescriptor inputRecDesc;

    public IndexNestedLoopJoinOperatorNodePushable(AbstractTreeIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx,
            int partition, IRecordDescriptorProvider recordDescProvider, boolean isForward, int[] lowKeyFields,
            int[] highKeyFields, boolean lowKeyInclusive, boolean highKeyInclusive) {
        inputRecDesc = recordDescProvider.getInputRecordDescriptor(opDesc.getActivityId(), 0);
        treeIndexOpHelper = (IndexDataflowHelper) opDesc.getIndexDataflowHelperFactory().createIndexDataflowHelper(
                opDesc, ctx, partition);
        this.lowKeyInclusive = lowKeyInclusive;
        this.highKeyInclusive = highKeyInclusive;
        this.recDesc = recordDescProvider.getInputRecordDescriptor(opDesc.getActivityId(), 0);
        if (lowKeyFields != null && lowKeyFields.length > 0) {
            lowKey = new PermutingFrameTupleReference();
            lowKey.setFieldPermutation(lowKeyFields);
        }
        if (highKeyFields != null && highKeyFields.length > 0) {
            highKey = new PermutingFrameTupleReference();
            highKey.setFieldPermutation(highKeyFields);
        }
    }

    protected void setCursor() {
        cursor = indexAccessor.createSearchCursor(false);
    }

    @Override
    public void open() throws HyracksDataException {
        accessor = new FrameTupleAccessor(recDesc);

        try {
            treeIndexOpHelper.open();
            index = (ITreeIndex) treeIndexOpHelper.getIndexInstance();
            writer.open();

            int lowKeySearchFields = index.getComparatorFactories().length;
            int highKeySearchFields = index.getComparatorFactories().length;
            if (lowKey != null) {
                lowKeySearchFields = lowKey.getFieldCount();
            }
            if (highKey != null) {
                highKeySearchFields = highKey.getFieldCount();
            }

            IBinaryComparator[] lowKeySearchComparators = new IBinaryComparator[lowKeySearchFields];
            for (int i = 0; i < lowKeySearchFields; i++) {
                lowKeySearchComparators[i] = index.getComparatorFactories()[i].createBinaryComparator();
            }
            lowKeySearchCmp = new MultiComparator(lowKeySearchComparators);

            if (lowKeySearchFields == highKeySearchFields) {
                highKeySearchCmp = lowKeySearchCmp;
            } else {
                IBinaryComparator[] highKeySearchComparators = new IBinaryComparator[highKeySearchFields];
                for (int i = 0; i < highKeySearchFields; i++) {
                    highKeySearchComparators[i] = index.getComparatorFactories()[i].createBinaryComparator();
                }
                highKeySearchCmp = new MultiComparator(highKeySearchComparators);
            }

            rangePred = new RangePredicate(null, null, lowKeyInclusive, highKeyInclusive, lowKeySearchCmp,
                    highKeySearchCmp);
            writeFrame = new VSizeFrame(treeIndexOpHelper.getTaskContext());
            tb = new ArrayTupleBuilder(inputRecDesc.getFields().length + index.getFieldCount());
            dos = tb.getDataOutput();
            appender = new FrameTupleAppender();
            appender.reset(writeFrame, true);
            indexAccessor = index.createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
            setCursor();
        } catch (Exception e) {
            treeIndexOpHelper.close();
            throw new HyracksDataException(e);
        }
    }

    private void writeSearchResults(IFrameTupleAccessor leftAccessor, int tIndex) throws Exception {
        while (cursor.hasNext()) {
            tb.reset();
            cursor.next();

            ITupleReference frameTuple = cursor.getTuple();
            for (int i = 0; i < inputRecDesc.getFields().length; i++) {
                int tupleStart = leftAccessor.getTupleStartOffset(tIndex);
                int fieldStart = leftAccessor.getFieldStartOffset(tIndex, i);
                int offset = leftAccessor.getFieldSlotsLength() + tupleStart + fieldStart;
                int len = leftAccessor.getFieldEndOffset(tIndex, i) - fieldStart;
                dos.write(leftAccessor.getBuffer().array(), offset, len);
                tb.addFieldEndOffset();
            }
            for (int i = 0; i < frameTuple.getFieldCount(); i++) {
                dos.write(frameTuple.getFieldData(i), frameTuple.getFieldStart(i), frameTuple.getFieldLength(i));
                tb.addFieldEndOffset();
            }
            FrameUtils.appendToWriter(writer, appender, tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());
        }
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);

        int tupleCount = accessor.getTupleCount();
        try {
            for (int i = 0; i < tupleCount; i++) {
                if (lowKey != null) {
                    lowKey.reset(accessor, i);
                }
                if (highKey != null) {
                    highKey.reset(accessor, i);
                }
                rangePred.setLowKey(lowKey, lowKeyInclusive);
                rangePred.setHighKey(highKey, highKeyInclusive);

                cursor.reset();
                indexAccessor.search(cursor, rangePred);
                writeSearchResults(accessor, i);
            }
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void close() throws HyracksDataException {
        try {
            if (appender.getTupleCount() > 0) {
                FrameUtils.flushFrame(writeFrame.getBuffer(), writer);
            }
            writer.close();
            try {
                cursor.close();
            } catch (Exception e) {
                throw new HyracksDataException(e);
            }
        } finally {
            treeIndexOpHelper.close();
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        writer.fail();
    }
}