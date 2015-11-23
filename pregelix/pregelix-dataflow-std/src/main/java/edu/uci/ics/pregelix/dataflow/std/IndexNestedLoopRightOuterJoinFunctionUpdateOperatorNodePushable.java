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
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.INullWriter;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputOperatorNodePushable;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.api.IIndexAccessor;
import org.apache.hyracks.storage.am.common.api.IIndexCursor;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.dataflow.AbstractTreeIndexOperatorDescriptor;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelper;
import org.apache.hyracks.storage.am.common.dataflow.TreeIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.common.ophelpers.MultiComparator;
import org.apache.hyracks.storage.am.common.tuples.PermutingFrameTupleReference;
import edu.uci.ics.pregelix.dataflow.std.base.IRecordDescriptorFactory;
import edu.uci.ics.pregelix.dataflow.std.base.IRuntimeHookFactory;
import edu.uci.ics.pregelix.dataflow.std.base.IUpdateFunctionFactory;
import edu.uci.ics.pregelix.dataflow.std.util.CopyUpdateUtil;
import edu.uci.ics.pregelix.dataflow.std.util.FunctionProxy;
import edu.uci.ics.pregelix.dataflow.std.util.SearchKeyTupleReference;
import edu.uci.ics.pregelix.dataflow.std.util.StorageType;
import edu.uci.ics.pregelix.dataflow.std.util.UpdateBuffer;

public class IndexNestedLoopRightOuterJoinFunctionUpdateOperatorNodePushable extends
        AbstractUnaryInputOperatorNodePushable {
    private IndexDataflowHelper treeIndexOpHelper;
    private FrameTupleAccessor accessor;

    private IFrame writeFrame;
    private FrameTupleAppender appender;
    private ArrayTupleBuilder nullTupleBuilder;
    private DataOutput dos;

    private ITreeIndex index;
    private RangePredicate rangePred;
    private MultiComparator lowKeySearchCmp;
    private MultiComparator highKeySearchCmp;
    private IIndexCursor cursor;
    protected IIndexAccessor indexAccessor;

    private RecordDescriptor recDesc;
    private final RecordDescriptor inputRecDesc;

    private PermutingFrameTupleReference lowKey;
    private PermutingFrameTupleReference highKey;

    private INullWriter[] nullWriter;
    private ITupleReference currentTopTuple;
    private boolean match;

    private final IFrameWriter[] writers;
    private final FunctionProxy functionProxy;
    private ArrayTupleBuilder cloneUpdateTb;
    private final UpdateBuffer updateBuffer;
    private final SearchKeyTupleReference tempTupleReference = new SearchKeyTupleReference();
    private final StorageType storageType;

    public IndexNestedLoopRightOuterJoinFunctionUpdateOperatorNodePushable(AbstractTreeIndexOperatorDescriptor opDesc,
            IHyracksTaskContext ctx, int partition, IRecordDescriptorProvider recordDescProvider, boolean isForward,
            int[] lowKeyFields, int[] highKeyFields, INullWriter[] nullWriter, IUpdateFunctionFactory functionFactory,
            IRuntimeHookFactory preHookFactory, IRuntimeHookFactory postHookFactory,
            IRecordDescriptorFactory inputRdFactory, int outputArity) throws HyracksDataException {
        inputRecDesc = recordDescProvider.getInputRecordDescriptor(opDesc.getActivityId(), 0);
        treeIndexOpHelper = (IndexDataflowHelper) opDesc.getIndexDataflowHelperFactory().createIndexDataflowHelper(
                opDesc, ctx, partition);
        if (treeIndexOpHelper instanceof TreeIndexDataflowHelper) {
            storageType = StorageType.TreeIndex;
        } else {
            storageType = StorageType.LSMIndex;
        }
        this.recDesc = recordDescProvider.getInputRecordDescriptor(opDesc.getActivityId(), 0);

        if (lowKeyFields != null && lowKeyFields.length > 0) {
            lowKey = new PermutingFrameTupleReference();
            lowKey.setFieldPermutation(lowKeyFields);
        }
        if (highKeyFields != null && highKeyFields.length > 0) {
            highKey = new PermutingFrameTupleReference();
            highKey.setFieldPermutation(highKeyFields);
        }
        this.nullWriter = nullWriter;

        this.writers = new IFrameWriter[outputArity];
        this.functionProxy = new FunctionProxy(ctx, functionFactory, preHookFactory, postHookFactory, inputRdFactory,
                writers);
        this.updateBuffer = new UpdateBuffer(ctx);
    }

    protected void setCursor() {
        cursor = indexAccessor.createSearchCursor(true);
    }

    @Override
    public void open() throws HyracksDataException {
        /**
         * function open
         */
        functionProxy.functionOpen();
        accessor = new FrameTupleAccessor(recDesc);

        try {
            treeIndexOpHelper.open();
            index = (ITreeIndex) treeIndexOpHelper.getIndexInstance();

            // construct range predicate
            // TODO: Can we construct the multicmps using helper methods?
            int lowKeySearchFields = index.getComparatorFactories().length;
            int highKeySearchFields = index.getComparatorFactories().length;

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

            rangePred = new RangePredicate(null, null, true, true, lowKeySearchCmp, highKeySearchCmp);

            writeFrame = new VSizeFrame(treeIndexOpHelper.getTaskContext());

            nullTupleBuilder = new ArrayTupleBuilder(inputRecDesc.getFields().length);
            dos = nullTupleBuilder.getDataOutput();
            nullTupleBuilder.reset();
            for (int i = 0; i < inputRecDesc.getFields().length; i++) {
                nullWriter[i].writeNull(dos);
                nullTupleBuilder.addFieldEndOffset();
            }

            appender = new FrameTupleAppender();
            appender.reset(writeFrame, true);

            indexAccessor = index.createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
            setCursor();

            /** set the search cursor */
            rangePred.setLowKey(null, true);
            rangePred.setHighKey(null, true);
            cursor.reset();
            indexAccessor.search(cursor, rangePred);

            /** set up current top tuple */
            if (cursor.hasNext()) {
                cursor.next();
                currentTopTuple = cursor.getTuple();
                match = false;
            }

            cloneUpdateTb = new ArrayTupleBuilder(index.getFieldCount());
        } catch (Exception e) {
            closeResource();
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);
        int tupleCount = accessor.getTupleCount();
        try {
            int i = 0;
            for (; i < tupleCount && currentTopTuple != null;) {
                if (lowKey != null) {
                    lowKey.reset(accessor, i);
                }
                if (highKey != null) {
                    highKey.reset(accessor, i);
                }
                // TODO: currently use low key only, check what they mean
                int cmp = compare(lowKey, currentTopTuple);
                if (cmp <= 0) {
                    if (cmp == 0) {
                        outputMatch(i);
                        currentTopTuple = cursor.getTuple();
                    } else {
                        // process the left-outer case
                        writeResult(accessor, i, null);
                    }
                    i++;
                } else {
                    // process the right-outer case
                    moveTreeCursor();
                }
            }
            // process the left-outer case
            for (; i < tupleCount; i++) {
                writeResult(accessor, i, null);
            }
        } catch (Exception e) {
            closeResource();
            throw new HyracksDataException(e);
        }
    }

    private void outputMatch(int i) throws Exception {
        writeResult(accessor, i, currentTopTuple);
        match = true;
    }

    private void moveTreeCursor() throws Exception {
        if (!match) {
            writeRightOuterResults(currentTopTuple);
        }
        if (cursor.hasNext()) {
            cursor.next();
            currentTopTuple = cursor.getTuple();
            match = false;
        } else {
            currentTopTuple = null;
        }
    }

    @Override
    public void close() throws HyracksDataException {
        try {
            while (currentTopTuple != null) {
                moveTreeCursor();
            }
            try {
                cursor.close();
                //batch update
                updateBuffer.updateIndex(indexAccessor);
            } catch (Exception e) {
                throw new HyracksDataException(e);
            }

            /**
             * function close
             */
            functionProxy.functionClose();
        } catch (Exception e) {
            throw new HyracksDataException(e);
        } finally {
            treeIndexOpHelper.close();
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        closeResource();
        populateFailure();
    }

    private void closeResource() throws HyracksDataException {
        try {
            cursor.close();
        } catch (Exception e) {
            throw new HyracksDataException(e);
        } finally {
            treeIndexOpHelper.close();
        }
    }

    private void populateFailure() throws HyracksDataException {
        for (IFrameWriter writer : writers) {
            writer.fail();
        }
    }

    /** compare tuples */
    private int compare(ITupleReference left, ITupleReference right) throws Exception {
        return lowKeySearchCmp.compare(left, right);
    }

    //for the join match casesos
    private void writeResult(IFrameTupleAccessor leftAccessor, int tIndex, ITupleReference indexTuple) throws Exception {
        /**
         * merge with the cached tuple, if any
         */
        ITupleReference indexEntryTuple = indexTuple;
        if (indexEntryTuple != null) {
            ITupleReference cachedUpdatedLastTuple = updateBuffer.getLastTuple();
            if (cachedUpdatedLastTuple != null) {
                if (compare(cachedUpdatedLastTuple, indexTuple) == 0) {
                    indexEntryTuple = cachedUpdatedLastTuple;
                }
            }
        }
        /**
         * function call
         */
        functionProxy.functionCall(leftAccessor, tIndex, indexEntryTuple, cloneUpdateTb, cursor);

        /**
         * doing clone update
         */
        CopyUpdateUtil.copyUpdate(tempTupleReference, indexEntryTuple, updateBuffer, cloneUpdateTb, indexAccessor,
                cursor, rangePred, true, storageType);
    }

    /** write result for right outer case */
    private void writeRightOuterResults(ITupleReference frameTuple) throws Exception {
        /**
         * function call
         */
        functionProxy.functionCall(nullTupleBuilder, frameTuple, cloneUpdateTb, cursor, true);

        //doing clone update
        CopyUpdateUtil.copyUpdate(tempTupleReference, frameTuple, updateBuffer, cloneUpdateTb, indexAccessor, cursor,
                rangePred, true, storageType);
    }

    @Override
    public void setOutputFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc) {
        writers[index] = writer;
    }
}
