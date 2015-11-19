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
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

public class ProjectOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;
    private final int[] projectFields;

    public ProjectOperatorDescriptor(JobSpecification spec, RecordDescriptor rDesc, int projectFields[]) {
        super(spec, 1, 1);
        this.recordDescriptors[0] = rDesc;
        this.projectFields = projectFields;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            final IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions)
                    throws HyracksDataException {
        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
            private final RecordDescriptor rd0 = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
            private final FrameTupleAccessor fta = new FrameTupleAccessor(rd0);
            private final FrameTupleAppender appender = new FrameTupleAppender();
            private final ArrayTupleBuilder tb = new ArrayTupleBuilder(projectFields.length);
            private final DataOutput dos = tb.getDataOutput();
            private final IFrame writeFrame = new VSizeFrame(ctx);

            @Override
            public void close() throws HyracksDataException {
                if (appender.getTupleCount() > 0) {
                    FrameUtils.flushFrame(writeFrame.getBuffer(), writer);
                }
                writer.close();
            }

            @Override
            public void fail() throws HyracksDataException {
                writer.fail();
            }

            @Override
            public void nextFrame(ByteBuffer frame) throws HyracksDataException {
                fta.reset(frame);
                int tupleCount = fta.getTupleCount();
                try {
                    for (int tIndex = 0; tIndex < tupleCount; tIndex++) {
                        tb.reset();
                        for (int j = 0; j < projectFields.length; j++) {
                            int fIndex = projectFields[j];
                            int tupleStart = fta.getTupleStartOffset(tIndex);
                            int fieldStart = fta.getFieldStartOffset(tIndex, fIndex);
                            int offset = fta.getFieldSlotsLength() + tupleStart + fieldStart;
                            int len = fta.getFieldEndOffset(tIndex, fIndex) - fieldStart;
                            dos.write(fta.getBuffer().array(), offset, len);
                            tb.addFieldEndOffset();
                        }
                        FrameUtils.appendToWriter(writer, appender, tb.getFieldEndOffsets(), tb.getByteArray(), 0,
                                tb.getSize());
                    }
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }

            @Override
            public void open() throws HyracksDataException {
                writer.open();
                appender.reset(writeFrame, true);
            }

        };
    }
}
