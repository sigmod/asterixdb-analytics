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
package edu.uci.ics.external.connector.asterixdb.dataflow;

import java.nio.ByteBuffer;

import edu.uci.ics.asterix.om.pointables.ARecordPointable;
import edu.uci.ics.asterix.om.pointables.PointableAllocator;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.external.connector.asterixdb.api.IReadConverter;
import edu.uci.ics.external.connector.asterixdb.api.IReadConverterFactory;
import edu.uci.ics.hyracks.api.comm.IFrame;
import edu.uci.ics.hyracks.api.comm.VSizeFrame;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

/**
 * This operator transforms AsterixDB types read from an Asterix LSMBTree to ANY other forms
 * that work with an external runtime.
 */
public class ReadTransformOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;
    private final int fieldSize = 2;
    private final ARecordType recordType;
    private final IReadConverterFactory readConverterFacotry;

    public ReadTransformOperatorDescriptor(JobSpecification spec, RecordDescriptor rDesc, ARecordType recordType,
            IReadConverterFactory recordConverterFactory) {
        super(spec, 1, 1);
        this.recordDescriptors[0] = rDesc;
        this.recordType = recordType;
        this.readConverterFacotry = recordConverterFactory;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            final IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions)
            throws HyracksDataException {
        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {

            private final RecordDescriptor rd0 = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
            private final FrameTupleAppender appender = new FrameTupleAppender();
            private final ArrayTupleBuilder outputTb = new ArrayTupleBuilder(fieldSize);
            private final IFrame frame = new VSizeFrame(ctx);
            private final FrameTupleAccessor accessor = new FrameTupleAccessor(rd0);
            private final ARecordPointable recordPointable = (ARecordPointable) new PointableAllocator()
                    .allocateRecordValue(recordType);
            private final IReadConverter readConverter = readConverterFacotry.getFieldReadConverter();

            @Override
            public void open() throws HyracksDataException {
                writer.open();
                appender.reset(frame, true);
            }

            @Override
            public void nextFrame(ByteBuffer frame) throws HyracksDataException {
                accessor.reset(frame);
                for (int tIndex = 0; tIndex < accessor.getTupleCount(); tIndex++) {
                    // Record is the second field.
                    int fldStart = accessor.getTupleStartOffset(tIndex) + accessor.getFieldSlotsLength()
                            + accessor.getFieldStartOffset(tIndex, 1);
                    int fldLen = accessor.getFieldLength(tIndex, 1);
                    // Parses the binary input.
                    recordPointable.set(accessor.getBuffer().array(), fldStart, fldLen);
                    // Converts the record into a tuple of a user-defined type.
                    readConverter.convert(recordType, recordPointable, outputTb);
                    // Writes the result into the output writer.
                    FrameUtils.appendToWriter(writer, appender, outputTb.getFieldEndOffsets(), outputTb.getByteArray(),
                            0, outputTb.getSize());
                }
            }

            @Override
            public void close() throws HyracksDataException {
                if (appender.getTupleCount() > 0) {
                    FrameUtils.flushFrame(frame.getBuffer(), writer);
                }
                writer.close();
            }

            @Override
            public void fail() throws HyracksDataException {
                writer.fail();
            }

        };
    }
}
