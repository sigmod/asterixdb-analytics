package org.apache.hyracks.imru.dataflow;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;

import org.json.JSONException;
import org.json.JSONObject;

import org.apache.hyracks.api.application.ICCApplicationContext;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.IPartitionCollector;
import org.apache.hyracks.api.comm.IPartitionWriterFactory;
import org.apache.hyracks.api.constraints.Constraint;
import org.apache.hyracks.api.constraints.IConstraintAcceptor;
import org.apache.hyracks.api.constraints.expressions.PartitionCountExpression;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ConnectorDescriptorId;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.OperatorDescriptorId;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.ActivityCluster;
import org.apache.hyracks.api.job.IConnectorDescriptorRegistry;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.std.base.AbstractConnectorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractMToNConnectorDescriptor;
import org.apache.hyracks.dataflow.std.collectors.NonDeterministicChannelReader;
import org.apache.hyracks.dataflow.std.collectors.NonDeterministicFrameReader;
import org.apache.hyracks.dataflow.std.collectors.PartitionCollector;
import org.apache.hyracks.dataflow.std.connectors.PartitionDataWriter;
import org.apache.hyracks.imru.data.MergedFrames;
import org.apache.hyracks.imru.jobgen.SpreadGraph;
import org.apache.hyracks.imru.util.Rt;

public class SpreadConnectorDescriptor extends AbstractMToNConnectorDescriptor {
    private static final long serialVersionUID = 1L;
    SpreadGraph.Level from;
    SpreadGraph.Level to;

    public SpreadConnectorDescriptor(IConnectorDescriptorRegistry spec,
            SpreadGraph.Level from, SpreadGraph.Level to) {
        super(spec);
        this.from = from;
        this.to = to;
    }

    @Override
    public IFrameWriter createPartitioner(final IHyracksTaskContext ctx,
            RecordDescriptor recordDesc,
            final IPartitionWriterFactory edwFactory,
            final int senderPartition, int nProducerPartitions,
            final int consumerPartitionCount) throws HyracksDataException {
        return new IFrameWriter() {
            private final IFrameWriter[] pWriters;

            {
                pWriters = new IFrameWriter[consumerPartitionCount];
                for (int i = 0; i < consumerPartitionCount; ++i) {
                    try {
                        pWriters[i] = edwFactory.createFrameWriter(i);
                    } catch (IOException e) {
                        throw new HyracksDataException(e);
                    }
                }
            }

            @Override
            public void close() throws HyracksDataException {
                for (int i = 0; i < pWriters.length; ++i) {
                    pWriters[i].close();
                }
            }

            private void flushFrame(ByteBuffer buffer, IFrameWriter frameWriter)
                    throws HyracksDataException {
                buffer.position(0);
                buffer.limit(buffer.capacity());
                frameWriter.nextFrame(buffer);
            }

            @Override
            public void open() throws HyracksDataException {
                for (int i = 0; i < pWriters.length; ++i) {
                    pWriters[i].open();
                }
            }

            @Override
            public void nextFrame(ByteBuffer buffer)
                    throws HyracksDataException {
                int targetPartition = buffer.getInt(MergedFrames.TARGET_OFFSET);
                flushFrame(buffer, pWriters[targetPartition]);
                //                if (from != null)
                //                    Rt.p("Level " + from.level + "->" + to.level + ": " + senderPartition + " "
                //                            + from.nodes.get(senderPartition) + "->" + targetPartition + " "
                //                            + to.nodes.get(targetPartition).name);
                //                    Rt.p(from.nodes.get(senderPartition) + "->" + to.nodes.get(targetPartition).name);
            }

            @Override
            public void fail() throws HyracksDataException {
                for (int i = 0; i < pWriters.length; ++i) {
                    pWriters[i].fail();
                }
            }
        };
    }

    @Override
    public IPartitionCollector createPartitionCollector(
            IHyracksTaskContext ctx, RecordDescriptor recordDesc, int index,
            int nProducerPartitions, int nConsumerPartitions)
            throws HyracksDataException {
        BitSet expectedPartitions = new BitSet(nProducerPartitions);
        expectedPartitions.set(0, nProducerPartitions);
        NonDeterministicChannelReader channelReader = new NonDeterministicChannelReader(
                nProducerPartitions, expectedPartitions);
        NonDeterministicFrameReader frameReader = new NonDeterministicFrameReader(
                channelReader);
        return new PartitionCollector(ctx, getConnectorId(), index,
                expectedPartitions, frameReader, channelReader);
    }
}