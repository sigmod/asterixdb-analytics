package org.apache.hyracks.imru.api;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;

public class FrameWriter {
    IFrameWriter writer;

    public FrameWriter(IFrameWriter writer) {
        this.writer = writer;
    }

    public void writeFrame(ByteBuffer frame) throws HyracksDataException {
        FrameUtils.flushFrame(frame, writer);
    }
}
