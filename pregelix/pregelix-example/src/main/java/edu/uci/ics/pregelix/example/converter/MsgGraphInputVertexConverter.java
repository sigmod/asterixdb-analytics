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

package edu.uci.ics.pregelix.example.converter;

import java.io.DataInput;
import java.io.DataInputStream;
import java.util.List;

import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import edu.uci.ics.asterix.om.pointables.AFlatValuePointable;
import edu.uci.ics.asterix.om.pointables.AListVisitablePointable;
import edu.uci.ics.asterix.om.pointables.ARecordVisitablePointable;
import edu.uci.ics.asterix.om.pointables.base.IVisitablePointable;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.pregelix.api.converter.VertexInputConverter;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.util.ResetableByteArrayInputStream;
import edu.uci.ics.pregelix.example.PageRankVertex2.StateWritable;
import edu.uci.ics.pregelix.example.data.VLongWritablePool;

public class MsgGraphInputVertexConverter implements VertexInputConverter {

    // Writables.
    private VLongWritable vertexId = new VLongWritable();
    private StateWritable vertexValue = new StateWritable();
    private VLongWritablePool pool = new VLongWritablePool();

    // Data Input.
    private ResetableByteArrayInputStream inputStream = new ResetableByteArrayInputStream();
    private DataInput dataInput = new DataInputStream(inputStream);

    @Override
    public void open(ARecordType recordType) {

    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void convert(ARecordVisitablePointable recordPointable, Vertex vertex) throws HyracksDataException {
        List<IVisitablePointable> fieldValues = recordPointable.getFieldValues();
        AFlatValuePointable vertexIdPointable = (AFlatValuePointable) fieldValues.get(0);
        AFlatValuePointable vertexIdentifierPointable = (AFlatValuePointable) fieldValues.get(1);
        AFlatValuePointable vertexValuePointable = (AFlatValuePointable) fieldValues.get(2);
        AListVisitablePointable edgeListPointable = (AListVisitablePointable) fieldValues.get(3);

        // Resets the vertex.
        vertex.getMsgList().clear();
        vertex.getEdges().clear();
        vertex.reset();

        // Sets up the vertex.
        setVertexId(vertexIdPointable, vertexId);
        setVertexValue(vertexValuePointable, vertexValue);
        vertex.setVertexId(vertexId);
        vertex.setVertexValue(vertexValue);
        setEdgeList(edgeListPointable, vertex);
    }

    @Override
    public void close() {

    }

    // Sets the value of vertex id from the AsterixDB pointable.
    private void setVertexId(AFlatValuePointable vertexIdPointable, VLongWritable vid) throws HyracksDataException {
        byte[] data = vertexIdPointable.getByteArray();
        int start = vertexIdPointable.getStartOffset() + 1; // Considers the AsterixDB type tag.
        long id = AInt64SerializerDeserializer.getLong(data, start);
        vid.set(id);
    }

    // Sets the vertex value from an AsterixDB pointable.
    private void setVertexValue(AFlatValuePointable vertexIdPointable, Writable v) throws HyracksDataException {
        byte[] data = vertexIdPointable.getByteArray();
        int start = vertexIdPointable.getStartOffset() + 1; // Considers the AsterixDB type tag.

    }

    // Sets the edge list from an AsterixDB AListPointable.
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void setEdgeList(AListVisitablePointable edgeListPointable, Vertex vertex) throws HyracksDataException {
        pool.reset();
        List<IVisitablePointable> items = edgeListPointable.getItems();
        for (int i = 0; i < items.size(); i++) {
            AFlatValuePointable destIdPointable = (AFlatValuePointable) items.get(i);
            VLongWritable destId = pool.allocate();
            this.setVertexId(destIdPointable, destId);
            vertex.addEdge(destId, null);
        }
    }

}
