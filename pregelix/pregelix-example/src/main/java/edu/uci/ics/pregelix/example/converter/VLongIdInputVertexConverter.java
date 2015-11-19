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
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;

import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt8SerializerDeserializer;
import org.apache.asterix.om.pointables.AFlatValuePointable;
import org.apache.asterix.om.pointables.AListVisitablePointable;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.pregelix.api.converter.VertexInputConverter;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.util.ResetableByteArrayInputStream;
import edu.uci.ics.pregelix.example.data.VLongWritablePool;

public class VLongIdInputVertexConverter implements VertexInputConverter {

    // Field indexes.
    private int vertexIdFieldIndex = -1;
    private int vertexValueFieldIndex = -1;
    private int edgeListIndex = -1;

    // Writables.
    private VLongWritable vertexId = new VLongWritable();
    private Writable vertexValue;
    private VLongWritablePool pool = new VLongWritablePool();

    // Type tags.
    private ATypeTag vertexIdTypeTag;
    private ATypeTag vertexValueTypeTag;

    // Data Input.
    private ResetableByteArrayInputStream inputStream = new ResetableByteArrayInputStream();
    private DataInput dataInput = new DataInputStream(inputStream);

    @Override
    public void open(ARecordType recordType) {
        setFieldPermutation(recordType);
        initializeVertexValue(vertexValueTypeTag);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void convert(ARecordVisitablePointable recordPointable, Vertex vertex) throws HyracksDataException {
        List<IVisitablePointable> fieldValues = recordPointable.getFieldValues();
        AFlatValuePointable vertexIdPointable = (AFlatValuePointable) fieldValues.get(vertexIdFieldIndex);
        AFlatValuePointable vertexValuePointable = (AFlatValuePointable) fieldValues.get(vertexValueFieldIndex);
        AListVisitablePointable edgeListPointable = (AListVisitablePointable) fieldValues.get(edgeListIndex);

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

    private void setFieldPermutation(ARecordType recordType) {
        IAType[] fieldTypes = recordType.getFieldTypes();
        for (int i = 0; i < fieldTypes.length; i++) {
            if (vertexIdFieldIndex >= 0 && vertexValueFieldIndex >= 0 && edgeListIndex >= 0) {
                break;
            }
            if (vertexIdFieldIndex == -1 && isIntegerType(fieldTypes[i].getTypeTag())) {
                vertexIdFieldIndex = i;
            } else if (edgeListIndex == -1 && isCollectionType(fieldTypes[i].getTypeTag())) {
                edgeListIndex = i;
            } else if (vertexValueFieldIndex == -1 && isPrimitiveType(fieldTypes[i].getTypeTag())) {
                vertexValueFieldIndex = i;
            }
        }
        vertexIdTypeTag = fieldTypes[vertexIdFieldIndex].getTypeTag();
        vertexValueTypeTag = fieldTypes[vertexValueFieldIndex].getTypeTag();
    }

    private boolean isIntegerType(ATypeTag typeTag) {
        return typeTag == ATypeTag.INT8 || typeTag == ATypeTag.INT16 || typeTag == ATypeTag.INT32
                || typeTag == ATypeTag.INT64 || typeTag == ATypeTag.STRING;
    }

    private boolean isCollectionType(ATypeTag typeTag) {
        return typeTag == ATypeTag.ORDEREDLIST || typeTag == ATypeTag.UNORDEREDLIST;
    }

    private boolean isRecordType(ATypeTag typeTag) {
        return typeTag == ATypeTag.RECORD;
    }

    private boolean isPrimitiveType(ATypeTag typeTag) {
        return !isCollectionType(typeTag) && !isRecordType(typeTag);
    }

    // Sets the value of vertex id from the AsterixDB pointable.
    private void setVertexId(AFlatValuePointable vertexIdPointable, VLongWritable vid) throws HyracksDataException {
        byte[] data = vertexIdPointable.getByteArray();
        int start = vertexIdPointable.getStartOffset() + 1; // Considers the AsterixDB type tag.
        switch (vertexIdTypeTag) {
            case INT8: {
                long id = AInt8SerializerDeserializer.getByte(data, start);
                vid.set(id);
                break;
            }
            case INT16: {
                long id = AInt16SerializerDeserializer.getShort(data, start);
                vid.set(id);
                break;
            }
            case INT32: {
                long id = AInt32SerializerDeserializer.getInt(data, start);
                vid.set(id);
                break;
            }
            case INT64: {
                long id = AInt64SerializerDeserializer.getLong(data, start);
                vid.set(id);
                break;
            }
            case STRING: {
                inputStream.setByteArray(data, start);
                try {
                    vid.set(Long.parseLong(dataInput.readUTF()));
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
                break;
            }
            default: {
                throw new NotImplementedException("No printer for type " + vertexIdTypeTag);
            }
        }
    }

    // Sets the vertex value from an AsterixDB pointable.
    private void setVertexValue(AFlatValuePointable vertexIdPointable, Writable v) throws HyracksDataException {
        byte[] data = vertexIdPointable.getByteArray();
        int start = vertexIdPointable.getStartOffset() + 1; // Considers the AsterixDB type tag.
        switch (vertexValueTypeTag) {
            case INT8: {
                long value = AInt8SerializerDeserializer.getByte(data, start);
                ((VLongWritable) v).set(value);
                break;
            }
            case INT16: {
                long value = AInt16SerializerDeserializer.getShort(data, start);
                ((VLongWritable) v).set(value);
                break;
            }
            case INT32: {
                long value = AInt32SerializerDeserializer.getInt(data, start);
                ((VLongWritable) v).set(value);
                break;
            }
            case INT64: {
                long value = AInt64SerializerDeserializer.getLong(data, start);
                ((VLongWritable) v).set(value);
                break;
            }
            case FLOAT: {
                float value = AFloatSerializerDeserializer.getFloat(data, start);
                ((FloatWritable) v).set(value);
                break;
            }
            case DOUBLE: {
                double value = ADoubleSerializerDeserializer.getDouble(data, start);
                ((DoubleWritable) v).set(value);
                break;
            }
            case STRING: {
                inputStream.setByteArray(data, start);
                try {
                    ((Text) v).set(dataInput.readUTF());
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
                break;
            }
            default: {
                throw new NotImplementedException("No printer for type " + vertexIdTypeTag);
            }
        }
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

    // Initialize the vertex value object.
    private void initializeVertexValue(ATypeTag typeTag) {
        switch (typeTag) {
            case INT8:
            case INT16:
            case INT32:
            case INT64: {
                vertexValue = new VLongWritable();
                break;
            }
            case BOOLEAN: {
                vertexValue = new BooleanWritable();
                break;
            }
            case FLOAT: {
                vertexValue = new FloatWritable();
                break;
            }
            case DOUBLE: {
                vertexValue = new DoubleWritable();
                break;
            }
            case STRING: {
                vertexValue = new Text();
                break;
            }
            default: {
                throw new NotImplementedException("Not able to deal with AsterixDB type " + typeTag);
            }
        }
    }

}
