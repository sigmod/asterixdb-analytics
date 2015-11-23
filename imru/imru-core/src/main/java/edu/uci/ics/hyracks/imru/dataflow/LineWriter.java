//package edu.uci.ics.hyracks.imru.dataflow;
//
//import java.io.IOException;
//
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//
//import org.apache.hyracks.api.comm.IFrameWriter;
//import org.apache.hyracks.api.context.IHyracksTaskContext;
//import org.apache.hyracks.api.exceptions.HyracksDataException;
//import org.apache.hyracks.hdfs.api.IKeyValueParser;
//import org.apache.hyracks.hdfs.api.IKeyValueParserFactory;
//import edu.uci.ics.hyracks.imru.api.TupleWriter;
//import edu.uci.ics.hyracks.imru.util.Rt;
//
//public class LineWriter implements IKeyValueParserFactory<LongWritable, Text> {
//    @Override
//    public IKeyValueParser<LongWritable, Text> createKeyValueParser(
//            final IHyracksTaskContext ctx) {
//        return new IKeyValueParser<LongWritable, Text>() {
//            TupleWriter tupleWriter;
//
//            @Override
//            public void open(IFrameWriter writer) {
//                tupleWriter = new TupleWriter(ctx, writer,
//                        1);
//            }
//
//            @Override
//            public void parse(LongWritable key, Text value,
//                    IFrameWriter writer)
//                    throws HyracksDataException {
//                try {
//                    tupleWriter.write(value.getBytes(), 0,
//                            value.getLength());
//                    tupleWriter.finishField();
//                    tupleWriter.finishTuple();
//                } catch (IOException e) {
//                    throw new HyracksDataException(e);
//                }
//            }
//
//            @Override
//            public void close(IFrameWriter writer)
//                    throws HyracksDataException {
//                tupleWriter.close();
//            }
//        };
//    }
//}