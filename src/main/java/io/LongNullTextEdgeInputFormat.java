package io;

import domain.MyEdge;
import org.apache.commons.lang.StringUtils;
import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class LongNullTextEdgeInputFormat extends TextEdgeInputFormat<LongWritable, NullWritable> {

    @Override
    public EdgeReader<LongWritable, NullWritable> createEdgeReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
        return new LongStringTextEdgeReader();
    }

    public class LongStringTextEdgeReader extends TextEdgeReaderFromEachLineProcessed<MyEdge> {

        @Override
        protected MyEdge preprocessLine(Text text) throws IOException {
            String[] tokens = StringUtils.splitPreserveAllTokens(text.toString());
            return new MyEdge(
                    Long.valueOf(tokens[0]),
                    Long.valueOf(tokens[1])
            );
        }

        @Override
        protected LongWritable getTargetVertexId(MyEdge myEdge) throws IOException {
            return new LongWritable(myEdge.getTarget());
        }

        @Override
        protected LongWritable getSourceVertexId(MyEdge myEdge) throws IOException {
            return new LongWritable(myEdge.getSource());
        }

        @Override
        protected NullWritable getValue(MyEdge myEdge) throws IOException {
            return NullWritable.get();
        }
    }

}
