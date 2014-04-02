package io;

import domain.MyEdge;
import org.apache.commons.lang.StringUtils;
import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class LongStringTextEdgeInputFormat extends TextEdgeInputFormat<LongWritable, Text> {

    @Override
    public EdgeReader<LongWritable, Text> createEdgeReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
        return new LongStringTextEdgeReader();
    }

    public class LongStringTextEdgeReader extends TextEdgeReaderFromEachLineProcessed<MyEdge> {

        @Override
        protected MyEdge preprocessLine(Text text) throws IOException {
            String[] tokens = StringUtils.splitPreserveAllTokens(text.toString());
            return new MyEdge(
                    Long.valueOf(tokens[0]),
                    Long.valueOf(tokens[1]),
                    tokens[2]
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
        protected Text getValue(MyEdge myEdge) throws IOException {
            return new Text(myEdge.getType());
        }
    }

}
