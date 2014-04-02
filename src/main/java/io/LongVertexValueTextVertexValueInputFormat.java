package io;

import domain.MyVertex;
import domain.VertexValue;
import org.apache.commons.lang.StringUtils;
import org.apache.giraph.io.formats.TextVertexValueInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class LongVertexValueTextVertexValueInputFormat<E extends Writable> extends TextVertexValueInputFormat<LongWritable, VertexValue, E> {
    @Override
    public TextVertexValueReader createVertexValueReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
        return new LongStringVertexValueReader();
    }

    public class LongStringVertexValueReader extends TextVertexValueReaderFromEachLineProcessed<MyVertex> {

        @Override
        protected MyVertex preprocessLine(Text text) throws IOException {
            String[] tokens = StringUtils.splitPreserveAllTokens(text.toString());
            return new MyVertex(
                    Long.valueOf(tokens[0]),
                    new VertexValue(tokens[1], tokens[2])
            );
        }

        @Override
        protected LongWritable getId(MyVertex myVertex) throws IOException {
            return new LongWritable(myVertex.getId());
        }

        @Override
        protected VertexValue getValue(MyVertex myVertex) throws IOException {
            return myVertex.getValue();
        }
    }
}
