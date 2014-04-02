package domain;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Path implements Writable {
    IntWritable length = new IntWritable();
    List<LongWritable> pathElements = new ArrayList<LongWritable>();

    public IntWritable getLength() {
        return length;
    }

    public List<LongWritable> getPathElements() {
        return pathElements;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        length.write(dataOutput);
        for (LongWritable pathElement : pathElements) {
            pathElement.write(dataOutput);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        length.readFields(dataInput);
        for (int i = 0; i < length.get(); i++) {
            LongWritable p = new LongWritable();
            p.readFields(dataInput);
            pathElements.add(p);
        }
    }

    public void append(LongWritable pathElement) {
        pathElements.add(pathElement);
        length.set(pathElements.size());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (LongWritable pathElement : pathElements) {
            sb.append(pathElement.get());
            sb.append("|");
        }
        sb.delete(sb.length() - 1, sb.length());
        return sb.toString();
    }
}
