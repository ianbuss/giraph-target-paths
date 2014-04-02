package domain;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by ianbuss on 01/04/2014.
 */
public class MyVertex implements Writable {

    private LongWritable id = new LongWritable();
    private VertexValue value = new VertexValue();

    public MyVertex() {}

    public MyVertex(long id, VertexValue value) {
        this.id.set(id);
        this.value = value;
    }

    public long getId() {
        return id.get();
    }

    public VertexValue getValue() {
        return value;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        id.write(dataOutput);
        value.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        id.readFields(dataInput);
        value.readFields(dataInput);
    }
}
