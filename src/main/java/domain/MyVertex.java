package domain;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MyVertex implements Writable {

    private long id;
    private VertexValue value = new VertexValue();

    public MyVertex() {}

    public MyVertex(long id, VertexValue value) {
        this.id = id;
        this.value = value;
    }

    public long getId() {
        return id;
    }

    public VertexValue getValue() {
        return value;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(id);
        value.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        id = dataInput.readLong();
        value.readFields(dataInput);
    }
}
