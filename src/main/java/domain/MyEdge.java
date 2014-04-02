package domain;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MyEdge implements Writable {

    private long source;
    private long target;

    public MyEdge() {}

    public MyEdge(long source, long target) {
        this.source = source;
        this.target = target;
    }

    public long getSource() {
        return source;
    }

    public long getTarget() {
        return target;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(source);
        dataOutput.writeLong(target);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        source = dataInput.readLong();
        target = dataInput.readLong();
    }
}
