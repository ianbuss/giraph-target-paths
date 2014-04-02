package domain;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MyEdge implements Writable {

    private LongWritable source = new LongWritable();
    private LongWritable target = new LongWritable();
    private Text type = new Text();

    public MyEdge(long source, long target, String type) {
        this.type.set(type);
        this.source.set(source);
        this.target.set(target);
    }

    public long getSource() {
        return source.get();
    }

    public long getTarget() {
        return target.get();
    }

    public String getType() {
        return type.toString();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        source.write(dataOutput);
        target.write(dataOutput);
        type.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        source.readFields(dataInput);
        target.readFields(dataInput);
        type.readFields(dataInput);
    }
}
