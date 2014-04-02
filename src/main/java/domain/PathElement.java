package domain;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PathElement implements Writable {

    public enum Type {
        VERTEX,
        EDGE
    }

    private Type type;
    private LongWritable id = new LongWritable();

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public LongWritable getId() {
        return id;
    }

    public void setId(LongWritable id) {
        this.id = id;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeEnum(dataOutput, type);
        id.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        type = WritableUtils.readEnum(dataInput, Type.class);
        id.readFields(dataInput);
    }

}
