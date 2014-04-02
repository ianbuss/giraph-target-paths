package domain;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class VertexValue implements Writable {
    private Text type;
    private Text value;
    private List<Path> cachedPaths = new ArrayList<Path>();

    public VertexValue() {}

    public VertexValue(String type, String value) {
        this.type = new Text(type);
        this.value = new Text(value);
        System.out.println(this.toString());
    }

    public String getType() {
        return type.toString();
    }

    public String getValue() {
        return value.toString();
    }

    public void appendPath(Path path) {
        cachedPaths.add(path);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{type='" + type + '\'' + ",value='" + value + '\'' + "}\n");
        for (Path path : cachedPaths) {
            sb.append(path.toString());
            sb.append("\n");
        }
        return sb.toString();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        type.write(dataOutput);
        value.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        type = new Text();
        type.readFields(dataInput);
        value = new Text();
        value.readFields(dataInput);
    }
}