package domain;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Path implements Writable {
    List<Long> pathElements = new ArrayList<Long>();

    public int getLength() {
        return pathElements.size();
    }

    public List<Long> getPathElements() {
        return pathElements;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        System.err.println("Writing: " + pathElements);
        dataOutput.writeInt(pathElements.size());
        for (Long pathElement : pathElements) {
            dataOutput.writeLong(pathElement);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        pathElements.clear();
        int length = dataInput.readInt();
        for (int i = 0; i < length; i++) {
            pathElements.add(dataInput.readLong());
        }
        System.err.println("Read: " + pathElements);
    }

    public void append(Long pathElement) {
        System.out.println("Appending " + pathElement);
        pathElements.add(pathElement);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Long pathElement : pathElements) {
            sb.append(pathElement);
            sb.append("|");
        }
        sb.delete(sb.length() - 1, sb.length());
        return sb.toString();
    }
}
