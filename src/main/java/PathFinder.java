import domain.Path;
import domain.VertexValue;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

public class PathFinder extends BasicComputation<LongWritable, VertexValue, NullWritable, Path> {

    private boolean isOrganisationOrPerson(VertexValue vertex) {
        String type = vertex.getType();
        boolean isOrganisationOrPerson = false;
        if (type.equals("Sub") || type.equals("Obj")) {
            isOrganisationOrPerson = true;
        }
        return isOrganisationOrPerson;
    }

    @Override
    public void compute(Vertex<LongWritable, VertexValue, NullWritable> vertex, Iterable<Path> messages) throws IOException {
        System.out.println("[" + getSuperstep() + "] Checking type: " + vertex.toString());
        boolean isOrganisationOrPerson = isOrganisationOrPerson(vertex.getValue());

        // Broadcast messages at the beginning of the algorithm
        if (getSuperstep() == 0 && isOrganisationOrPerson) {
            Path path = new Path();
            path.append(vertex.getId().get());
            for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
                System.out.println("[" + getSuperstep() + "] " + vertex.getId().get() + " sending '" + path.toString() + "' to " + edge.getTargetVertexId());
                sendMessage(edge.getTargetVertexId(), path);
            }
        }

        // Store or forward all incoming paths
        for (Path path : messages) {
            System.out.println("[" + getSuperstep() + "] " + vertex.getId().get() + " received '" + path.toString() + "'");

            // Check for cycles
            boolean seenBefore = false;
            for (Long pathElement : path.getPathElements()) {
                if (pathElement == vertex.getId().get()) {
                    seenBefore = true;
                    System.out.println("Cycle detected");
                }
            }

            // Append this vertex to path
            if (!seenBefore) {
                path.append(vertex.getId().get());

                if (isOrganisationOrPerson) {
                    vertex.getValue().appendPath(path);
                }

                // Broadcast
                System.out.println("[" + getSuperstep() + "] " + vertex.getId().get() + " forwarding '" + path.toString() + "'");
                for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
                    System.out.println("[" + getSuperstep() + "] " + vertex.getId().get() + " forwarding '" + path.toString() + "' to " + edge.getTargetVertexId());
                    sendMessage(edge.getTargetVertexId(), path);
                }
            }
        }

        vertex.voteToHalt();
    }
}
