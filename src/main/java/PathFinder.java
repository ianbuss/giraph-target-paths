import domain.MyVertex;
import domain.Path;
import domain.PathElement;
import domain.VertexValue;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class PathFinder extends BasicComputation<LongWritable, VertexValue, Text, Path> {

    private boolean isOrganisationOrPerson(VertexValue vertex) {
        String type = vertex.getType();
        boolean isOrganisationOrPerson = false;
        if (type.equals("Sub") || type.equals("Obj")) {
            isOrganisationOrPerson = true;
        }
        return isOrganisationOrPerson;
    }

    @Override
    public void compute(Vertex<LongWritable, VertexValue, Text> vertex, Iterable<Path> messages) throws IOException {
        boolean isOrganisationOrPerson = isOrganisationOrPerson(vertex.getValue());

        // Broadcast messages at the beginning of the algorithm
        if (getSuperstep() == 0 && isOrganisationOrPerson) {
            LongWritable pathElement = new LongWritable();
            pathElement.set(vertex.getId().get());
            Path path = new Path();
            path.append(pathElement);

            for (Edge<LongWritable, Text> edge : vertex.getEdges()) {
                sendMessage(edge.getTargetVertexId(), path);
            }
        }

        // Store or forward all incoming paths
        for (Path path : messages) {
            // Check for cycles
            boolean seenBefore = false;
            for (LongWritable pathElement : path.getPathElements()) {
                if (pathElement.get() == vertex.getId().get()) {
                    seenBefore = true;
                }
            }

            // Append this vertex to path
            if (!seenBefore) {
                LongWritable vertexElement = new LongWritable();
                vertexElement.set(vertex.getId().get());
                path.append(vertexElement);

                if (isOrganisationOrPerson) {
                    vertex.getValue().appendPath(path);
                }

                // Broadcast
                for (Edge<LongWritable, Text> edge : vertex.getEdges()) {
                    sendMessage(edge.getTargetVertexId(), path);
                }
            }
        }

        vertex.voteToHalt();
    }
}
