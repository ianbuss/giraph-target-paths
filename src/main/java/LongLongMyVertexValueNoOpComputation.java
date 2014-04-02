import domain.MyVertex;
import domain.VertexValue;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

/**
 * Computation which does nothing, used for testing. Vertex ids and values
 * are integers, edge values and messages are nulls.
 */
public class LongLongMyVertexValueNoOpComputation extends
        NoOpComputation<LongWritable, VertexValue, Text, Text> {
}
