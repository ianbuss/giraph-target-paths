# Giraph Target Paths

## Running it

 hadoop jar giraph/giraph-testing-1.0-SNAPSHOT.jar org.apache.giraph.GiraphRunner \
  -Dgiraph.zkList=cdh4-vm:2181 \
  -libjars giraph/giraph-core.jar algos.PathFinder \
  -vif io.LongVertexValueTextVertexValueInputFormat \
  -vip path/to/vertex/data \
  -eif io.LongNullTextEdgeInputFormat \
  -eip path/to/edge/data \
  -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat \
  -op path/to/output\
  -w 1 \
  -ca PathFinder.targets=Sub:Obj

 The final `ca` parameter defines the types of vertices which will be the beginning and end points of paths