import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class PageRankMapper extends Mapper<Object, Text, Text, Text> {

    private static final double DAMPING_FACTOR = 0.85;

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // The value is expected to be a line of the form: "node <PR_value> <linked_nodes>"
        String line = value.toString();
        String[] parts = line.split("\t");
        String node = parts[0];
        String[] rest = parts[1].split(" ");
        double pageRank = Double.parseDouble(rest[0]);
        String[] neighbors = new String[rest.length - 1];
        System.arraycopy(rest, 1, neighbors, 0, rest.length - 1);

        // Calculate the distributed PageRank value for each neighbor
        int numLinks = neighbors.length;
        double distributedPR = pageRank / numLinks;

        // Emit the contribution to each neighbor
        for (String neighbor : neighbors) {
            context.write(new Text(neighbor), new Text(Double.toString(distributedPR)));
        }

        // Emit the node with its current PageRank and its list of neighbors for the reducer
        context.write(new Text(node), new Text("!" + pageRank + " " + String.join(" ", neighbors)));
    }
}

