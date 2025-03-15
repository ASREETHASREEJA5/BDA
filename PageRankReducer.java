import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PageRankReducer extends Reducer<Text, Text, Text, Text> {

    private static final double DAMPING_FACTOR = 0.85;
    private static final double BASE_RANK = 1.0;

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double sumPageRank = 0.0;
        List<String> neighbors = new ArrayList<>();
        double originalPageRank = 0.0;

        // Iterate over all the values that are passed for a specific node
        for (Text value : values) {
            String valueStr = value.toString();
            if (valueStr.startsWith("!")) {
                // This is the node's original information
                String[] parts = valueStr.substring(1).split(" ");
                originalPageRank = Double.parseDouble(parts[0]);
                for (int i = 1; i < parts.length; i++) {
                    neighbors.add(parts[i]);
                }
            } else {
                // This is the contribution from a neighboring node
                sumPageRank += Double.parseDouble(valueStr);
            }
        }

        // Calculate the new PageRank based on the sum of contributions
        double newPageRank = (1 - DAMPING_FACTOR) + DAMPING_FACTOR * sumPageRank;

        // Emit the node with its updated PageRank and the list of neighbors
        String result = newPageRank + " " + String.join(" ", neighbors);
        context.write(key, new Text(result));
    }
}

