import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRankDriver {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // Set the number of iterations
        int numIterations = 10;

        // Input and output paths
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        // Loop through the iterations
        for (int i = 0; i < numIterations; i++) {
            Job job = Job.getInstance(conf, "PageRank Iteration " + i);
            job.setJarByClass(PageRankDriver.class);

            // Set the mapper and reducer classes
            job.setMapperClass(PageRankMapper.class);
            job.setReducerClass(PageRankReducer.class);

            // Set the output key and value types
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            // Set input and output paths
            FileInputFormat.addInputPath(job, i == 0 ? inputPath : outputPath);
            Path outputDir = new Path(outputPath + "_iter_" + i);
            FileOutputFormat.setOutputPath(job, outputDir);

            // Run the job
            if (!job.waitForCompletion(true)) {
                System.exit(1);
            }

            // Prepare for next iteration
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(outputPath)) {
                fs.delete(outputPath, true);
            }
            fs.rename(outputDir, outputPath);
        }
    }
}

