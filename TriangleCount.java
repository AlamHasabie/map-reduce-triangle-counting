import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TriangleCount
{
    private static final Path firstReduceTemp = new Path("/tmp/hadoop/output/1");
    private static final Path secondReduceTemp = new Path("/tmp/hadoop/output/2");

    public static void main(String[] args) throws Exception {

        boolean pipelineRunning;
        Job job = Job.getInstance(new Configuration(), "Triangle Count - Phase 1");

        job.setJarByClass(TriangleCount.class);

        job.setMapperClass(EdgeMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setReducerClass(LengthTwoReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(SortedLongPairWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Job job2 = Job.getInstance(new Configuration(), "Triangle Count - Phase 2");

        job2.setJarByClass(TriangleCount.class);

        job2.setMapperClass(ClosingEdgeMapper.class);
        job2.setMapOutputKeyClass(SortedLongPairWritable.class);
        job2.setMapOutputValueClass(LongWritable.class);

        job2.setReducerClass(ClosingEdgeReducer.class);
        job2.setOutputKeyClass(LongWritable.class);
        job2.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        Job job3 = new Job(new Configuration(), "Triangle Count - Summarizer");
        job3.setNumReduceTasks(1);

        job3.setJarByClass(TriangleCount.class);

        job3.setMapperClass(SummerMapper.class);
        job3.setMapOutputKeyClass(LongWritable.class);
        job3.setMapOutputValueClass(LongWritable.class);

        job3.setCombinerClass(SummerCombiner.class);

        job3.setReducerClass(SummerReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job3, new Path(args[2]));
        FileOutputFormat.setOutputPath(job3, new Path(args[3]));

        pipelineRunning = job.waitForCompletion(true);

        if(pipelineRunning) 
        {
            pipelineRunning = job2.waitForCompletion(true);
        
        } else 
        {
            System.out.println("Error during running first job");
            System.exit(1);
        }

        if(pipelineRunning)
        {
            pipelineRunning = job3.waitForCompletion(true);
        } else 
        {
            System.out.println("Error during running second job");
            System.exit(1);
        }

        if(!pipelineRunning)
        {
            System.out.println("Error during running third job");
        }

        System.exit(pipelineRunning ? 0 : 1);
    }
}