import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class PhaseThree
{
    public static void main(String[] args) throws Exception {

        Job job = Job.getInstance(new Configuration(), "Triangle Count - Phase 3");
        job.setNumReduceTasks(1);

        job.setJarByClass(PhaseThree.class);

        job.setMapperClass(SummerMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setCombinerClass(SummerCombiner.class);

        job.setReducerClass(SummerReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);    
    }
}