import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class PhaseOne
{
    public static void main(String[] args) throws Exception {

        Job job = Job.getInstance(new Configuration(), "Triangle Count - Phase 1");

        job.setJarByClass(PhaseOne.class);

        job.setMapperClass(EdgeMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setReducerClass(LengthTwoReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(SortedLongPairWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}