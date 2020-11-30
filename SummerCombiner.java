import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class SummerCombiner
    extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable>
{
    	public void reduce(LongWritable k, Iterable<LongWritable> values, Context context)
    		throws IOException, InterruptedException 
        {
        	long sum = 0;
        	for (LongWritable v : values)
        	{
        		sum += v.get();
        	}
        	context.write(k, new LongWritable(sum));
        }
}