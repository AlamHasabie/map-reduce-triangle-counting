import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SummerReducer
	extends Reducer<LongWritable,LongWritable,Text,LongWritable>
{
	private static long sum = 0;
	public void reduce(LongWritable k, Iterable<LongWritable> values, Context context)
		throws IOException, InterruptedException
	{
		for(LongWritable v : values)
		{
			sum += v.get();
		}
	}

	public void cleanup(Context context)
		throws IOException, InterruptedException
	{
		context.write(new Text("sum"), new LongWritable(sum));
	}
}