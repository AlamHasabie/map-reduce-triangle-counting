import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SummerMapper
	extends Mapper<Object, Text, LongWritable, LongWritable>
{

	private static LongWritable node = new LongWritable(0);
	private static LongWritable count = new LongWritable(0);

	public void map(Object key, Text value, Context context) 
    	throws IOException, InterruptedException
    {
    	String[] input = value.toString().split("\\s");
	    try {
            if (input.length == 2)
            {
            	node.set(Long.parseLong(input[0]));
            	count.set(Long.parseLong(input[1]));
                context.write(node, count);
            }
        } catch (NumberFormatException e)
        {
            // do nothing
        }

    }

}