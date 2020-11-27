import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ClosingEdgeMapper
    extends Mapper<Object, Text, SortedLongPairWritable, LongWritable>
{
	/** 
	Ughhhh, can we use serialization instead of string-like files ?
	I hate myself
	*/
	private static LongWritable node = new LongWritable(0);
	private static SortedLongPairWritable pair = new SortedLongPairWritable();

    public void map(Object key, Text value, Context context) 
        throws IOException, InterruptedException 
    {
    	String[] input, input_pair;
        input = value.toString().split("\\s");
        try {
            if (input.length == 2)
            {
                node.set(Long.parseLong(input[0]));
                input_pair = input[1].split(",");
                pair.set(Long.parseLong(input_pair[0]), Long.parseLong(input_pair[1]));
        		context.write(pair, node);
            }

        } catch (NumberFormatException e)
        {
            // do nothing
        }
    }
}