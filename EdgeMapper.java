import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class EdgeMapper
    extends Mapper<Object, Text, LongWritable, LongWritable>
{
    private LongWritable node1 = new LongWritable();
    private LongWritable node2 = new LongWritable();
    private long node1_int, node2_int;

    public void map(Object key, Text value, Context context) 
        throws IOException, InterruptedException 
    {
        String[] input = value.toString().split("\\s");
        try {
            if (input.length == 2)
            {
                node1_int = Long.parseLong(input[0]);
                node2_int = Long.parseLong(input[1]);
                if(node1_int > node2_int)
                {
                    node1.set(node2_int);
                    node2.set(node1_int);
                } else 
                {
                    node1.set(node1_int);
                    node2.set(node2_int);
                }
                context.write(node1, node2);
            }

        } catch (NumberFormatException e)
        {
            // do nothing
        }
    }
}