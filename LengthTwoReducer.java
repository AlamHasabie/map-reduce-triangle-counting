import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class LengthTwoReducer
    extends Reducer<LongWritable,LongWritable,LongWritable,SortedLongPairWritable> 
{
    private SortedLongPairWritable pair = new SortedLongPairWritable();

    public void reduce(LongWritable v, Iterable<LongWritable> values, Context context) 
        throws IOException, InterruptedException 
    {
        ArrayList<Long> valueArray = new ArrayList<>();
        for (LongWritable i : values)
        {
            System.out.println(i);
            valueArray.add(i.get());
        }

        for (int i = 0 ; i < valueArray.size(); i++) 
        {
            for (int j = i + 1 ; j < valueArray.size(); j++)
            {
                /** To reduce memory usage, we avoid storing bidirectional edge */
                /** Instead, enforce id-based natural ordering */
                pair.set(valueArray.get(i), valueArray.get(j));
                System.out.println(pair);
                context.write(v, pair);
            }
        }
    }
}