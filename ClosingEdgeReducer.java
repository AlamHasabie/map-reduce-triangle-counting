import java.util.*;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class ClosingEdgeReducer
    extends Reducer<SortedLongPairWritable,LongWritable,LongWritable,LongWritable> 
{
    private static final LongWritable one = new LongWritable(1);
    private static LongWritable temp = new LongWritable();
    private Set<Long> set;

    private void flush(Context context)
        throws IOException, InterruptedException
    {
        for(Long l : set)
        {
            temp.set(l);
            context.write(temp, one);
        }
    }
    public void reduce(SortedLongPairWritable v, Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException 
    {
        set = new HashSet<>();
        boolean isFlagFound = false;
        Long content;

        for(LongWritable i : values)
        {
            content = i.get();
            if((content == -1)&&(!isFlagFound))
            {
                isFlagFound = true;
                flush(context);
            }

            if((content!=-1)&&isFlagFound)
            {
                if(set.add(content))
                {
                    temp.set(content);
                    context.write(temp, one);
                }
            } else 
            {
                set.add(content);
            }
        }

    }
}