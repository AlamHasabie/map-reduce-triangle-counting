import java.io.IOException;
import java.io.DataInput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;

/** ascending sorted pair */
public class SortedLongPairWritable
	extends LongPairWritable
{
	public SortedLongPairWritable(){}

	public SortedLongPairWritable(LongWritable first, LongWritable second)
	{
		super(first, second);
		sort();
	}

	@Override
	public void set(Long l, Long r)
	{
		left.set(l);
		right.set(r);
		sort();
	}

	@Override
	public void setLeft(Long l)
	{
		super.setLeft(l);
		sort();
	}

	@Override
	public void setRight(Long r)
	{
		super.setRight(r);
		sort();
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		super.readFields(in);
		sort();
	}

	private void sort()
	{
		if(left.compareTo(right) == 1)
		{
			Long temp = left.get();
			left.set(right.get());
			right.set(temp);
		}
	}
}