import org.apache.hadoop.io.LongWritable;

/** ascending sorted pair */
public class SortedLongPairWritable
	extends LongPairWritable
{
	public SortedLongPairWritable(){}

	public SortedLongPairWritable(LongWritable first, LongWritable second)
	{
		super(first, second);
		order();
	}

	@Override
	public void set(Long l, Long r)
	{
		super.set(l, r);
		order();
	}

	private void order()
	{
		if(left.compareTo(right) == 1)
		{
			Long temp = left.get();
			left.set(right.get());
			right.set(temp);
		}
	}
}