import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.LongWritable;

public class LongPairWritable implements
	WritableComparable<LongPairWritable>
{
	protected LongWritable left;
	protected LongWritable right;

    // Empty constructor
    public LongPairWritable()
    {
        left = new LongWritable(0);
        right = new LongWritable(0);
    }

	public LongPairWritable(LongWritable first, LongWritable second)
	{
		left = first;
		right = second;
	}

	@Override
    public void readFields(DataInput in) throws IOException {
        left.readFields(in);
        right.readFields(in);
    }
 
    @Override
    public void write(DataOutput out) throws IOException 
    {
        left.write(out);
        right.write(out);
    }

    @Override
    public String toString() 
    {
        return left + "," + right;
    }

    @Override
    // Arbitrary hash code
    public int hashCode()
    {
        return left.hashCode()*43 + right.hashCode();
    }

    public void set(Long l, Long r)
    {
        setLeft(l);
        setRight(r);
    }
    public void setLeft(Long l)
    {
        left.set(l);
    }

    public void setRight(Long r)
    {
        right.set(r);
    }

    @Override
    public int compareTo(LongPairWritable pw) 
    {
    	int left_compare = left.compareTo(pw.left);
    	if(left_compare == 0)
    	{
    		return right.compareTo(pw.right);
    	}

    	return left_compare;
    }

    @Override
    public boolean equals(Object o)
    {
    	if(o instanceof LongPairWritable)
    	{
    		LongPairWritable pw = (LongPairWritable) o;
    		return left.equals(pw.left) && right.equals(pw.right);
    	}
    	return false;
    }
}