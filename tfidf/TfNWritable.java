package tfidf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class TfNWritable implements WritableComparable<TfNWritable> {

	public long tf;
	public long n;

	/* empty constructor required for serialization */
	public TfNWritable() {

	}

	public TfNWritable(long tf, long n) {
		this.tf = tf;
		this.n = n;
	}

	public void write(DataOutput out) throws IOException {
		out.writeLong(tf);
		out.writeLong(n);
	}

	public void readFields(DataInput in) throws IOException {
		tf = in.readLong();
		n = in.readLong();
	}

	public int compareTo(TfNWritable other) {
		int ret = (tf<other.tf ? -1 : (tf==other.tf ? 0 : 1));
		if (ret == 0) {
			return (n<other.n ? -1 : (n==other.n ? 0 : 1));
		}
		return ret;
	}

	public String toString() {
		return "(" + tf + "," + n + ")";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (n ^ (n >>> 32));
		result = prime * result + (int) (tf ^ (tf >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TfNWritable other = (TfNWritable) obj;
		if (n != other.n)
			return false;
		if (tf != other.tf)
			return false;
		return true;
	}
	
}
