package tfidf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class DocIdTfWritable implements WritableComparable<DocIdTfWritable> {

	public String docId;
	public long tf;

	/* empty constructor required for serialization */
	public DocIdTfWritable() {

	}

	public DocIdTfWritable(String docId, long tf) {
		this.docId = docId;
		this.tf = tf;
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(docId);
		out.writeLong(tf);
	}

	public void readFields(DataInput in) throws IOException {
		docId = in.readUTF();
		tf = in.readLong();
	}

	public int compareTo(DocIdTfWritable other) {
		int ret = docId.compareTo(other.docId);
		if (ret == 0) {
			return (tf<other.tf ? -1 : (tf==other.tf ? 0 : 1));
		}
		return ret;
	}

	public String toString() {
		return "(" + docId + "," + tf + ")";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((docId == null) ? 0 : docId.hashCode());
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
		DocIdTfWritable other = (DocIdTfWritable) obj;
		if (docId == null) {
			if (other.docId != null)
				return false;
		} else if (!docId.equals(other.docId))
			return false;
		if (tf != other.tf)
			return false;
		return true;
	}

}
