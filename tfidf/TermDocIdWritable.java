package tfidf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class TermDocIdWritable implements WritableComparable<TermDocIdWritable> {

	public String term;
	public String docId;

	/* empty constructor required for serialization */
	public TermDocIdWritable() {

	}

	public TermDocIdWritable(String term, String docId) {
		this.term = term;
		this.docId = docId;
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(term);
		out.writeUTF(docId);
	}

	public void readFields(DataInput in) throws IOException {
		term = in.readUTF();
		docId = in.readUTF();
	}

	public int compareTo(TermDocIdWritable other) {
		int ret = term.compareTo(other.term);
		if (ret == 0) {
			return docId.compareTo(other.docId);
		}
		return ret;
	}

	public String toString() {
		return "(" + term + "," + docId + ")";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((docId == null) ? 0 : docId.hashCode());
		result = prime * result + ((term == null) ? 0 : term.hashCode());
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
		TermDocIdWritable other = (TermDocIdWritable) obj;
		if (docId == null) {
			if (other.docId != null)
				return false;
		} else if (!docId.equals(other.docId))
			return false;
		if (term == null) {
			if (other.term != null)
				return false;
		} else if (!term.equals(other.term))
			return false;
		return true;
	}

	

}
