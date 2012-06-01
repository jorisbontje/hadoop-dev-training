package tfidf;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class TfIdfStep2Reducer extends MapReduceBase implements
    Reducer<Text, DocIdTfWritable, TermDocIdWritable, TfNWritable> {

  @Override
  public void reduce(Text term, Iterator<DocIdTfWritable> values,
      OutputCollector<TermDocIdWritable, TfNWritable> output, Reporter reporter)
      throws IOException {
	  
	  List<DocIdTfWritable> buffer = new ArrayList<DocIdTfWritable>();
	  long n = 0;
	  
	  while(values.hasNext()) {
		  DocIdTfWritable value = values.next();
		  n++;
		  buffer.add(new DocIdTfWritable(value.docId, value.tf));
	  }
	  
      for (DocIdTfWritable value : buffer) {
    	  output.collect(new TermDocIdWritable(term.toString(), value.docId) ,
    			  new TfNWritable(value.tf, n));
      }
  }
}
