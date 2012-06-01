package tfidf;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class TfIdfStep2Mapper extends MapReduceBase implements
    Mapper<TermDocIdWritable, LongWritable, Text, DocIdTfWritable> {

  @Override
  public void map(TermDocIdWritable termDocId, LongWritable tf,
      OutputCollector<Text, DocIdTfWritable> output, Reporter reporter)
      throws IOException {
	  
	  output.collect(new Text(termDocId.term), new DocIdTfWritable(termDocId.docId, tf.get()));
  }
}