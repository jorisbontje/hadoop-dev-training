package tfidf;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class TfIdfStep1Mapper extends MapReduceBase implements
    Mapper<LongWritable, Text, TermDocIdWritable, LongWritable> {

  @Override
  public void map(LongWritable offset, Text value,
      OutputCollector<TermDocIdWritable, LongWritable> output, Reporter reporter)
      throws IOException {

	  FileSplit fileSplit = (FileSplit) reporter.getInputSplit();
	  String docId = fileSplit.getPath().getName();
	  
	  for (String term: value.toString().split("\\W+")) {
		  if (term.length() > 0) {
			  output.collect(new TermDocIdWritable(term, docId), new LongWritable(1L));
		  }
	  }
  }
}