package tfidf;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class TfIdfStep3Mapper extends MapReduceBase implements
    Mapper<TermDocIdWritable, TfNWritable, TermDocIdWritable, DoubleWritable> {
	
	private long N = 0;
	
	@Override
	public void configure(JobConf job) {
		N = job.getLong("N", 0);
	}
		
  @Override
  public void map(TermDocIdWritable termDocId, TfNWritable tfN,
      OutputCollector<TermDocIdWritable, DoubleWritable> output, Reporter reporter)
      throws IOException {
	  
	  double tfIdf = tfN.tf * Math.log(N / tfN.n);
	  
	  output.collect(termDocId, new DoubleWritable(tfIdf));
  }
}