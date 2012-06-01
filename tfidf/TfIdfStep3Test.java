package tfidf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mrunit.MapDriver;
import org.junit.Before;
import org.junit.Test;

public class TfIdfStep3Test {
	MapDriver<TermDocIdWritable, TfNWritable, TermDocIdWritable, DoubleWritable> mapDriver;

	@Before
	public void setUp() {
		mapDriver = new MapDriver<TermDocIdWritable, TfNWritable, TermDocIdWritable, DoubleWritable>();
		mapDriver.setMapper(new TfIdfStep3Mapper());
	}

	@Test
	public void testMapper() {
		TermDocIdWritable termDocId = new TermDocIdWritable("How", "somefile");
		Configuration conf = new Configuration();
		conf.setLong("N", 3);
		mapDriver.withConfiguration(conf);
		mapDriver.withInput(termDocId,
				new TfNWritable(2, 1));
		mapDriver.withOutput(termDocId, new DoubleWritable(2.1972245773362196));
		mapDriver.runTest();
	}

}
