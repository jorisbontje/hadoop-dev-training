package tfidf;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.MapDriver;
import org.junit.Before;
import org.junit.Test;

public class TfIdfStep1Test {
  MapDriver<LongWritable, Text, TermDocIdWritable, LongWritable> mapDriver;

  @Before
  public void setUp() {
    mapDriver = new MapDriver<LongWritable, Text, TermDocIdWritable, LongWritable>();
    mapDriver.setMapper(new TfIdfStep1Mapper());
  }

  @Test
  public void testMapper() {
    mapDriver.withInput(new LongWritable(0), new Text("How now brown cow"));
    mapDriver.withOutput(new TermDocIdWritable("How", "somefile"), new LongWritable(1));
    mapDriver.withOutput(new TermDocIdWritable("now", "somefile"), new LongWritable(1));
    mapDriver.withOutput(new TermDocIdWritable("brown", "somefile"), new LongWritable(1));
    mapDriver.withOutput(new TermDocIdWritable("cow", "somefile"), new LongWritable(1));
    mapDriver.runTest();
  }

}
