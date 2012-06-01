package tfidf;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.MapReduceDriver;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class TfIdfStep2Test {
	MapDriver<TermDocIdWritable, LongWritable, Text, DocIdTfWritable> mapDriver;
	ReduceDriver<Text, DocIdTfWritable, TermDocIdWritable, TfNWritable> reduceDriver;
	MapReduceDriver<TermDocIdWritable, LongWritable, Text, DocIdTfWritable, TermDocIdWritable, TfNWritable> mapReduceDriver;

	
	@Before
	public void setUp() {
		mapDriver = new MapDriver<TermDocIdWritable, LongWritable, Text, DocIdTfWritable>();
		mapDriver.setMapper(new TfIdfStep2Mapper());
		reduceDriver = new ReduceDriver<Text, DocIdTfWritable, TermDocIdWritable, TfNWritable>();
		reduceDriver.setReducer(new TfIdfStep2Reducer());
		mapReduceDriver = new MapReduceDriver<TermDocIdWritable, LongWritable, Text, DocIdTfWritable, TermDocIdWritable, TfNWritable>();
		mapReduceDriver.setMapper(new TfIdfStep2Mapper());
		mapReduceDriver.setReducer(new TfIdfStep2Reducer());
	}

	@Test
	public void testMapper() {
		mapDriver.withInput(new TermDocIdWritable("How", "somefile"),
				new LongWritable(2));
		mapDriver.withOutput(new Text("How"),
				new DocIdTfWritable("somefile", 2));
		mapDriver.runTest();
	}

	@Test
	public void testReducer() {
		List<DocIdTfWritable> values = new ArrayList<DocIdTfWritable>();
		values.add(new DocIdTfWritable("somefile", 2L));
		values.add(new DocIdTfWritable("otherfile", 1L));
		reduceDriver.withInput(new Text("How"), values);
		reduceDriver.withOutput(new TermDocIdWritable("How", "somefile"),
				new TfNWritable(2, 2));
		reduceDriver.withOutput(new TermDocIdWritable("How", "otherfile"),
				new TfNWritable(1, 2));
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReduce() {
		mapReduceDriver.withInput(new TermDocIdWritable("How", "somefile"),
				new LongWritable(2));
		mapReduceDriver.withInput(new TermDocIdWritable("How", "otherfile"),
				new LongWritable(1));
		mapReduceDriver.withOutput(new TermDocIdWritable("How", "somefile"),
				new TfNWritable(2, 2));
		mapReduceDriver.withOutput(new TermDocIdWritable("How", "otherfile"),
				new TfNWritable(1, 2));
		mapReduceDriver.runTest();
	}

}
