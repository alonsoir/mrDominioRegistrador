package com.aironman.hadoop.mapReduceDominiosRegistrador;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test for simple App. https://cwiki.apache.org/confluence/display/MRUNIT/MRUnit+Tutorial
 */
public class AppTest {

	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver=null;
	private ReduceDriver<Text, DoubleWritable, Text, Text> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, DoubleWritable,Text, Text> mapReduceDriver;
	
	@Before
	public void setUp(){
	
		final DominiosRegistradorMapper mapper = new DominiosRegistradorMapper();
		final DominiosRegistradorReducer reducer = new DominiosRegistradorReducer();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper,reducer);
		
	}
	
	/**
	 * ;Agente Registrador ;Total dominios;;Agente Registrador;Total
	 * dominios;;Agente Registrador;Total dominios 
	 * 1 ;1&1 Internet ;382.972;
	 * ...
	 * 36;WEIS CONSULTING ;4.154;
	 * ... 
	 * 71;MESH DIGITAL LIMITED;910;
	 * ...
	 * @throws IOException 
	 * @throws Exception s
	 * */
	@Test
	public void testMapper() throws IOException  {

		final Text val = new Text("1;1&1 Internet;382.972;");
		mapDriver.withInput(new LongWritable(), val);
		mapDriver.withOutput(new Text("1&1 Internet"), new DoubleWritable(382.972));
		mapDriver.runTest();
	}

	@Test
	public void testReducer() throws IOException {
		
		final List<DoubleWritable> milista = new ArrayList<DoubleWritable>();
		milista.add(new DoubleWritable(382.972));
		milista.add(new DoubleWritable(1));
		milista.add(new DoubleWritable(10));
		milista.add(new DoubleWritable(100));
		//la operacion de reduccion consiste en consiste en aquel que tiene el mayor numero de dominios registrados, es decir el mayor de todos
		reduceDriver.withInput(new Text("1&1 Internet"), milista);
		reduceDriver.withOutput(new Text("1&1 Internet"), new Text("382,972"));
		reduceDriver.runTest();
	}
	

}
