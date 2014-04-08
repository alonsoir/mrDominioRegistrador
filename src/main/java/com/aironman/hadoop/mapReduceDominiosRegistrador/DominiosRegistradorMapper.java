package com.aironman.hadoop.mapReduceDominiosRegistrador;

import java.io.IOException;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DominiosRegistradorMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	private static final String SEPARATOR = ";";

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException,
			InterruptedException {
		
		System.out.println(" =value= " + value.toString());
		
		final String[] values = value.toString().split(SEPARATOR);
		for (int i=0;i<values.length;i++)
			System.out.println(" elem " + i + ": " + values[i]);
		/**
		 *   ;Agente Registrador  ;Total dominios;;Agente Registrador;Total dominios;;Agente Registrador;Total dominios 
		 * 1 ;1&1 Internet		  ;382.972;
		 * 36;WEIS CONSULTING	  ;4.154;
		 * 71;MESH DIGITAL LIMITED;910;
		 * 
		 * */
		final String agente = format(values[1]);
		final String totalDominios = format(values[2]);
		System.out.println("agente: " + agente + " tiene " + totalDominios + " dominios.");
		

		if (NumberUtils.isNumber(totalDominios.toString())) {
			context.write(new Text(agente), new DoubleWritable(NumberUtils.toDouble(totalDominios)));
		}
	}

	private String format(String value) {
		return value.trim();
	}

}
