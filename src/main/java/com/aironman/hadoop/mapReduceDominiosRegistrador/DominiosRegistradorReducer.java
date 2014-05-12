package com.aironman.hadoop.mapReduceDominiosRegistrador;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/***
 * La operacion de reduccion consiste en quedarse con el mayor valor del total de dominios registrados dados por agente
 * 
 * reducer operation consists in keep the largest value of total of registred domains
 * @author aironman
 *
 */
public class DominiosRegistradorReducer extends Reducer<Text, DoubleWritable, Text, Text> {

	private final DecimalFormat decimalFormat = new DecimalFormat("#.###");

	public void reduce(Text key, Iterable<DoubleWritable> totalDominiosValues, Context context)
			throws IOException, InterruptedException {
		double _maxtotalDominios = 0.0f;

		for (DoubleWritable totalDominiosValue : totalDominiosValues) {
			double _total = totalDominiosValue.get() ;
			
			_maxtotalDominios = Math.max(_maxtotalDominios, _total);
		}
		// esto es para quedarme con el agente registrador que tiene el mayor
		// numero de dominios contratados
		context.write(key, new Text(decimalFormat.format(_maxtotalDominios)));

	}

}
