package hadoopPractise.MapReduce.version2;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTemperatureMapper extends
	Mapper<LongWritable, Text, Text, IntWritable> {

	enum Temperature{
		OVER_100,
		MALFORMED
	}

	private NcdcRecordParser parser=new NcdcRecordParser();

	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
		parser.parse(value);
		if(parser.isValidTemperature()){
			int airTmeperature=parser.getAirTemperature();
			if(airTmeperature>1000){
				System.err.println("Temperature over 100 degrees for input: "+value);
				context.setStatus("Detected possibly corrupt record: see logs.");
				context.getCounter(Temperature.OVER_100).increment(1L);
			}
			context.write(new Text(parser.getYear()),new IntWritable(parser.getAirTemperature()));
		}else{
			context.getCounter(Temperature.MALFORMED).increment(1);
		}
	}
}
