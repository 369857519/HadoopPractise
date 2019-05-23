package hadoopPractise.MapReduce.version2;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTemperature {
// export HADOOP_CLASSPATH=/Users/qilianshan/Documents/videoSpace/hadoopPractise/target/hadoopPractise-1.0-SNAPSHOT.jar
	public static void main(String[] args)
		throws IOException, ClassNotFoundException, InterruptedException {
		if(args.length!=2){
			System.err.println("Usage: MaxTemperature");
			System.exit(-1);
		}

		Job job=Job.getInstance();
		job.setJarByClass(MaxTemperature.class);
		job.setJobName("Max temperature");

		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));

		job.setMapperClass(MaxTemperatureMapper.class);
		job.setReducerClass(MaxTemperatureReducer.class);
		job.setCombinerClass(MaxTemperatureReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		System.exit(job.waitForCompletion(true)?0:1);
	}

}
