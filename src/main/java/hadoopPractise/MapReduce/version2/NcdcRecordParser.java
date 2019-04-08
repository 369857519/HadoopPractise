package hadoopPractise.MapReduce.version2;

import lombok.Data;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

@Data
public class NcdcRecordParser {
	private static final int MISSING_TEMPERATURE=9999;

	private String year;
	private int airTemperature;
	private String quality;

	public void parse(String record){
		String year=record.substring(15,19);
		String airTemperatureStr;
		if(record.charAt(87)=='+'){
			airTemperatureStr=record.substring(88,92);
		}else{
			airTemperatureStr=record.substring(87,92);
		}
		airTemperature = Integer.parseInt(airTemperatureStr);
		String quality=record.substring(92,93);
	}
	public void parse(Text record){
		parse(record.toString());
	}
	public boolean isValidTemperature(){
		return airTemperature !=MISSING_TEMPERATURE&&quality.matches("[01459]");
	}
}
