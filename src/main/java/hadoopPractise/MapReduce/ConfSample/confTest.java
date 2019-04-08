package hadoopPractise.MapReduce.ConfSample;

import java.util.Map.Entry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class confTest extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		Configuration configuration=new Configuration();
		configuration.addResource("config1.xml");
		configuration.addResource("config2.xml");
		String weight=configuration.get("weight");
		String height=configuration.get("height");
		String character=configuration.get("character");

		//辅助类
		//GenericOptionsParser 常用命令
		//ToolRunner
		Configuration.addDefaultResource("config1.xml");
		Configuration.addDefaultResource("config2.xml");
		int exitCode=ToolRunner.run(new confTest(),args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] strings) throws Exception {
		Configuration configuration=getConf();
		for(Entry<String,String> entry:configuration){
			System.out.printf("%s=%s\n",entry.getKey(),entry.getValue());
		}
		return 0;
	}
}
