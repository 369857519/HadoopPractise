package hadoopPractise.MapReduce.version2;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Mapper;

public class LoggingIdentityMapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT> extends Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {

	private static final Log LOG = LogFactory.getLog(LoggingIdentityMapper.class);

	@Override
	@SuppressWarnings("unchecked")
	public void map(KEYIN keyin,VALUEIN valuein,Context context)
		throws IOException, InterruptedException {
		System.out.println("Map key: " + keyin);
		LOG.info("Map key: {}"+keyin);
		if(LOG.isDebugEnabled()){
			LOG.debug("Map value: "+valuein);
		}
		context.write((KEYOUT)keyin,(VALUEOUT)valuein);
	}
}
