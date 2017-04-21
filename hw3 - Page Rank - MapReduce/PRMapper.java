import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class PRMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		int i=1,len;
		Double pr=0.0;
		String[] line = value.toString().split(" ");
		len=line.length;
		pr = Double.parseDouble(line[len-1])/(len-2);
		for(i=1;i<len-1;i++)
		{
			context.write(new Text(line[i]), new Text(pr.toString()));
			context.write(new Text(line[0]), new Text(line[i]));
		}
		
	}
}
