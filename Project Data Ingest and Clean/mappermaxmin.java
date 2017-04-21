import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class mappermaxmin extends Mapper<LongWritable, Text, IntWritable, DoubleWritable>{
@Override
public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
	String line = value.toString();
	if(line!= null && key.get() != 0){
		String[] tokens = line.split(",");
		double gpa = Double.parseDouble(tokens[0]);
		double sat = Double.parseDouble(tokens[1]);
		int a=1,b=2;
		if(gpa<=4.0 && gpa>=0.0 && sat>=0.0 && sat<=1600){
		context.write(new IntWritable(a),new DoubleWritable(gpa));
		context.write(new IntWritable(b), new DoubleWritable(sat));
		}
	}
}
}
