import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
public class reducermaxmin extends Reducer<IntWritable, DoubleWritable, Text, DoubleWritable> {
		@Override
		public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context)
		throws IOException, InterruptedException {
		double sum = 0;
		double max = 0, min = Double.MAX_VALUE;
		
		for (DoubleWritable value : values) {
			max = Math.max(max, value.get());
			min = Math.min(min, value.get());
		}
		context.write(new Text("Max of column "+key.toString()+" is"), new DoubleWritable(max));
		context.write(new Text("Min of column "+key.toString()+" is"), new DoubleWritable(min));
		}
}
