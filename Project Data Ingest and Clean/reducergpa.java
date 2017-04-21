import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
public class reducergpa extends Reducer<DoubleWritable, DoubleWritable, DoubleWritable, DoubleWritable> {
		@Override
		public void reduce(DoubleWritable key, Iterable<DoubleWritable> values, Context context)
		throws IOException, InterruptedException {
		double sum = 0;
		int count = 0;
		for (DoubleWritable value : values) {
			sum+= value.get();
			count++;
		}
		context.write(key, new DoubleWritable(sum/count));
		}
}
