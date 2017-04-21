import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TweetAMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] searchterms= new String[]{"Chicago","Dec", "Java", "hackathon"};
		String line = value.toString().toLowerCase();
		for(int i =0;i<searchterms.length;i++){
			if(line.contains(searchterms[i].toLowerCase())){
				context.write(new Text(searchterms[i]), new IntWritable(1));
			}
			else context.write(new Text(searchterms[i]), new IntWritable(0));
		}		
	}
}
