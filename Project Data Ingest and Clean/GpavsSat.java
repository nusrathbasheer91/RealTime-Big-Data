import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class GpavsSat {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
		System.err.println("Usage: GPavSat <input path> <output path>");
		System.exit(-1);
		}
		Job job = new Job();
		job.setJarByClass(GpavsSat.class);
		job.setJobName("GPA vs SAT");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]+"1"));
		job.setMapperClass(mappergpa.class);
		job.setReducerClass(reducergpa.class);
		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.waitForCompletion(true);
		job = new Job();
		job.setJarByClass(GpavsSat.class);
		job.setJobName("SAT vs GPA");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]+"2"));
		job.setMapperClass(mappersat.class);
		job.setReducerClass(reducergpa.class);
		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.waitForCompletion(true);
		job = new Job();
		job.setJarByClass(GpavsSat.class);
		job.setJobName("Max and Min");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]+"3"));
		job.setMapperClass(mappermaxmin.class);
		job.setReducerClass(reducermaxmin.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
		}