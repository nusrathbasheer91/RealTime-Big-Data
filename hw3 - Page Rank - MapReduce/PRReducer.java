import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class PRReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Double pr=0.0;
		String outlink = key.toString();
		for(Text value : values){
			if(isNum(value.toString()))
			{
				pr+=Double.parseDouble(value.toString());
			}
			else
				outlink=outlink+" "+value.toString();
		}
		context.write(new Text(outlink), new Text(pr.toString()));
	}
	public boolean isNum(String s) {  
	    return s.matches("[-+]?\\d*\\.?\\d+");  
	}  
}