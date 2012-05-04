import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class ActualCount {
	private static class ActualCountsMapper extends MapReduceBase 
    implements Mapper<LongWritable, Text, Text, NullWritable> 
	{
		
		public void map(LongWritable lineid, Text edge,
                OutputCollector<Text, NullWritable> output, Reporter reporter)
		throws IOException 
		{
			if(lineid.toString().equals("0"))return;
			output.collect(edge, NullWritable.get());
			
		}
	}
	private static class ActualCountsReducer extends MapReduceBase 
	implements Reducer<Text, NullWritable, LongWritable, NullWritable> 
	{
		
		/* Reduce output is a collection of <Messgae Type> <u,v>/ <u, B(u)> -- whatever is applicable*/ 
		public void reduce(Text key, Iterator<NullWritable> nulls,
				OutputCollector<LongWritable, NullWritable> output, Reporter reporter)
				throws IOException 
		{
			output.collect(new LongWritable(1), NullWritable.get());
		}
	}
	
	public long run(String inputPath, String outputPath) throws Exception
	{ 

		
		JobConf conf = new JobConf(EstimateReachables.class);
			
		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(NullWritable.class);

		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(NullWritable.class);

		conf.setMapperClass(ActualCountsMapper.class);
		conf.setReducerClass(ActualCountsReducer.class);
		conf.setNumReduceTasks(1);
		//delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		JobClient.runJob(conf);
		return 0;
	}
}
