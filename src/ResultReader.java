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

public class ResultReader {
	public static String ResultMsg = "R";

	private static class ResultReaderMapper extends MapReduceBase 
    implements Mapper<LongWritable, Text, LongWritable, LongWritable> 
	{
		
		public void map(LongWritable lineid, Text message,
                OutputCollector<LongWritable, LongWritable> output, Reporter reporter)
		throws IOException 
		{
			StringTokenizer st = new StringTokenizer(message.toString());
			String msgtype = st.nextToken();
			long u = Long.parseLong(st.nextToken());
			if(msgtype.equals(ResultMsg))
			{
				long result  = Long.parseLong(st.nextToken());
				output.collect(new LongWritable(1), new LongWritable(result)); // Keyed on v
			}			
			
		}
	}
	private static class ResultReaderReducer extends MapReduceBase 
	implements Reducer<LongWritable, LongWritable, LongWritable, NullWritable> 
	{
			
		public void reduce(LongWritable id, Iterator<LongWritable> results,
				OutputCollector<LongWritable, NullWritable> output, Reporter reporter)
				throws IOException 
		{
			long orValue = 0;
			while(results.hasNext())
			{
				long result = results.next().get();
				orValue = orValue |result;
			}
			output.collect(new LongWritable(orValue), NullWritable.get());
		}
	}
	
	public long run(String inputPath, String outputPath) throws Exception
	{ 

		JobConf conf = new JobConf(ResultReader.class);
			
		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setMapOutputKeyClass(LongWritable.class);
		conf.setMapOutputValueClass(LongWritable.class);

		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(NullWritable.class);

		conf.setMapperClass(ResultReaderMapper.class);
		conf.setReducerClass(ResultReaderReducer.class);
		conf.setNumReduceTasks(1);
		//delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		JobClient.runJob(conf);
		return 0;
	}
}
