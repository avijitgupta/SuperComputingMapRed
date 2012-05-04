import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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


public class DisplayCount {

	private static class DisplayCountMapper extends MapReduceBase 
    implements Mapper<LongWritable, Text, LongWritable, Text> 
	{
		static long K;
		public void configure(JobConf job) 
		{
			K = job.getLong("K", -1);
		}
		public void map(LongWritable lineid, Text arrayvalue,
                OutputCollector<LongWritable, Text> output, Reporter reporter)
		throws IOException 
		{
			if(K==-1)
			{
				throw new IOException("Expected a value of K as input");	
			}
			output.collect(new LongWritable(1), arrayvalue);
			
			
		}
	}
	private static class DisplayCountReducer extends MapReduceBase 
	implements Reducer<LongWritable, Text, Text, NullWritable> 
	{
		static long K;
		
		public void configure(JobConf job) 
		{
			K = job.getLong("K", -1);
		}
		
		public void reduce(LongWritable id, Iterator<Text> array,
				OutputCollector<Text, NullWritable> output, Reporter reporter)
				throws IOException 
		{
			if(K==-1)
			{
				throw new IOException("Expected a value of K as input");
			}
			int B[] = new int[(int)K];
			int i;
			for(i=0;i<K;i++)B[i]=0;
			while(array.hasNext())
			{
				String arrayValue = array.next().toString();
				StringTokenizer st = new StringTokenizer(arrayValue);
				int index = Integer.parseInt(st.nextToken());
				int value = Integer.parseInt(st.nextToken());
				B[index] = value;
			}
			long s = 0;
			double m = 0;
			for(i=0;i<K;i++)
			{
				int r = 0;
				int bitvalue = 1;
				while((B[i]&bitvalue) !=0)
				{
					r++;
					bitvalue = bitvalue<<1;
				}
				s+=(long)r;
			}
			
			m = Math.floor(Math.pow(2, (s*1.0)/K) * (K/0.77351));
			
			Double result = new Double(m); 
			output.collect(new Text(result.toString()), NullWritable.get());
		}
	}
	
	public long run(String inputPath, String outputPath, int K) throws Exception
	{ 

		JobConf conf = new JobConf(DisplayCount.class);
			
		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setLong("K", K);
		conf.setMapOutputKeyClass(LongWritable.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(NullWritable.class);

		conf.setMapperClass(DisplayCountMapper.class);
		conf.setReducerClass(DisplayCountReducer.class);
		conf.setNumReduceTasks(1);
		//delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		JobClient.runJob(conf);
		return 0;
	}
}
