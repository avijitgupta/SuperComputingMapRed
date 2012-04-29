import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class EstimateCount {
	
	static long L = 32;
	
	public static long p(long x)
	{
		long bitvalue = 1;
		long index = 0;
		if(x == 0) return L;
		else
		{
			while((bitvalue&x)==0)
			{
				index++;
				bitvalue= bitvalue<<1;
			}
			return index;
		}
	}
	
	public static long hash(long x)
	{
		long a = 1073741827l;
		long b = 17179869209l;
		long q = 4294967291l;
		long hash = (((a%q)*1l*(x%q))%q + (b%q))%q; // this might overflow? 
		return hash;
	}
	
	private static class EstimateCountMapper extends MapReduceBase 
    implements Mapper<LongWritable, Text, LongWritable, LongWritable> 
	{
		static long K;
		
		public void configure(JobConf job) 
		{
			K = job.getLong("K", -1);
			
		}
		
		public void map(LongWritable lineid, Text xstring,
                OutputCollector<LongWritable, LongWritable> output, Reporter reporter)
		throws IOException 
		{
			if(K==-1)
			{
				throw new IOException("Expected a value of K as input");
				
			}
			if(lineid.toString().equals("0"))return;
			long x = Long.parseLong(xstring.toString());
			long h = hash(x);
			long k = h%K;
			System.out.println(k+ "\t" + x+"\t"+h+"\t");
			LongWritable out_k = new LongWritable(k);
			LongWritable out_v = new LongWritable(h);
			output.collect(out_k, out_v);
		}
	}
	
	
	private static class EstimateCountReducer extends MapReduceBase 
	implements Reducer<LongWritable, LongWritable, LongWritable, LongWritable> 
	{
		static long K;
		
		public void configure(JobConf job) 
		{
			K = job.getLong("K", -1);
			
		}
		
		public void reduce(LongWritable index, Iterator<LongWritable> hashes,
				OutputCollector<LongWritable, LongWritable> output, Reporter reporter)
				throws IOException 
		{
			System.out.println("Entered");
			
			if(K==-1)
			{
				throw new IOException("Expected a value of K as input");
				
			}
			System.out.println("Index : "+ index + "K = "+ K);
			long result = 0;
			while(hashes.hasNext())
			{
				long hash = hashes.next().get();
				long l = p(hash/K);
				if(l<L)
				{
					result = 1<<l | result;
				}
				System.out.println("K = "+K+ "Hasnext "+hashes.hasNext()+"result "+result);

			}
			System.out.println(index+"\t"+result);

			output.collect(index, new LongWritable(result));
			
		}
	}
	
	public long run(String inputPath, String outputPath, int K) throws Exception
	{ 

		
		JobConf conf = new JobConf(EstimateCount.class);
			
		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setLong("K", K);
		conf.setMapOutputKeyClass(LongWritable.class);
		conf.setMapOutputValueClass(LongWritable.class);

		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(LongWritable.class);

		conf.setMapperClass(EstimateCountMapper.class);
		conf.setReducerClass(EstimateCountReducer.class);
//conf.setNumReduceTasks(0);
		//delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		JobClient.runJob(conf);
		return 0;
	}
}
