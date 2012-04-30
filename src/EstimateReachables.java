import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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




public class EstimateReachables {
	
	public static String EdgeMsg = "E";
	public static String HashMsg = "B";
	public static String NMsg = "N";
	
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
	
	public static double evaluate(int B[], long K)
	{
		long s = 0;
		double m = 0;
		int i;
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
		
		
		m = Math.floor(Math.pow(2, s/K) * (K/0.77351));
		return m;
	}
	
	private static class EstimateReachablesMapper extends MapReduceBase 
    implements Mapper<LongWritable, Text, LongWritable, LongWritable> 
	{
		static long K;
		
		public void configure(JobConf job) 
		{
			K = job.getLong("K", -1);
			
		}
		
		public void map(LongWritable lineid, Text edge,
                OutputCollector<LongWritable, LongWritable> output, Reporter reporter)
		throws IOException 
		{
			if(K==-1)
			{
				throw new IOException("Expected a value of K as input");
				
			}
			StringTokenizer st = new StringTokenizer(edge.toString());
			long u = Long.parseLong(st.nextToken());
			long v = Long.parseLong(st.nextToken());
			output.collect(new LongWritable(u), new LongWritable(v));
			
		}
	}
	
	
	private static class EstimateReachablesReducer extends MapReduceBase 
	implements Reducer<LongWritable, LongWritable, Text, Text> 
	{
		static long K;
		
		public void configure(JobConf job) 
		{
			K = job.getLong("K", -1);
		}
		
		/* Reduce output is a collection of <Messgae Type> <u,v>/ <u, B(u)> -- whatever is applicable*/ 
		public void reduce(LongWritable node, Iterator<LongWritable> neighbours,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException 
		{
			
			if(K==-1)
			{
				throw new IOException("Expected a value of K as input");
			}
			while(neighbours.hasNext())
			{
				String emsg = node + " "+ neighbours.next().toString();
				output.collect(new Text(EdgeMsg), new Text(emsg));
			}
			
			int Bu[] = new int[(int)K];
			for(int i=0;i<K;i++)Bu[i]=0;
			//Inline update()
			
			long h = hash(node.get());
			long k = h %K;
			long l = p(h/K);
		
			if(l<L)
			{
				Bu[(int)k]= 1<<l;    // Initial value assignment
			}
			
			String hmsg = node +" ";
			for(int i=0;i<K;i++)
			{
				hmsg+=Bu[i]+" ";
			}
			
			output.collect(new Text(HashMsg), new Text(hmsg));
			
			double N = evaluate(Bu, K);
			String nmsg = node + " "+new Double(N).toString();
			output.collect(new Text(NMsg), new Text(nmsg));
		}
	}
	
	public long run(String inputPath, String outputPath, int K) throws Exception
	{ 

		
		JobConf conf = new JobConf(EstimateReachables.class);
			
		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setLong("K", K);
		conf.setMapOutputKeyClass(LongWritable.class);
		conf.setMapOutputValueClass(LongWritable.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(EstimateReachablesMapper.class);
		conf.setReducerClass(EstimateReachablesReducer.class);
//conf.setNumReduceTasks(0);
		//delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		JobClient.runJob(conf);
		return 0;
	}
	
}
