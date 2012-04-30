import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.examples.SecondarySort.IntPair;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;


public class GlobalOr {
	public static String EdgeMsg = "E";
	public static String HashMsg = "B";
	public static String NMsg = "N";
	public static String ResultMsg = "R";
	public static double epsilon = 0.0000001; 
	
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
	/*
	public static class FirstPartitioner implements Partitioner<Text, Text>
	{
		@Override
		public void configure(JobConf job) {}
	
		@Override
		public int getPartition(Text key, Text value, int numPartitions) 
		{
			StringTokenizer st = new StringTokenizer(key.toString());
			//return Math.abs(Integer.parseInt(st.nextToken()) * 127) % numPartitions;   // The nodeId is actually a long value
			int pno = (int)(Long.parseLong(st.nextToken())% numPartitions);
			return Math.abs(((pno%numPartitions)*(127%numPartitions))%numPartitions);
		}
	}
	
	public static class KeyComparator extends WritableComparator 
	{
		protected KeyComparator() {
		super(Text.class, true);
		}
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) 
		{
			Text ip1 = (Text) w1;
			Text ip2 = (Text) w2;
			StringTokenizer st1 = new StringTokenizer(ip1.toString());
			StringTokenizer st2 = new StringTokenizer(ip2.toString());
			
			int cmp = st1.nextToken().compareTo(st2.nextToken());
			if (cmp != 0) 
			{
				return cmp;
			}
		
			return st1.nextToken().compareTo(st2.nextToken()); //reverse
		
		}
	}

	public static class GroupComparator extends WritableComparator 
	{
		protected GroupComparator() {
		super(IntPair.class, true);
		}
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
		Text ip1 = (Text) w1;
		Text ip2 = (Text) w2;
		
		StringTokenizer st1 = new StringTokenizer(ip1.toString());
		StringTokenizer st2 = new StringTokenizer(ip2.toString());
		
		return st1.nextToken().compareTo(st2.nextToken());
		}
	}
*/
	
	private static class GlobalOrMapper extends MapReduceBase 
    implements Mapper<LongWritable, Text, LongWritable, Text> 
	{
		static long K;
		
		public void configure(JobConf job) 
		{
			K = job.getLong("K", -1);
			
		}
		
		public void map(LongWritable lineid, Text message,
                OutputCollector<LongWritable, Text> output, Reporter reporter)
		throws IOException 
		{
			if(K==-1)
			{
				throw new IOException("Expected a value of K as input");
				
			}
			
			StringTokenizer st = new StringTokenizer(message.toString());
			String msgtype = st.nextToken();
			long u = Long.parseLong(st.nextToken());
			//System.out.println("Message Type" + msgtype + "u = "+ u);
			output.collect(new LongWritable(u), message); // Keyed on u
			
			
		}
	}
	
	
	private static class GlobalOrReducer extends MapReduceBase 
	implements Reducer<LongWritable, Text, Text, NullWritable> 
	{
		static long K;
		
		public void configure(JobConf job) 
		{
			K = job.getLong("K", -1);
		}
		
		/* Reduce output is a collection of <Messgae Type> <u,v>/ <u, B(u)> -- whatever is applicable*/ 
		public void reduce(LongWritable nodeid, Iterator<Text> message,
				OutputCollector<Text, NullWritable> output, Reporter reporter)
				throws IOException 
		{
			
			double Nu = 0;
			if(K==-1)
			{
				throw new IOException("Expected a value of K as input");
			}
			int Bu[] = new int[(int)K];
			for(int i=0 ; i< K ; i++)
			{
				Bu[i] = 0 ; // Default hash - Used in case of directed edges.. am I correct??
			}
			
			while(message.hasNext())
			{
				System.out.print("u = " + nodeid + "Bu= ");
				for(int i=0; i<K;i++)
					System.out.print(Bu[i] + " ");
				
				String msgstring = message.next().toString();
				//System.out.println(msgstring);
				
				StringTokenizer st = new StringTokenizer(msgstring);
				String msgtype = st.nextToken();
				long u = Long.parseLong(st.nextToken());
				
				if(msgtype.equals(HashMsg))
				{
					for(int i = 0 ;i <K; i++)
					{
				//		Bu[i] = Integer.parseInt(st.nextToken());
						Bu[i] = Bu[i] | Integer.parseInt(st.nextToken());
					}
					
					//output.collect(new Text(msgstring), NullWritable.get());
				}
				else if(msgtype.equals(EdgeMsg))
				{
					long v = Long.parseLong(st.nextToken());
					
					for(int i = 0; i<K; i++)
					{
						int v_i = Integer.parseInt(st.nextToken());
						System.out.print(v_i+ " ");
						Bu[i] = Bu[i] | v_i;
					}
					System.out.println();

					String out = EdgeMsg+ " " + u+ " " + v;
					
					output.collect(new Text(out), NullWritable.get());
				}
				else
				{
					Nu = Double.parseDouble(st.nextToken());
					//output.collect(new Text(msgstring), NullWritable.get());
				}
			}
			
			String hashmsg = HashMsg + " " + nodeid.get()+ " ";
			for(int i=0;i<K;i++)
			{
				hashmsg = hashmsg+ Bu[i]+" ";
			}
			
			output.collect(new Text(hashmsg), NullWritable.get());
			
			double t = evaluate(Bu, K);
			
			String nmsg = NMsg + " " + nodeid.get() + " " + t;
			
			output.collect(new Text(nmsg), NullWritable.get());
			
			if((t - Nu < epsilon && t-Nu >= 0) || (Nu-t <epsilon && Nu - t >=0))
			{
				String out = ResultMsg+" " + nodeid.get()+ " " + "1";
				output.collect(new Text(out), NullWritable.get());
			}
		}
	}
	
	public long run(String inputPath, String outputPath, int K) throws Exception
	{ 

		
		JobConf conf = new JobConf(EstimateReachablesLooper.class);
			
		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setLong("K", K);
		conf.setMapOutputKeyClass(LongWritable.class);
		conf.setMapOutputValueClass(Text.class);
		//conf.setPartitionerClass(FirstPartitioner.class);
		//conf.setOutputKeyComparatorClass(KeyComparator.class);
		//conf.setOutputValueGroupingComparator(GroupComparator.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(NullWritable.class);

		conf.setMapperClass(GlobalOrMapper.class);
		conf.setReducerClass(GlobalOrReducer.class);
		//conf.setNumReduceTasks(0);
		//delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		JobClient.runJob(conf);
		return 0;
	}
}
