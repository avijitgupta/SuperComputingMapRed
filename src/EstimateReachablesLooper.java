import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
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
import org.apache.hadoop.examples.SecondarySort.IntPair;



public class EstimateReachablesLooper {
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

	
	private static class EstimateReachablesLooperMapper extends MapReduceBase 
    implements Mapper<LongWritable, Text, Text, Text> 
	{
		static long K;
		
		public void configure(JobConf job) 
		{
			K = job.getLong("K", -1);
			
		}
		
		public void map(LongWritable lineid, Text message,
                OutputCollector<Text, Text> output, Reporter reporter)
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
			if(msgtype.equals(EdgeMsg))
			{
				long v = Long.parseLong(st.nextToken());
				Text compositeKey = new Text(v+" "+msgtype);
				output.collect(compositeKey, message); // Keyed on v
			}
			if(msgtype.equals(NMsg))
			{
				Text compositeKey = new Text(u+" "+msgtype);
				output.collect(compositeKey, message);
			}
			if(msgtype.equals(HashMsg))
			{
				Text compositeKey = new Text(u+" "+msgtype);
				output.collect(compositeKey, message);
			}
			
		}
	}
	
	
	private static class EstimateReachablesLooperReducer extends MapReduceBase 
	implements Reducer<Text, Text, Text, NullWritable> 
	{
		static long K;
		
		public void configure(JobConf job) 
		{
			K = job.getLong("K", -1);
		}
		
		/* Reduce output is a collection of <Messgae Type> <u,v>/ <u, B(u)> -- whatever is applicable*/ 
		public void reduce(Text compositeKey, Iterator<Text> message,
				OutputCollector<Text, NullWritable> output, Reporter reporter)
				throws IOException 
		{
			
		
			if(K==-1)
			{
				throw new IOException("Expected a value of K as input");
			}
			
			String hash = "";
			for(int i=0 ; i< K ; i++)
			{
				hash = hash + "0 "; // Default hash - Used in case of directed edges.. am I correct??
			}
			
			while(message.hasNext())
			{
				System.out.print("Message has next ");

				String msgstring = message.next().toString();
				StringTokenizer st = new StringTokenizer(msgstring);
				String msgtype = st.nextToken();
				if(msgtype.equals(HashMsg))
				{
					int index = msgstring.indexOf(' ');
					hash = msgstring.substring(index+1);
					output.collect(new Text(msgstring), NullWritable.get());
				}
				else if(msgtype.equals(EdgeMsg))
				{
					String out = msgstring+ " " + hash;
					output.collect(new Text(out), NullWritable.get());
				}
				else
				{
					output.collect(new Text(msgstring), NullWritable.get());
				}
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
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setPartitionerClass(FirstPartitioner.class);
		conf.setOutputKeyComparatorClass(KeyComparator.class);
		conf.setOutputValueGroupingComparator(GroupComparator.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(NullWritable.class);

		conf.setMapperClass(EstimateReachablesLooperMapper.class);
		conf.setReducerClass(EstimateReachablesLooperReducer.class);
		//conf.setNumReduceTasks(0);
		//delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		JobClient.runJob(conf);
		return 0;
	}
}
