
public class Main {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		EstimateCount count = new EstimateCount();
		int K = Integer.parseInt(args[0]);
		String inputPath = args[1];
		String outputPath = args[2];
		count.run(inputPath, "estimateArrayOutput", K);
		DisplayCount disp = new DisplayCount();
		disp.run("estimateArrayOutput", outputPath, K);
		
		if(args.length == 4)
		{
			String actualCountPath = args[3];
			ActualCount actual = new ActualCount();
			actual.run(inputPath, "UniqueS1CountInput");
			CountUnique ct = new CountUnique();
			ct.run("UniqueS1CountInput", actualCountPath);
		}
	/*	EstimateReachables reach = new EstimateReachables();
		int K = 32;
		reach.run("estimateReachablesInput", "estimateReachablesOutput", K);
		EstimateReachablesLooper loop = new EstimateReachablesLooper();
		loop.run("estimateReachablesOutput", "estimateReachablesLoopOutput", K);
		GlobalOr or = new GlobalOr();
		or.run("estimateReachablesLoopOutput", "IterationOutput", K);
		ResultReader reader = new ResultReader();
		reader.run("IterationOutput", "ShouldStop");*/
		//DisplayCount dc = new DisplayCount();
		//dc.run("estimateOutput", "Result", K);
	}

}
