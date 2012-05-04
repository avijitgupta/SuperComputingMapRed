
public class Main {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		/*EstimateCount count = new EstimateCount();
		int K = Integer.parseInt(args[0]);
		count.run("estimateCountInput", "estimateArrayOutput", K);
		DisplayCount disp = new DisplayCount();
		disp.run("estimateArrayOutput", "estimateCountOutput", K);
		*/
		ActualCount actual = new ActualCount();
		actual.run("estimateInput1", "UniqueS1CountInput");
		CountUnique ct = new CountUnique();
		ct.run("UniqueS1CountInput", "UniqueCountOutput");
		
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
