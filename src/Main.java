
public class Main {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		EstimateReachables reach = new EstimateReachables();
		int K = 32;
		reach.run("estimateReachablesInput", "estimateReachablesOutput", K);
		EstimateReachablesLooper loop = new EstimateReachablesLooper();
		loop.run("estimateReachablesOutput", "estimateReachablesLoopOutput", K);
		GlobalOr or = new GlobalOr();
		or.run("estimateReachablesLoopOutput", "IterationOutput", K);
		ResultReader reader = new ResultReader();
		reader.run("IterationOutput", "ShouldStop");
		//DisplayCount dc = new DisplayCount();
		//dc.run("estimateOutput", "Result", K);
	}

}
