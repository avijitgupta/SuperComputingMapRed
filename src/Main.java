
public class Main {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		EstimateCount count = new EstimateCount();
		int K = 128;
		count.run("estimateInput", "estimateOutput", K);
		DisplayCount dc = new DisplayCount();
		dc.run("estimateOutput", "Result", K);
	}

}
