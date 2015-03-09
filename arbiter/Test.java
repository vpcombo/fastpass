public class Test 
{
	public static void main(String [] args)
	{
		Thread fp = new Thread(new FastPass());
		fp.start();
	}
}
