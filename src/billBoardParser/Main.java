package billBoardParser;

import billBoardParser.readers.Olx;


public class Main {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try
		{
			Olx parser = new Olx();
			System.out.println("Find new posts");
			parser.FindNewPosts();
			System.out.println("download pages");
			parser.downloadAllReadedPages();
			System.out.println("parsing pages");
			parser.ParsePages();
			System.out.println("finish");
			
		}
		catch (Exception e)
		{
			System.out.println("error: " + e.getMessage());
		}
	}

}
