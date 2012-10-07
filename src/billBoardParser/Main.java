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
			//ArrayList<String> result = parser.FindNewPosts();
			//parser.ParsePage("http://krasnoyarsk.olx.ru/16-iid-442571778");
			//parser.DownloadPage("http://krasnoyarsk.olx.ru/130-270-iid-442572452");
			parser.downloadAllReadedPages();
			System.out.println("all: ");
			
		}
		catch (Exception e)
		{
			System.out.println("error: " + e.getMessage());
		}
	}

}
