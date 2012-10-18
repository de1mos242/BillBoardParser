package billBoardParser.readers;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;

import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class Olx {
	private String SearchUrl = "http://krasnoyarsk.olx.ru/nf/all-results";
	private String UserAgent = "Mozilla/5.0 (X11; Linux i686) AppleWebKit/536.11 (KHTML, like Gecko) Ubuntu/12.04 Chromium/20.0.1132.47 Chrome/20.0.1132.47 Safari/536.11";
	
	private BufferedWriter logger;
	
	private java.sql.Connection dbConnection;
	private Statement dbStatement;
	
	public Olx() throws Exception {
		logger  = new BufferedWriter(new FileWriter(new File("log2.txt"), true));
		prepareDB();
	}
	
	private void prepareDB() throws Exception {
		Class.forName("org.sqlite.JDBC");
		dbConnection = DriverManager.getConnection("jdbc:sqlite:sqlite.db3");
		dbStatement = dbConnection.createStatement();
		dbStatement.execute("create table if not exists 'pages' ('id' INTEGER PRIMARY KEY AUTOINCREMENT, 'url' text, 'filename' text);");
		dbStatement.execute("create unique index if not exists 'url_index' on 'pages' ('url') ");
		dbStatement.execute("create unique index if not exists 'filename_index' on 'pages' ('filename') ");
		System.out.println("Db prepared");
	}
	
	public ArrayList<String> FindNewPosts() throws Exception {
		ArrayList<String> entries = new ArrayList<String>();
		try
		{
			Connection conn;
			Document currentPage;
			String currentUrl = SearchUrl;
			
			int pageNumber = 1;
			while(true) {
				conn = Jsoup.connect(currentUrl);
				conn.userAgent(UserAgent);
				currentPage = null;
				boolean loaded = true;
				do {
					loaded = true;
					try {
						currentPage = conn.get();
					}
					catch (Exception e) {
						loaded = false;
					}
				}
				while (!loaded);
				
				ArrayList<String> currentPagePosts = GetPostsFromPage(currentPage);
				log("page " + pageNumber + " " + currentUrl);
				printList(currentPagePosts);
				log("page loaded " + pageNumber);
				entries.addAll(currentPagePosts);
				pageNumber++;
				
				String nextPageUrl = GetNextPage(currentPage);
				if (nextPageUrl == "")
					break;
				if (currentUrl == nextPageUrl)
					break;
				currentUrl = nextPageUrl;
			}
		}
		catch (Exception e) 
		{
			e.printStackTrace();
		}
		finally {
		}
		return entries;
	}
	
	private ArrayList<String> GetPostsFromPage(Document page) throws Exception {
		Elements posts = page.select("h3 a");
		ArrayList<String> result = new ArrayList<String>();
		log("posts size: " + posts.size());
		PreparedStatement insertPageStatment = dbConnection.prepareStatement("insert into 'pages' ('url') values (?);");
		boolean hasToInsert = false;
		for (Element el : posts ) {
			String url = el.attr("href");
			if (url.matches("^http.*"))
			{
				if (!dbStatement.executeQuery("select 'id' from 'pages' where url = '" + url + "'").next())
				{
					hasToInsert = true;
					result.add(url);
					insertPageStatment.setString(1, url);
					insertPageStatment.addBatch();
				}
				else
				{
					log("skip: " + url);
				}
			}
			else
			{
				log("wrong: " + url);
			}
		}
		if (hasToInsert)
		{
			//dbConnection.setAutoCommit(false);
			insertPageStatment.executeBatch();
			//dbConnection.setAutoCommit(true);
		}
		return result;
	}
	
	private String GetNextPage(Document page) {
		Element nextPage = page.select(".next").first();
		if (nextPage == null)
			return "";
		return nextPage.attr("href");
	}
	
	private void printList(ArrayList<String> list) {
		for (String s : list)
			log("url: " + s);
	}
	
	private void log(String line) {
		String dt = new java.text.SimpleDateFormat("dd-MMM-yy hh:mm:ss").format(java.util.Calendar.getInstance ().getTime());
		System.out.println(line);
		try {
			logger.write(dt + " " + line);
			logger.newLine();
			logger.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public void ParsePage(String url) throws IOException {
		Connection conn = Jsoup.connect(url);
		conn.userAgent(UserAgent);
		Document page = conn.get();
		Element buyType = page.select("#firstpath1").first();
		System.out.println("buy type: " + buyType.text());
		Element category = page.select("#firstpath2").first();
		System.out.println("category " + category.text());
		
		Element subject = page.select("div.h1").first();
		System.out.println("subject: " + subject.text());
		
		Element price = page.select(".price").select("strong").first();
		System.out.println("price: " + price.text());
	}
	
	public String DownloadPage(String link) throws Exception {
		String[] splittedUrl = link.split("/");
		String filename = "pages/" + splittedUrl[splittedUrl.length-1];
		System.out.println("Working " + link);
		if (!new File(filename).exists())
		{
			BufferedWriter writer = new BufferedWriter(new FileWriter(new File(filename), false));
			
			String oldUA = System.getProperty("http.agent");
			System.setProperty("http.agent", "");
			URL url = new URL(link);
			URLConnection connection = url.openConnection();
			connection.setRequestProperty("User-Agent", UserAgent);
			InputStream is = connection.getInputStream();
			BufferedReader in = new BufferedReader(
			        new InputStreamReader(is));
			String inputLine;
	        while ((inputLine = in.readLine()) != null)
	        {
	            writer.write(inputLine);
	        	writer.newLine();
	        }
	        in.close();
	        if (oldUA != null)
	        {
	        	System.setProperty("http.agent", oldUA);
	        }
			writer.flush();
			writer.close();
			
			System.out.println("file " + filename + " created");
		}
		else
		{
			System.out.println("file " + filename + " exists");
		}
		/*Statement updateStatement = dbConnection.createStatement();
		String updateQuery = "update pages set filename = '" + filename + "'  where url = '" + link + "'";
		System.out.println(updateQuery);
		int rows = updateStatement.executeUpdate(updateQuery);
		System.out.println("updated " + rows + " rows");
		//dbConnection.setAutoCommit(false);
		//dbConnection.setAutoCommit(true);
		updateStatement.close();*/
		return filename;
	}
	
	public void downloadAllReadedPages() throws Exception {
		File downloadDir = new File("pages/");
		if (downloadDir.exists())
		{
			downloadDir.mkdir();
		}
		ResultSet counter = dbStatement.executeQuery("select count(*) from pages where filename is not null and filename <> ''");
		counter.next();
		System.out.println("count with filename: " + counter.getString(1));
		counter.close();
		
		ArrayList<String> workSet = new ArrayList<String>();
		
		ResultSet rs = dbStatement.executeQuery("select url from pages where filename is null");
		while (rs.next()) {
			String link = rs.getString(1);
			workSet.add(link);
		}
		rs.close();
		
		int i = 0;
		int trashHold = 10;
		String updateQuery = "update pages set filename = ? where url = ?";
		PreparedStatement updatePageStatment = null;
		for (String link : workSet) {
			if (updatePageStatment == null)
			{
				updatePageStatment = dbConnection.prepareStatement(updateQuery);
			}
			
			String filename = DownloadPage(link);
			updatePageStatment.setString(1, filename);
			updatePageStatment.setString(2, link);
			updatePageStatment.addBatch();
			
			i++;
			if (i >= trashHold)
			{
				updatePageStatment.executeBatch();
				updatePageStatment = null;
				i=0;
			}
		}
		updatePageStatment.close();
	}
	
}
