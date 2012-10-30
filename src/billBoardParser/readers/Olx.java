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
import java.util.HashMap;
import java.util.Map.Entry;

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
		
		dbStatement.execute("create table if not exists 'parsed_page' ('id' INTEGER PRIMARY KEY AUTOINCREMENT, 'type' string, 'name' string, 'data' text, 'filename' text);");
		dbStatement.execute("create index if not exists 'filename_index' on 'parsed_page' ('filename')");
		dbStatement.execute("create index if not exists 'type_index' on 'parsed_page' ('type')");
		
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
				
				ArrayList<Boolean> cruchFoundLoaded = new ArrayList<>();
				ArrayList<String> currentPagePosts = GetPostsFromPage(currentPage, cruchFoundLoaded);
				if (cruchFoundLoaded.size() > 0)
					break;
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
	
	private ArrayList<String> GetPostsFromPage(Document page, ArrayList<Boolean> foundLoaded) throws Exception {
		Elements posts = page.select("h3 a");
		ArrayList<String> result = new ArrayList<String>();
		log("posts size: " + posts.size());
		PreparedStatement insertPageStatment = dbConnection.prepareStatement("insert into 'pages' ('url') values (?);");
		boolean hasToInsert = false;
		for (Element el : posts ) {
			String url = el.attr("href");
			if (url.matches("^http.*"))
			{
				ResultSet checkRS = dbStatement.executeQuery("select 'id' from 'pages' where url = '" + url + "'");
				if (!checkRS.next())
				{
					hasToInsert = true;
					result.add(url);
					insertPageStatment.setString(1, url);
					insertPageStatment.addBatch();
				}
				else
				{
					log("skip: " + url);
					foundLoaded.add(true);
				}
				checkRS.close();
			}
			else
			{
				//log("wrong: " + url);
			}
		}
		if (hasToInsert)
		{
			//dbConnection.setAutoCommit(false);
			insertPageStatment.executeBatch();
			//dbConnection.setAutoCommit(true);
		}
		insertPageStatment.close();
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
		if (updatePageStatment != null)
		{
			updatePageStatment.executeBatch();
			updatePageStatment.close();
		}
	}
	
	public void ParsePages() throws Exception {
		ArrayList<String> parsedFiles = new ArrayList<>();
		ResultSet rs = dbStatement.executeQuery("select distinct filename from parsed_page");
		while (rs.next()) {
			parsedFiles.add(rs.getString(1));
		}
		rs.close();
		
		ArrayList<String> workSet = new ArrayList<>();
		rs = dbStatement.executeQuery("select filename from pages where filename is not null");
		while(rs.next()) {
			String tempFilename = rs.getString(1);
			if (!parsedFiles.contains(tempFilename)) {
				workSet.add(tempFilename);
			}
		}
		rs.close();
		
		parsedFiles.clear();
		for (String filename : workSet) {
			System.out.println("File " + filename);
			if (new File(filename).length() == 0L) {
				System.out.println("file skipped because it's empty!");
				continue;
			}
			HashMap<String, Object> parsedData = parsePage(filename);
			SaveParsedData(parsedData, filename);
		}
	}
	
	@SuppressWarnings("unchecked")
	void SaveParsedData(HashMap<String, Object> parsedData, String filename) throws Exception {
		PreparedStatement insertParsedPageStatment = dbConnection.prepareStatement("insert into 'parsed_page' ('type','name','data','filename') values (?,?,?,?);");
		//HashMap<String, String> allFields = new HashMap<>();
		
		HashMap<String, String> commonPart = (HashMap<String, String>)parsedData.get("commonFields");
		//System.out.println("  commonFields:");
		for (Entry<String, String> commonField : commonPart.entrySet()) {
			//System.out.println("    " + commonField.getKey() + " - " + commonField.getValue());
			//allFields.put(commonField.getKey(), commonField.getValue());
			insertParsedPageStatment.setString(1, "commonField");
			insertParsedPageStatment.setString(2, commonField.getKey());
			insertParsedPageStatment.setString(3, commonField.getValue());
			insertParsedPageStatment.setString(4, filename);
			insertParsedPageStatment.addBatch();
		}
		
		HashMap<String, String> userData = (HashMap<String, String>)parsedData.get("userData");
		//System.out.println("  userData:");
		for (Entry<String, String> userDataEntry : userData.entrySet()) {
			//System.out.println("    " + userDataEntry.getKey() + " - " + userDataEntry.getValue());
			//allFields.put(userDataEntry.getKey(), userDataEntry.getValue());
			insertParsedPageStatment.setString(1, "userData");
			insertParsedPageStatment.setString(2, userDataEntry.getKey());
			insertParsedPageStatment.setString(3, userDataEntry.getValue());
			insertParsedPageStatment.setString(4, filename);
			insertParsedPageStatment.addBatch();
		}
		
		HashMap<String, String> additionalPart = (HashMap<String, String>)parsedData.get("additionalFields");
		//System.out.println("  additionalFields:");
		for (Entry<String, String> additionalField : additionalPart.entrySet()) {
			//System.out.println("    " + additionalField.getKey() + " - " + additionalField.getValue());
			//allFields.put(additionalField.getKey(), additionalField.getValue());
			insertParsedPageStatment.setString(1, "additionalFields");
			insertParsedPageStatment.setString(2, additionalField.getKey());
			insertParsedPageStatment.setString(3, additionalField.getValue());
			insertParsedPageStatment.setString(4, filename);
			insertParsedPageStatment.addBatch();
		}
		
		ArrayList<String> images = (ArrayList<String>)parsedData.get("images");
		//System.out.println("  images:");
		for (int i=0;i<images.size();i++) {
			//System.out.println("    " + images.get(i));
			//allFields.put("img"+i, images.get(i));
			insertParsedPageStatment.setString(1, "images");
			insertParsedPageStatment.setString(2, "img"+i);
			insertParsedPageStatment.setString(3, images.get(i));
			insertParsedPageStatment.setString(4, filename);
			insertParsedPageStatment.addBatch();
		}
		
		insertParsedPageStatment.executeBatch();
		System.out.println("file "+ filename + " parsed");
	}
	
	HashMap<String, Object> parsePage(String filename) throws Exception {
		HashMap<String, Object> pageData = new HashMap<>();
		Document page = Jsoup.parse(new File(filename), "utf-8");
		pageData.put("commonFields", getCommonFields(page));
		pageData.put("userData", getUserFields(page));
		pageData.put("additionalFields", getAdditionalFields(page));
		pageData.put("images", getImages(page));
		return pageData;
	}
	
	HashMap<String, String> getCommonFields(Document page) {
		HashMap<String, String> commonFields = new HashMap<>();
		commonFields.put("Type", page.select("#firstpath1").first().text());
		commonFields.put("SubType", page.select("#firstpath2").first().text());
		commonFields.put("Subject", page.select("div.h1").first().text().replaceAll(" — Красноярск", ""));
		Element description = page.select("#item-desc #description-text").first();
		if (description != null)
		{
			String textDesc = "";
			for (Element paragraph : description.select("p")) {
				textDesc += paragraph.text();
			}
			if (textDesc != "")
				commonFields.put("Description", textDesc);
		}
		return commonFields;
	}
	
	HashMap<String, String> getUserFields(Document page) {
		HashMap<String, String> userFields = new HashMap<>();
		userFields.put("UserNick", page.select(".user-wrapper").first().text());
		Element phone = page.select(".phone").first();
		if (phone != null)
			userFields.put("UserPhone", phone.text());
		return userFields;
	}
	
	HashMap<String, String> getAdditionalFields(Document page) {
		HashMap<String, String> additionalFields = new HashMap<>();
		for (Element highLight : page.select(".item-highlights")) {
			//System.out.println("check " + highLight.text());
			if (highLight.text().equalsIgnoreCase(""))
				continue;
			String data = highLight.select("strong").first().text();
			additionalFields.put(highLight.text().replaceFirst(data, "").trim(), data);
		}
		for (Element option : page.select(".optionals")) {
			Elements optionsDT = option.select("dt");
			Elements optionsDD = option.select("dd");
			for (int i=0;i<optionsDT.size();i++) {
				additionalFields.put(optionsDT.get(i).text().replace(":", ""), optionsDD.get(i).text());
			}
		}
		return additionalFields;
	}
	
	
	
	ArrayList<String> getImages(Document page) {
		ArrayList<String> images = new ArrayList<>();
		Elements imgList = page.select(".big-nav-thumb");
		for (Element img: imgList)
		{
			Element link = img.select("a").first();
			if (link != null)
				images.add(link.attr("href"));
		}
		return images;
	}
}

