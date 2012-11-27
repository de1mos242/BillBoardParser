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
import java.sql.SQLException;
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
		
		dbStatement.execute("create table if not exists 'exported_pages' ('id' INTEGER PRIMARY KEY AUTOINCREMENT, 'filename' string);");
		dbStatement.execute("create unique index if not exists 'filename_index' on 'exported_pages' ('filename') ");
		
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
				
				ArrayList<Boolean> cruchFoundLoaded = new ArrayList<Boolean>();
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
		ArrayList<String> parsedFiles = new ArrayList<String>();
		ResultSet rs = dbStatement.executeQuery("select distinct filename from parsed_page");
		while (rs.next()) {
			parsedFiles.add(rs.getString(1));
		}
		rs.close();
		
		ArrayList<String> workSet = new ArrayList<String>();
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
			try
			{
				if (new File(filename).length() == 0L) {
					System.out.println("file skipped because it's empty!");
					continue;
				}
				HashMap<String, Object> parsedData = parsePage(filename);
				if (parsedData.size()==0)
					continue;
				SaveParsedData(parsedData, filename);
			}
			catch (Exception e) {
				System.out.println("failure " + e.getMessage());
			}
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
		HashMap<String, Object> pageData = new HashMap<String, Object>();
		Document page = Jsoup.parse(new File(filename), "utf-8");
		HashMap<String, String> commonFields = getCommonFields(page);
		if (commonFields.size() == 0)
			return pageData;
		pageData.put("commonFields", commonFields);
		pageData.put("userData", getUserFields(page));
		pageData.put("additionalFields", getAdditionalFields(page));
		pageData.put("images", getImages(page));
		return pageData;
	}
	
	HashMap<String, String> getCommonFields(Document page) {
		HashMap<String, String> commonFields = new HashMap<String, String>();
		Element type = page.select("#firstpath1").first();
		if (type == null)
			return commonFields;
		commonFields.put("Type", type.text());
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
		HashMap<String, String> userFields = new HashMap<String, String>();
		userFields.put("UserNick", page.select(".user-wrapper").first().text());
		Element phone = page.select(".phone").first();
		if (phone != null)
			userFields.put("UserPhone", phone.text());
		return userFields;
	}
	
	HashMap<String, String> getAdditionalFields(Document page) {
		HashMap<String, String> additionalFields = new HashMap<String, String>();
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
		ArrayList<String> images = new ArrayList<String>();
		Elements imgList = page.select(".big-nav-thumb");
		for (Element img: imgList)
		{
			Element link = img.select("a").first();
			if (link != null)
				images.add(link.attr("href"));
		}
		return images;
	}
	
	public void ExportDML() throws SQLException, IOException {
		System.out.println("preparing worked pages");
		ArrayList<String> exportedFiles = new ArrayList<String>();
		ResultSet rs = dbStatement.executeQuery("select filename from exported_pages");
		while (rs.next()) {
			exportedFiles.add(rs.getString(1));
		}
		rs.close();
		
		System.out.println("calculating pages to work");
		HashMap<String, HashMap<String, HashMap<String, String>>> workSet = new HashMap<String, HashMap<String,HashMap<String,String>>>();
		//ArrayList<String> workSet = new ArrayList<String>();
		rs = dbStatement.executeQuery("select filename, type, name, data from parsed_page");
		while(rs.next()) {
			String tempFilename = rs.getString(1);
			if (!exportedFiles.contains(tempFilename)) {
				if (!workSet.containsKey(tempFilename))
				{
					workSet.put(tempFilename, new HashMap<String, HashMap<String, String>>());
					workSet.get(tempFilename).put("commonPart", new HashMap<String, String>());
					workSet.get(tempFilename).put("userData", new HashMap<String, String>());
					workSet.get(tempFilename).put("additionalFields", new HashMap<String, String>());
				}
				String type = rs.getString(2);
				String name = rs.getString(3);
				String data = rs.getString(4);
				if (type.equals("commonField")) {
					workSet.get(tempFilename).get("commonPart").put(name, data);
				}
				else if (type.equals("userData")) {
					workSet.get(tempFilename).get("userData").put(name, data);
				}
				else if (type.equals("additionalFields")) {
					workSet.get(tempFilename).get("additionalFields").put(name, data);
				}
			}
		}
		rs.close();
		
		System.out.println("exporting...");
		
		PreparedStatement insertStatment = dbConnection.prepareStatement("insert into 'exported_pages' ('filename') values (?);");
		
		exportedFiles.clear();
		BufferedWriter exporter = new BufferedWriter(new FileWriter(new File("insert_statements.sql"), false));
		int maxcount = 10;
		int counter = 0;
		int i=0;
		for (Entry<String, HashMap<String, HashMap<String, String>>> workHash : workSet.entrySet()) {
			String filename = workHash.getKey();
			if (counter == 0)
			{
				exporter.write(GetInsertHeader());
				exporter.newLine();
			}
			System.out.println("export file " + filename);
			try
			{
				exporter.write(exportSinglePage(workHash.getValue()));
				insertStatment.setString(1, filename);
				insertStatment.addBatch();
				counter++;
				if (counter < maxcount && i<workSet.size()-1)
				{
					exporter.write(",");
				}
				else
				{
					insertStatment.executeBatch();
					exporter.write(";");
					exporter.newLine();
					counter = 0;
				}
				exporter.newLine();
			}
			catch (Exception e) {
				System.out.println("failure " + e.getMessage());
			}
			exporter.flush();
		}
		insertStatment.close();
		exporter.close();
	}
	
	String exportSinglePage(HashMap<String, HashMap<String, String>> entry) throws Exception {
		//'id' 'type' 'name' 'data' 'filename'
		HashMap<String, String> commonPart = entry.get("commonPart");
		HashMap<String, String> userData = entry.get("userData");
		HashMap<String, String> additionalFields = entry.get("additionalFields");
		
		String category, Author, title, phone, text, price, catFields;
		if (categoryMap.containsKey(commonPart.get("Type")+"||" + commonPart.get("SubType")))
		{
			String catblock = categoryMap.get(commonPart.get("Type")+"||"+commonPart.get("SubType"));
			category = catblock.replace('|', '>') .split(">>")[1];
		}
		else
		{
			throw new Exception("Not found category: " + commonPart.get("Type")+" -> " + commonPart.get("SubType"));
		}
		
		Author = userData.get("UserNick");
		if (userData.containsKey("UserPhone"))
			phone = userData.get("UserPhone");
		else
			phone = "";
		
		title = commonPart.get("Subject");
		
		if (additionalFields.containsKey("Цена")) {
			String tempPrice = additionalFields.get("Цена");
			tempPrice = tempPrice.split("р")[0].replaceAll(" ", "");
			price = String.valueOf(Integer.parseInt(tempPrice));
		}
		else
		{
			price = "0";
		}
		
		catFields = "";
		text = commonPart.get("Description");
		for (Entry<String, String> addition : additionalFields.entrySet()) {
			//text += ", " + addition.getKey() + ": " + addition.getValue();
			catFields += "<b>"+addition.getKey()+":</b> <span>" + addition.getValue() + "</span><br/>";
		}
		
		
		String res = GetInsertLine(category, Author, title, phone, text, price, catFields);
		return res;
	}
	
	String GetInsertLine(String category, String Author, String title, String phone,
			String text, String price, String catFields)
	{
		String res = "("+category+",'"+Author+"','"+title+"','','Красноярск', 1,'','"+
				phone + "','"+text+"',"+price+",'','','"+curTS+"','"+catFields+"')";
		return res;
	}
	
	String GetInsertHeader()
	{
		return "INSERT INTO `jb_board` (`id_category`, `autor`, `title`, `email`, `city`, `city_id`, `url`, " +
				"`contacts`, `text`, `price`, `video`, `tags`, `date_add`, `cat_fields`) VALUES ";
	}
	
	HashMap<String, String> categoryMap = generateCategoryMap();
	
	private HashMap<String, String> generateCategoryMap() {
		HashMap<String, String> map = new HashMap<String, String>();
		map.put("Бизнес и промышленность||Металлы","8||86");
		map.put("Бизнес и промышленность||Пищевая промышленность, продукты питания","8||198");
		map.put("Бизнес и промышленность||Лесная и деревообрабатывающая промышленность","11||89");
		map.put("Бизнес и промышленность||Продается бизнес","9||184");
		map.put("Бизнес и промышленность||Строительство","11||193");
		map.put("Бизнес и промышленность||Другие предложения","11||193");
		map.put("Бизнес и промышленность||Оборудование","8||86");
		map.put("Бизнес и промышленность||Опт, поставки, импорт-экспорт","12||217");
		map.put("Бизнес и промышленность||Нефть, газ, уголь","1||175");
		map.put("Бизнес и промышленность||Химия","8||86");
		map.put("Бизнес и промышленность||ИТ, Интернет, связь","12||219");
		map.put("Куплю - Продам||Животные и растения","2||71");
		map.put("Куплю - Продам||Детские товары","8||108");
		map.put("Куплю - Продам||Компьютеры и комплектующие","6||230");
		map.put("Куплю - Продам||Все для дома и сада","2||100");
		map.put("Куплю - Продам||Одежда, обувь, аксессуары","8||105");
		map.put("Куплю - Продам||Все для офиса","2||102");
		map.put("Куплю - Продам||Все остальное","8||86");
		map.put("Куплю - Продам||Распродажа","205||209");
		map.put("Куплю - Продам||Музыкальные инструменты","7||239");
		map.put("Куплю - Продам||Электроника и техника","4||179");
		map.put("Куплю - Продам||Красота и здоровье","8||246");
		map.put("Куплю - Продам||Билеты","205||214");
		map.put("Куплю - Продам||Книги, учебники и журналы","7||203");
		map.put("Куплю - Продам||Игрушки, игры","6||241");
		map.put("Куплю - Продам||Сотовые телефоны","4||69");
		map.put("Куплю - Продам||Фото и видео камеры","4||65");
		map.put("Куплю - Продам||Товары для спорта - велосипеды","7||74");
		map.put("Куплю - Продам||Искусство, Коллекционирование, Хобби","7||70");
		map.put("Куплю - Продам||Видеоигры - консоли","6||243");
		map.put("Куплю - Продам||Ювелирные украшения, часы","8||107");
		map.put("Куплю - Продам||Музыка и фильмы","7||239");
		map.put("Недвижимость||Продажа квартир, домов","3||95");
		map.put("Недвижимость||Аренда квартир, домов","3||180");
		map.put("Недвижимость||Гаражи, стоянки","3||98");
		map.put("Недвижимость||Аренда и продажа магазинов","3||99");
		map.put("Недвижимость||Офисы, торговые площади","3||99");
		map.put("Недвижимость||Аренда комнат - совместная аренда квартир","3||180");
		map.put("Недвижимость||Земельные участки","3||97");
		map.put("Недвижимость||Обмен недвижимости","3||186");
		map.put("Недвижимость||Аренда недвижимости на курортах","3||180");
		map.put("Образование||Другие курсы и тренинги","9||93");
		map.put("Образование||Репетиторы - Частные уроки","9||93");
		map.put("Образование||Изучение языков","9||93");
		map.put("Образование||Компьютерные курсы","9||93");
		map.put("Образование||Музыкальные, театральные и танцевальные школы","9||93");
		map.put("Общество||Потерянное и найденное","205||211");
		map.put("Общество||События","205||214");
		map.put("Общество||Общественная деятельность","205||214");
		map.put("Общество||Музыканты, группы, DJ, артисты","205||208");
		map.put("Общество||Волонтеры, добровольцы","205||214");
		map.put("Работа / Вакансии||Работа с клиентами","9||92");
		map.put("Работа / Вакансии||Строительство, архитектура","9||92");
		map.put("Работа / Вакансии||Финансы, бухгалтерия, банки","9||92");
		map.put("Работа / Вакансии||Управление персоналом","9||92");
		map.put("Работа / Вакансии||Медицина, фармацевтика","9||92");
		map.put("Работа / Вакансии||Недвижимость","9||92");
		map.put("Работа / Вакансии||Логистика, транспорт","9||92");
		map.put("Работа / Вакансии||Продажи, закупки","9||92");
		map.put("Работа / Вакансии||Прочие сферы деятельности","9||92");
		map.put("Работа / Вакансии||ИТ, Интернет, связь","9||92");
		map.put("Работа / Вакансии||Розничная торговля","9||92");
		map.put("Работа / Вакансии||Маркетинг, Реклама, PR","9||92");
		map.put("Работа / Вакансии||Производство","9||92");
		map.put("Работа / Вакансии||Бары, рестораны","9||92");
		map.put("Работа / Вакансии||Образование, тренинги","9||92");
		map.put("Работа / Вакансии||Без спецнавыков, физический труд","9||92");
		map.put("Работа / Вакансии||Секретариат, административный персонал","9||92");
		map.put("Работа / Вакансии||Руководство, топ-менеджмент","9||92");
		map.put("Работа / Вакансии||Юриспруденция","9||92");
		map.put("Работа / Вакансии||Искусство, развлечения, масс-медиа","9||92");
		map.put("Работа / Вакансии||Работа в гостиничном и туристическом бизнесе","9||92");
		map.put("Работа / Вакансии||Некоммерческие организации, благотворительность","9||92");
		map.put("Транспорт||Грузовые автомобили - коммерческие автомобили","1||51");
		map.put("Транспорт||Легковые автомобили","1||50");
		map.put("Транспорт||Автозапчасти","1||55");
		map.put("Транспорт||Другой транспорт","1||54");
		map.put("Транспорт||Лодки, Яхты","1||52");
		map.put("Транспорт||Фургоны - дома на колесах - трейлеры","1||54");
		map.put("Транспорт||Мотоциклы - скутеры","1||52");
		map.put("Услуги||Прочие услуги","12||187");
		map.put("Услуги||Красота, здоровье, фитнес","12||76");
		map.put("Услуги||Ремонтные работы","12||201");
		map.put("Услуги||Переезд, хранение","12||79");
		map.put("Услуги||Организация праздников, видео и фотосъемка","12||77");
		map.put("Услуги||Няни, домработницы","9||91");
		map.put("Услуги||Хозяйство - Помощь по дому","12||216");
		map.put("Услуги||Переводы, редактирование, копирайтинг","12||199");
		map.put("Услуги||Интернет, компьютеры","12||219");
		map.put("Услуги||Кастинг, прослушивание","205||214");
		return map;
	}
	
	String curTS = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(java.util.Calendar.getInstance ().getTime());
}

