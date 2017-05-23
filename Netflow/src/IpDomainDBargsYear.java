import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class IpDomainDBargsYear {

	private static final String comma_Delemiter = ",";
	private static final String NEW_LINE_SEPARATOR = "\n";

	private static final String Column_header = "Ip,Total_Bytes,Country,Region Name,City,Zip,Latitude,Longitude,ISP,Org,ASNUM";
		
	public static Connection con;
	public IpDomainDBargsYear() throws ClassNotFoundException, SQLException{
		Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		con = DriverManager.getConnection("jdbc:mysql://localhost:3306/PrefixToAsNumDB", "root",
				"Mysql@123");
		
	}
	
	public static void main(String[] args) throws IOException, SAXException, ParserConfigurationException,ClassNotFoundException,SQLException{
		
		int httpCount=0;
		long startTime = System.currentTimeMillis();

		List<IpEntry> ipEntries = new ArrayList<>();
		HttpClient httpClient = new DefaultHttpClient();
		IpDomainDBargsYear ipargs=new IpDomainDBargsYear();
		// Scanner scanner=new Scanner(new
		// File("C:/Users/Abhi/workspace/IPDomainMap/src/ace-y2q3-top-us-dstip4-24-ipAndBytes1"));
		Scanner scanner = new Scanner(
				new File(args[0]));

		scanner.useDelimiter("\n");

		// String[] ar=scanner.split(",");

		int count = 1;
		int count_total = 0;
		while (scanner.hasNext() & count_total < 3000000) {
			IpEntry entry = new IpEntry();
			count++;
			count_total++;

			String str = scanner.next();
			String[] tempStr = str.split(",");
			entry.ip = tempStr[0].trim();
			if(entry.ip.length()<10){
				System.out.println("entry.ip.length()<10 is" + entry.ip);
				continue;
			}
			System.out.println("iptemp is" + entry.ip);
			entry.bytes = Long.parseLong(tempStr[1].trim());

			System.out.println("bytes is" + entry.bytes);

			System.out.println("\n");
			System.out.println("IP is :" + entry.ip);

			try {
				//Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
				//Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/PrefixToAsNumDB?autoReconnect=true&useSSL=false","root","Mysql@123");
				Statement stmt = con.createStatement();
				ResultSet rs = stmt.executeQuery("select * from IPV4PREFIXTOASNUM where ip='" + entry.ip + "'");
				if (!rs.isBeforeFirst()) {
					rs = stmt.executeQuery("select * from IPV6PREFIXTOASNUM where ip='" + entry.ip + "'");
				}
				if (!rs.next()) {

					HttpGet request = new HttpGet("http://pro.ip-api.com/xml/" + entry.ip + "?key=zTbNOOuTjoBXLZu");
					System.out.println("sending request");
					HttpResponse response = httpClient.execute(request);
					System.out.println("Response is" + response);
					httpCount++;	
					// BufferedReader reader=new BufferedReader(new
					// InputStreamReader(response.getEntity().getContent()));
					InputStreamReader reader = new InputStreamReader(response.getEntity().getContent());

					DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
					DocumentBuilder builder = factory.newDocumentBuilder();
					InputSource is = new InputSource(reader);
					Document doc = builder.parse(is);

					int nullAS = 0;
					int emptyAS = 0;

					NodeList nodeList = doc.getDocumentElement().getChildNodes();
					for (int i = 0; i < nodeList.getLength(); i++) {
						Node node = nodeList.item(i);
						if (node instanceof Element) {

							String content = node.getLastChild().getTextContent().trim();
							switch (node.getNodeName()) {
							case "country":
								entry.country = content;
								// System.out.println("country is :"+
								// entry.country);
								break;

							case "regionName":
								entry.regionName = content;
								// System.out.println("regionName is :"+
								// entry.regionName);
								break;

							case "city":
								entry.city = content;
								// System.out.println("city is :"+ entry.city);
								break;

							case "zip":
								entry.zip = content;
								// System.out.println("zip is :"+ entry.zip);
								break;

							case "lat":
								entry.lat = content;
								// System.out.println("lat is :"+ entry.lat);
								break;

							case "lon":
								entry.lon = content;
								// System.out.println("lon is :"+ entry.lon);
								break;

							case "isp":
								entry.isp = content;
								// System.out.println("isp is :"+ entry.isp);
								break;

							case "org":
								entry.org = content;
								// System.out.println("org is :"+ entry.org);
								break;

							case "as":
								entry.as = content;
								if (entry.as == "") {
									emptyAS++;
									System.out.println("Blank AS found");
									break;
								} else if (entry.as == "null") {
									nullAS++;
									System.out.println("null AS found - could be reserved IP");
									break;
								} else {
									System.out.println("as is :" + entry.as);

								}

							}
						}

					}
					// System.out.println("entry added");
					// ipEntries.add(entry);

				} else {
					do {
						System.out.println("Response is");
						System.out.println(rs.getString(1) + "  " + rs.getString(3) + "  " + rs.getString(4) + " "
								+ rs.getString(5) + " " + rs.getString(6) + " " + rs.getString(7) + " "
								+ rs.getString(8) + " " + rs.getString(9) + " " + rs.getString(10) + " "
								+ rs.getString(11));

						entry.country = rs.getString(3);
						entry.regionName = rs.getString(4);
						System.out.println("region name" + entry.region);

						entry.city = rs.getString(5);
						entry.zip = rs.getString(6);
						entry.lat = rs.getString(7);
						entry.lon = rs.getString(8);
						entry.isp = rs.getString(9);
						entry.org = rs.getString(10);
						entry.as = rs.getString(11);

						break;

					} while (rs.next());
				}

				//con.close();
				ipEntries.add(entry);

			}
			 catch (SQLException e) {

				e.printStackTrace();
			}

		}
		scanner.close();
		con.close();

		System.out.println("Done reading response!!!!");

		FileWriter fileWriter = null;
		fileWriter = new FileWriter(args[0]+"withASandDetails.csv");
		fileWriter.append(Column_header.toString());
		fileWriter.append(NEW_LINE_SEPARATOR);
		for (IpEntry ipentry : ipEntries) {
			fileWriter.append(String.valueOf(ipentry.ip));
			fileWriter.append(comma_Delemiter);

			fileWriter.append(String.valueOf(ipentry.bytes));
			fileWriter.append(comma_Delemiter);

			fileWriter.append(String.valueOf(ipentry.country));
			fileWriter.append(comma_Delemiter);
			// fileWriter.append(String.valueOf(ipentry.regionName));

			String region_str = String.valueOf(ipentry.regionName);
			String region_new_str = region_str.replaceAll(",", "@");
			//System.out.println("Region new string***************** " + region_new_str);
			fileWriter.append(region_new_str);

			fileWriter.append(comma_Delemiter);
			// fileWriter.append(String.valueOf(ipentry.city));
			String city_str = String.valueOf(ipentry.city);
			String city_new_str = city_str.replaceAll(",", "-");
			fileWriter.append(city_new_str);
			fileWriter.append(comma_Delemiter);
			fileWriter.append(String.valueOf(ipentry.zip));
			fileWriter.append(comma_Delemiter);
			fileWriter.append(String.valueOf(ipentry.lat));
			fileWriter.append(comma_Delemiter);
			fileWriter.append(String.valueOf(ipentry.lon));
			fileWriter.append(comma_Delemiter);
			String isp_str = String.valueOf(ipentry.isp);
			String isp_new_str = isp_str.replaceAll(",", "@");
			fileWriter.append(isp_new_str);
			// fileWriter.append(String.valueOf(ipentry.isp));
			fileWriter.append(comma_Delemiter);
			// isp
			String my_str = String.valueOf(ipentry.org);
			String my_new_str = my_str.replaceAll(",", "@");
			fileWriter.append(my_new_str);
			fileWriter.append(comma_Delemiter);
			// trim
			String asp_str = String.valueOf(ipentry.as);
			// String asp_new_str = asp_str.trim();
			String asp_new_str = asp_str.split(" ")[0];
			fileWriter.append(asp_new_str);
			fileWriter.append(NEW_LINE_SEPARATOR);

		}

		fileWriter.flush();
		fileWriter.close();
		System.out.println("CSV file was created successfully !!!");
		long endTime = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		System.out.println("Total calls made to ip-api.com is" +httpCount);
		System.out.println("total execution time is (in mins):" + (totalTime / 60000));

	}
}
