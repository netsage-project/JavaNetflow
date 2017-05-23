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

                               
public class IpDomainMapping {
	
	private static final String comma_Delemiter=",";
	private static final String NEW_LINE_SEPARATOR = "\n";

	
	
	private static final String Column_header="Ip,Total_Bytes,Country,Region Name,City,Zip,Latitude,Longitude,ISP,Org,ASNUM";
	
	
	
	public static void main(String[] args) throws ClientProtocolException,IOException, ParserConfigurationException, SAXException, InterruptedException {
		
		long startTime = System.currentTimeMillis();
		
		List<IpEntry> ipEntries=new ArrayList<>();
		HttpClient httpClient=new DefaultHttpClient();
				
		Scanner scanner=new Scanner(new File("/home/asampath/IPDomainMap/src/ace-y7q2-top-eu-dstip4-24-ipAndBytes"));
		scanner.useDelimiter("\n");
		
		
		//String[] ar=scanner.split(",");
		
		int count=1;
		int count_total=0;
		while(scanner.hasNext() & count_total <1000000)
		{
			IpEntry entry=new IpEntry();
			count++;
			count_total++;
			
			/*if(count==100000){
				count=-1;
				String timeStamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());
				System.out.println("time0 "+timeStamp);
				System.out.println("total :"+count_total);
				Thread.sleep(0);
			
				String timeStamp1 = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());
				System.out.println("time1 "+timeStamp1);
			}*/
			
			
			//todo
			//sleep for a time, do not want to time out. 
			
			String str=scanner.next();
			String []tempStr=str.split(",");
			entry.ip = tempStr[0].trim();
			System.out.println("iptemp is"+entry.ip);
			//entry.bytes=Double.parseDouble(tempStr[1]);
			entry.bytes=Long.parseLong(tempStr[1].trim());
			
			System.out.println("bytes is"+entry.bytes);
			
//			String country="";
//			String org="";
//			String as="";
//			String regionName="";
//			String city="";
//			String zip="";
//			String lat="";
//			String lon="";
			//System.out.println(ip);
			//String ip="158.210.220.206";
			//System.out.println("http://ip-api.com/xml/"+ip);
			
			// Subnet to avoid dUPLICATE 
			System.out.println("\n");
			System.out.println("IP is :"+entry.ip);
			
		

	

	
			//http://pro.ip-api.com/json/208.80.152.201?key=zTbNOOuTjoBXLZu
			HttpGet request=new HttpGet("http://pro.ip-api.com/xml/"+entry.ip+"?key=zTbNOOuTjoBXLZu");
			System.out.println("sending request");
			HttpResponse response=httpClient.execute(request);
			System.out.println("Response is"+response);
			
			//BufferedReader reader=new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
			InputStreamReader reader=new InputStreamReader(response.getEntity().getContent());



			DocumentBuilderFactory factory =DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			InputSource is = new InputSource(reader);
			Document doc=builder.parse(is);
			
			int nullAS=0;
			int emptyAS=0;


			NodeList nodeList = doc.getDocumentElement().getChildNodes();
			for (int i = 0; i < nodeList.getLength(); i++){
				Node node = nodeList.item(i);
				if (node instanceof Element) 
				{

					String content=node.getLastChild().getTextContent().trim();
					switch (node.getNodeName())
					{
					case "country":
						entry.country=content;
						//System.out.println("country is :"+ entry.country);
						break;

					case "regionName":
						entry.regionName=content;
						//System.out.println("regionName is :"+ entry.regionName);
						break;
					
					case "city":
						entry.city=content;
						//System.out.println("city is :"+ entry.city);
						break;
					
					case "zip":
						entry.zip=content;
						//System.out.println("zip is :"+ entry.zip);
						break;
					
					case "lat":
						entry.lat=content;
						//System.out.println("lat is :"+ entry.lat);
						break;
					
					case "lon":
						entry.lon=content;
						//System.out.println("lon is :"+ entry.lon);
						break;
					
					case "isp":
						entry.isp=content;
						//System.out.println("isp is :"+ entry.isp);
						break;	
						
					case "org":
						entry.org=content;
						//System.out.println("org is :"+ entry.org);
						break;
				

					case "as":
						entry.as=content;
						if(entry.as == ""){
							emptyAS++;
							System.out.println("Blank AS found");
							break;
						}
						else if (entry.as == "null") {
							nullAS++;
							System.out.println("null AS found - could be reserved IP");
							break;
						}
						else {
							System.out.println("as is :"+ entry.as);
							
						}
						
					}	
				}

				
			}
			//System.out.println("entry added");
			ipEntries.add(entry);
		}
		scanner.close();
		
		System.out.println("Done reading response!!!!");
		
		FileWriter fileWriter = null;
		fileWriter = new FileWriter("ace-y7q2-top-eu-dstip4-24-ipAndBytes-withASandDetails.csv");
		fileWriter.append(Column_header.toString());
		fileWriter.append(NEW_LINE_SEPARATOR);
		for (IpEntry ipentry:ipEntries)
		{
			fileWriter.append(String.valueOf(ipentry.ip));
			fileWriter.append(comma_Delemiter);
			
			fileWriter.append(String.valueOf(ipentry.bytes));
			fileWriter.append(comma_Delemiter);
			
			String country_str=String.valueOf(ipentry.country);
			String country_new_str=country_str.replaceAll(",","@");			
			fileWriter.append(country_new_str);
			fileWriter.append(comma_Delemiter);
		
			//fileWriter.append(String.valueOf(ipentry.regionName));
			String region_str=String.valueOf(ipentry.regionName);
			String region_new_str = region_str.replaceAll(",", "@");
			fileWriter.append(region_new_str);
			fileWriter.append(comma_Delemiter);
			//fileWriter.append(String.valueOf(ipentry.city));
			String city_str=String.valueOf(ipentry.city);
			String city_new_str = city_str.replaceAll(",", "-");
			fileWriter.append(city_new_str	);
			fileWriter.append(comma_Delemiter);
			fileWriter.append(String.valueOf(ipentry.zip));
			fileWriter.append(comma_Delemiter);
			fileWriter.append(String.valueOf(ipentry.lat));
			fileWriter.append(comma_Delemiter);
			fileWriter.append(String.valueOf(ipentry.lon));
			fileWriter.append(comma_Delemiter);
			String isp_str=String.valueOf(ipentry.isp);
			String isp_new_str = isp_str.replaceAll(",", "@");
			fileWriter.append(isp_new_str);
			//fileWriter.append(String.valueOf(ipentry.isp));
			fileWriter.append(comma_Delemiter);
			//isp 
			String my_str=String.valueOf(ipentry.org);
			String my_new_str = my_str.replaceAll(",", "@");
			fileWriter.append(my_new_str);
			fileWriter.append(comma_Delemiter);
			//trim
			String asp_str=String.valueOf(ipentry.as);
			//String asp_new_str = asp_str.trim();
			String asp_new_str = asp_str.split(" ")[0];
			fileWriter.append(asp_new_str);
			fileWriter.append(NEW_LINE_SEPARATOR);
			
			
		}
		
		fileWriter.flush();
		fileWriter.close();
		System.out.println("CSV file was created successfully !!!");
		long endTime   = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		System.out.println("total execution time is (in mins):" +(totalTime/60000) );
	}
}
