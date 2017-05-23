
public class IpEntry {
	String ip;
	long bytes;
	public double getBytes() {
		return bytes;
	}

	public void setBytes(long bytes) {
		this.bytes = bytes;
	}
	String country;
	String region;
	String org;
	String as;
	String regionName;
	String city;
	String zip;
	String lat;
	String lon;
	String isp;
	
	
	public String toString(){
		return ip+"is "+"in country "+country+ "in region " +regionName+" " +city+ " "+ zip+ "organisation "+org;
		
	}
	
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public String getCountry() {
		return country;
	}
	public void setCountry(String country) {
		this.country = country;
	}
	public String getRegion() {
		return region;
	}
	public void setRegion(String region) {
		this.region = region;
	}
	public String getOrg() {
		return org;
	}
	public void setOrg(String org) {
		this.org = org;
	}
	public String getAs() {
		return as;
	}
	public void setAs(String as) {
		this.as = as;
	} 
}
