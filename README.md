# JavaRepo


Stitching files are coded by Sydney for stitching files across day for Transpac Data. 
BGP and Elephant Flows were also written by ayush and tina

Netflow was written by Abhinandan : 

Explanation of files 

IpDomainDB.java, IpDomainDBargs.java, IpDomainDBargsYear.java are almost same files. IpDomainDBargs.java takes filename as argument
and can be used for running java programs in parallel( i.e , javaThreadDb use this program). Also the connection to mysql is created in
constructor of the class. Use IpDomainDBargsYear.java for running data which has one year of data. IpDomainDB.java is for basic testing 
and the input filename and output filename has to be changed in the code

IpDomainMapping.java is where we do not contact the database and query ip-api.com for all prefixes.

IpEntry.java is more like a strucuture. We do not have to change this unless we want more details for an IP in future. 
