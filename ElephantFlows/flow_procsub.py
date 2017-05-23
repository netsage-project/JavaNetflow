#!/usr/bin/env python
# -*- coding: utf-8 -*-
import subprocess
import csv
import time
from datetime import datetime
#mport os
import json

"""
IF SCRIPT GIVES SYNTAX ERROR ETC RUN IT THROUGH AUTOPEP8 
TODO:
	1. Robustify code and output better error messages
	2. Refactoring
	3. Add date of flow when creating flow profile (Easy fix)
	4. Currently script only creates flow profiles for the first day
		eg if an elephant flow occurs for 2 days, the script will only create flow profiles for day 1
			need to add iteration so it goes to day 2
	5. In NFDUMP, some flows occur  in days before they actually occured.
		EG: If Get request claims flow occured on 28th Feb, there is a possibility the flow records are in March 1.
"""

"""
2 TABLES CREATED
ID IS ES SEARCH 5 TUPLES
start,duration,byte and ES
"""

DEBUG=False


def create_row(curr_row):
	"""CREATE ROW FOR WRITING TO CSV
	INPUT: ROW INPUT FROM csv
	OUTPUT: ROW FOR CSV [NEED TO ADD DATE OF FLOW STILL]
	"""
	crow_print=[curr_row[0].split(" ")[1], float(curr_row[2]), float(curr_row[12]),float(0)]
	if(float(curr_row[2]) != 0):
		crow_print[3]=((crow_print[2])/float(crow_print[1]))
	crow_print[1]=round(crow_print[1],3)
	crow_print[-1]=round(crow_print[-1],3)
	return crow_print

def debug(*str):
    """FANCY PRINTING FOR DEBUGGING PURPOSES
    INPUT: STRING
    OUTPUT: CONSOLE OUTPUT
    """
    if DEBUG:
        print(str)

def get_time(time_string):
    """
	PASS IN YEAR SPACE TIME AND RETURN TIME
	INPUT : YEAR TIME
	OUTPUT : TIME FORMATED TIME
    """

    time=time_string.split((" "))[1]
    debug(time)
#    debug(time.strptime(time, "%H:%M:%S"))

    date=datetime.strptime(time,"%H:%M:%S")
    debug("PRINTING IN FUNCTION")
    debug(date.time())
    return date.time()

def json_to_py(file):
	"""JSON -> PYTHON NATIVE
	INPUT: FILE TO JSONIFY
	OUTPUT:JSONIFIED FOR PYTHON
	"""
	return json.load((open(file)))


def create_var(str):
    """RETURN NFDUMP ARGUMENT FOR QUERYING
    INPUT: STRING TO PARSE
    EX: "ID,BYTES,CONN,DATE : START,DATE : END,SOURCEIP,SOURCEPORT,DESTPORT,DESTIP"
    OUTPUT: ID AND /YEAR AND ARGUMENT AND PROTOCL
    RETURN (ID,date,argument,protocol)
    """
    split_str = str.split(',')
    date=split_str[4].split()[0]
    formatted_date=date.split("-")
    debug(formatted_date)

    argument="src ip " + split_str[5] + " and " + "dst ip " + split_str[-1] + " and " + "src port " + split_str[6] + " and " + "dst port " + split_str[-2] + " and" + " proto " + split_str[2]
    debug(split_str[0],"/".join(formatted_date),argument,split_str[2])
    return (split_str[0],"/".join(formatted_date),argument,split_str[2])

PATH="/media/REMOTE_SHARES/flow-store/rtr.losa.transpac/"
COMMAND="nfdump"
R="-R"
O="-o"
FORMAT="csv"

def raw_nfdump(modcsv,path=PATH,command=COMMAND,r=R,o=O,form=FORMAT):
    """
    proc = subprocess.Popen([command,arg1,path,o,format,query],stdout=subprocess.PIPE)
    """

    date=create_var(modcsv)[1]

    query=create_var(modcsv)[2]
    #print(create_var(modcsv)[1])
    #print(create_var(modcsv)[2])
#    print("DATE IS " + date " AND " + "QUERY IS " + query)
    OUTPUT=""
    try:
	print([command,r,path+date,o,form,query])
        proc = subprocess.Popen([command,r,path+date,o,form,query],stdout=subprocess.PIPE)
	cut_csv=subprocess.Popen(["cut","--complement","-d",",","-f","14-48"],stdin=proc.stdout,stdout=subprocess.PIPE)
	#output,err=proc.communicate()
	proc.stdout.close()
	#print output
	modoutput,moderr = cut_csv.communicate()
	#print("PRINTING OUTPUT FOR " + str(cut_csv))
	#print(moderr)
	#print modoutput
	OUTPUT=modoutput

    except Exception as e:
        print(e)
    """
    return output.split("\n")
    this can be parsed then...
    """
    return OUTPUT.split("\n")



"""
PARSING JSON
"""
data = json_to_py("remote_curl")
actual_content = [content for content in data['hits']['hits']]
parsed_file=[]
for result in actual_content:
	#print((result['_id']))
	values = [data for data in result['_source'].values()]
	values.insert(0,result['_id'])
	#print(result['_source'])
#	print(",".join(values))
	parsed_file.append(",".join(values))
#	time.sleep(1)

for line in parsed_file:
	print line
#	time.sleep(1)
#print "File parsed"
print "LENGTH OF PARSED JSON IS: str(len(parsed_file))"
print len(parsed_file)
#parsed_json= open("sample_json","r")
#for line in parsed_json:
#	print line
#	time.sleep(1)
#time.sleep(2)
#print "Parsed Json"
flow_profiles=open("flowprofiles.csv",'wt')
flow_writer=csv.writer(flow_profiles)
six_tuples=open("tuple.csv",'wt')
six_tuples_writer=csv.writer(six_tuples)
for i,line in enumerate(parsed_file):
	print "ITERATION IS " + str(i)
	output=(raw_nfdump(line.rstrip("\n")))
	"""
	IF TCP LOOK AT OTHER THINGS TO DETERMINE IF STITCHING REQUIRED
	"ID,BYTES,CONN,DATE : START,DATE : END,SOURCEIP,SOURCEPORT,DESTPORT,DESTIP"
	('AVWd2AZjs7RyIojDgVsG', '2015/12/30', 'src ip 146.201.68.176 and dst ip 143.89.188.4 and src port 80 and dst port 56227 and proto TCP', 'TCP')
	"""
	#print line.rstrip("\n")
	#print(output[:-4])
	#	if(part.isdigit()):
	#		print part
	print output
	eid=create_var(line.rstrip("\n"))[0]
	proto=create_var(line.rstrip("\n"))[-1]
	#ftuple=line.rstrip("\n").split(" ")[2]
	six_tuple=line.rstrip("\n").split(" ")[2].split(",")
	six_tuple[0]=eid
	six_tuple[2]=int(six_tuple[2])
	six_tuple[3]=int(six_tuple[3])
	six_tuple[3] , six_tuple[4] = six_tuple[4] , six_tuple[3]
	six_tuple.append(proto)
	print(six_tuple)
	print("WRITING TUPLE TO FILE")
	six_tuples_writer.writerow(six_tuple)
	print("WRITEN")

#	time.sleep(1)
	#print(ftuple,"Printing t")
	#print(eid,proto)
	#print(create_var(line.rstrip("\n")))
#	(time.sleep(1))
#	stripped_output=[output[0:3],output[8],output[12]]
#	print("STRIPPED OUTPUT BELOW")
#	print(stripped_output)
#	time.sleep(1)
	fakecsv=csv.reader(output[:-3])

	AFLAG='.A....'
	APFLAG='.AP...'
	NFLAG='.....'
	try:
		fields = fakecsv.next()
	except Exception as e:
		print e
		continue
	try:
		curr_row = fakecsv.next()
	except Exception as e:
		print e
		continue
	try:
		next_row = fakecsv.next()
	except Exception as e:
		print e
		continue
	while(True):
		try:
			cflag = curr_row[8]
		except Exception as e:
			print e
			break
		try:
			nflag = next_row[8]
		except Exception as e:
			print e
			break
	#	print(curr_row)
#		print(cflag)
	#	print(next_row)
#		print(nflag)
		if(cflag == AFLAG and nflag == AFLAG):
#			print "SAME FLAGS. NO STITCHING..."
			crow_print=create_row(curr_row)
#			print "CREATED ABOVE"
#			time.sleep(2)
			crow_print.insert(0,eid)
			print(crow_print)
			print "WRITING ABOVE TO FILE"
			flow_writer.writerow(crow_print)
			print "WRITTEN"
			curr_row=next_row[:]
			try:
				next_row = fakecsv.next()
			except Exception as e:
				print e
				break

		elif(cflag == AFLAG and nflag == APFLAG):
			curr_rowts=get_time(curr_row[0])
		        next_rowts=get_time(next_row[0])
		        curr_rowte=get_time(curr_row[1])
		        next_rowte=get_time(next_row[1])
		        crow_print=[]
		        nrow_print=[]
			stitch=((curr_rowts<=next_rowts) and (curr_rowte>=next_rowte))
			crow_print=create_row(curr_row)
			if(stitch):
				print "CRITERIA MATCH.STITCHING TIME... "
				curr_row[12] = int(curr_row[12]) + int(next_row[12])
				crow_print=create_row(curr_row)

			else:
#				print "CRITERIA NOT MATCHED. NO STITCHING"

				nrow_print=create_row(next_row)
#				nrow_print=[next_row[0].split(" ")[1], float(next_row[2]), float(next_row[12]),float(0)]

    		#       print(crow_print)

			crow_print.insert(0,eid)
			print(crow_print)
			print "WRITING ABOVE TO FILE"
			flow_writer.writerow(crow_print)
			print "WRITTEN"
     		#        print(nrow_print)
		        if(stitch is False):
#			        print("STITCH IS FALSE")
				nrow_print.insert(0,eid)
				print(nrow_print)
				print "WRITING ABOVE TO FILE"
				flow_writer.writerow(nrow_print)
				print "WRITTEN"
       			try:
           			curr_row=fakecsv.next()
       			except Exception as e:
            			print e
           			break
       			try:
           			next_row=fakecsv.next()
       			except Exception as e:
           			print e
           			break
      #  time.sleep(2)
   		else:
#      			print "OTHER/EMPTY FLAG. JUST WRITE NORMALLY..."
			crow_print=create_row(curr_row)
			crow_print.insert(0,eid)
			nrow_print=create_row(next_row)
			nrow_print.insert(0,eid)
#			print(curr_row)
#			print(next_row)
			print(crow_print)
			print "WRITING ABOVE TO FILE"
			flow_writer.writerow(crow_print)
			print "WRITTEN"
			print(nrow_print)
			print "WRITING ABOVE TO FILE"
			flow_writer.writerow(nrow_print)
			print "WRITTEN"
			try:
				curr_row=fakecsv.next()
			except Exception as e:
				print e
				break
			try:
				next_row=fakecsv.next()
			except Exception as e:
				print e
				break
flow_profiles.close()
#flow_writer=csv.writer(flow_profiles)
six_tuples.close()
print "DONE"

