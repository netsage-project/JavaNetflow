#!/usr/bin/env python
# -*- coding: utf-8 -*-
import subprocess
import csv
import time
from datetime import datetime
#mport os
import json
import sys


"""
Version 1
"""
"""
2 TABLES CREATED
ID IS ES SEARCH 5 TUPLES
start,duration,byte and ES
"""

DEBUG=False

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
    date=datetime.strptime(time,"%H:%M:%S")
#    debug("PRINTING IN FUNCTION")
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
    #debug(create_var(modcsv)[1])
    #debug(create_var(modcsv)[2])
    debug("DATE IS " + date " AND " + "QUERY IS " + query)
    OUTPUT=""
    try:
	print([command,r,path+date,o,form,query]) #Command to be executed
        proc = subprocess.Popen([command,r,path+date,o,form,query],stdout=subprocess.PIPE)
	cut_csv=subprocess.Popen(["cut","--complement","-d",",","-f","14-48"],stdin=proc.stdout,stdout=subprocess.PIPE)#Cut command to strip out output not required
	proc.stdout.close()
	modoutput,moderr = cut_csv.communicate() #retrieve output and error if any
	OUTPUT=modoutput

    except Exception as e:
        print(e)
	print "SUBPROCESS FAILED"
	print "EXIT FAILURE"
	sys.exit(1)

    """
    return output.split("\n")
    this can be parsed then...
    """
    return OUTPUT.split("\n")


#PATH="/media/REMOTE_SHARES/flow-store/rtr.losa.transpac/"
#COMMAND="nfdump"
#R="-R"
#O="-o"
#FORMAT="csv"


"""
PARSING JSON
"""
data = json_to_py("remote_curl")
actual_content = [content for content in data['hits']['hits']]#navigate to only json part of data (skip elasticsearch server stats)
parsed_file=[]
for result in actual_content:
	debug((result['_id']))
	values = [data for data in result['_source'].values()]#strip all the values
	values.insert(0,result['_id'])
	debug(result['_source'])
	debug(",".join(values))
	parsed_file.append(",".join(values))
#	if(DEBUG):
#		time.sleep(1)

for line in parsed_file:
	debug(line)
	if(DEBUG):
		time.sleep(0.01)
debug("File parsed")
if(DEBUG):
	time.sleep(4)


flow_profiles=open("flowprofiles.csv",'wt')
flow_writer=csv.writer(flow_profiles)
six_tuples=open("tuple.csv",'wt')
six_tuples_writer=csv.writer(six_tuples)
for line in parsed_file:
	#CREATE FLOWPROFILE
	output=(raw_nfdump(line.rstrip("\n"))) #creating flow_profile for each record parsed from json object
	#DONE
	"""
	"ID,BYTES,CONN,DATE : START,DATE : END,SOURCEIP,SOURCEPORT,DESTPORT,DESTIP"
	('AVWd2AZjs7RyIojDgVsG', '2015/12/30', 'src ip 146.201.68.176 and dst ip 143.89.188.4 and src port 80 and dst port 56227 and proto TCP', 'TCP')
	"""

	"""
	TUPLE FILE LIKE BELOW
	EID,SRCIP,SCRAS,DSTIP,DSTAS,PROTOCOL
	AVXglyhApmnqJAYbIQ5Z,130.14.29.95,33001,143.89.47.112,50606,UDP

	"""
	#CREATE 6 TUPLE
	eid=create_var(line.rstrip("\n"))[0]
	proto=create_var(line.rstrip("\n"))[-1]
	six_tuple=line.rstrip("\n").split(" ")[2].split(",") #creating 5+1_tuple
	six_tuple[0]=eid
	six_tuple[2]=int(six_tuple[2])
	six_tuple[3]=int(six_tuple[3])
	six_tuple[3] , six_tuple[4] = six_tuple[4] , six_tuple[3]
	six_tuple.append(proto)
	#DONE
	print(six_tuple)
	print("WRITING TUPLE TO FILE")
	six_tuples_writer.writerow(six_tuple)
	print("WRITEN")
	if(DEBUG):
		time.sleep(1)
	fakecsv=csv.reader(output[:-4]) #create csv object for file
	AFLAG='.A....'
	APFLAG='.AP...'
	NFLAG='.....'
	fields = fakecsv.next()
	curr_row = fakecsv.next()
	next_row = fakecsv.next()


	#LOOP THROUGH EACH RECORD TO DETERMING STITCHING AND WRITE ACCORDINGLY
	while(True):
		try:
			cflag = curr_row[8]#get curr row flag value
		except Exception as e:
			print e
			break
		try:
			nflag = next_row[8]#get next row flag value
		except Exception as e:
			print e
			break
		debug(curr_row)
		debug(cflag)
		debug(next_row)
		debug(nflag)

		if(DEBUG):
			time.sleep(2)

		"""
		Dynamically reading 2 rows : curr_row and next_row
		There are three scenarios with flags:
			1.
				A
				A
				Write curr_row (First A)
				Make curr_row = next_row (Second A)
				Continue

			2.
				A
				AP
				Check time of flows and see if AP fits in A
					If fits:
						stitch next_row ( AP record) into curr_row (A record)
						write curr_row
					else:
						write curr_row
						write next_row
			3.
				N
				N
					write curr_row
					write next_row

		"""
		if(cflag == AFLAG and nflag == AFLAG):


#		Same flags no stitching

#		crow_print=csv line output
#		crow_print=eid,time,duration,bytes,rate=(bytes/time)

			debug("SAME FLAGS. NO STITCHING...")
			crow_print=[curr_row[0].split(" ")[1], float(curr_row[2]), float(curr_row[12]),float(0)]#creating record to store in csv file
			if(float(curr_row[2]) != 0):
				crow_print[3]=((crow_print[2])/float(crow_print[1]))#if duration not 0, create rate
			crow_print[1]=round(crow_print[1],3)#rounding duration
			crow_print[-1]=round(crow_print[-1],3)#rounding bytes
			crow_print.insert(0,eid)
			debug(crow_print)
			debug("WRITING ABOVE TO FILE")
			flow_writer.writerow(crow_print)
			debug("WRITTEN")
			curr_row=next_row[:]#make next_row values in curr_row and move on
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
			if(stitch):
				debug("CRITERIA MATCH.STITCHING TIME... ")
				curr_row[12] = int(curr_row[12]) + int(next_row[12])
				curr_row[2] = float(curr_row[2])
				crow_print = [curr_row[0].split(" ")[1], float(curr_row[2]), float(curr_row[12]),float(0)]
				if(float(curr_row[2]) != 0):
                			crow_print[3]=(float(crow_print[2])/float(crow_print[1]))
			else:
				debug("CRITERIA NOT MATCHED. NO STITCHING")
				crow_print=[curr_row[0].split(" ")[1], float(curr_row[2]), float(curr_row[12]),float(0)]
				if(float(curr_row[2]) != 0):
                			crow_print[3]=round(float(crow_print[2])/float(crow_print[1]),3)
				nrow_print=[next_row[0].split(" ")[1], float(next_row[2]), float(next_row[12]),float(0)]
				if(float(next_row[2]) != 0):
                			nrow_print[3]=((float(nrow_print[2])/float(nrow_print[1])))
			debug(curr_row)
		        debug(next_row)
    		        debug(crow_print)
			crow_print[1]=round(crow_print[1],3)
                        crow_print[-1]=round(crow_print[-1],3)
			crow_print.insert(0,eid)
			debug(crow_print)
			debug("WRITING ABOVE TO FILE")
			flow_writer.writerow(crow_print)
			debug("WRITTEN")
     		        debug(nrow_print)
			if(DEBUG):
				time.sleep(2)
		        if(stitch is False):
			        debug("STITCH IS FALSE")
				nrow_print[1]=round(nrow_print[1],3)
                       		nrow_print[-1]=round(nrow_print[-1],3)
				nrow_print.insert(0,eid)
				debug(nrow_print)
				debug("WRITING ABOVE TO FILE")
				flow_writer.writerow(nrow_print)
				debug("WRITTEN")
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
			if(DEBUG):
				time.sleep(2)
      #  time.sleep(2)
   		else:
      			debug("OTHER/EMPTY FLAG. JUST WRITE NORMALLY...")
			crow_print=[curr_row[0].split(" ")[1], float(curr_row[2]), float(curr_row[12]),float(0)]
			if(float(curr_row[2]) != 0):
				crow_print[3]=(round(float(crow_print[2])/float(crow_print[1]),3))
			nrow_print=[next_row[0].split(" ")[1], float(next_row[2]), float(next_row[12]),float(0)]
			if(float(next_row[2]) != 0):
				nrow_print[3]=((float(nrow_print[2])/float(nrow_print[1])))
			crow_print[1]=round(crow_print[1],3)
                        crow_print[-1]=round(crow_print[-1],3)
			nrow_print[1]=round(nrow_print[1],3)
                        nrow_print[-1]=round(nrow_print[-1],3)
			crow_print.insert(0,eid)
			nrow_print.insert(0,eid)
			debug(curr_row)
			debug(next_row)
			debug(crow_print)
			debug("WRITING ABOVE TO FILE")
			flow_writer.writerow(crow_print)
			debug("WRITTEN")
			debug(nrow_print)
			debug("WRITING ABOVE TO FILE")
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
	"""
			[8] is flag
			[ts] is time start
			[te] is time end
	"""
flow_profiles.close()
six_tuples.close()
print "DONE CREATING FLOW PROFILES"
