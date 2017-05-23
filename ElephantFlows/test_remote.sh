#!/bin/sh

bytesize=1
query='{ \"_source\" : [\"date_start\",\"date_end\",\"source_ip\",\"dest_ip\",\"source_port\",\"dest_port\",\"protocol\",\"input_byte\"], \"from\" : 0, \"size\" : 10000, \"query\" : { \"constant_score\" : { \"filter\" : { \"range\" : { \"input_byte\" : { \"gte\" : 10737412824  } } } } } }'

q='{ \"_source\" : [\"date_start\",\"date_end\",\"source_ip\",\"dest_ip\",\"source_port\",\"dest_port\",\"protocol\",\"input_byte\"], \"from\" : 0, \"size\" : 10000, \"query\" : { \"constant_score\" : { \"filter\" : { \"range\" : { \"input_byte\" : { \"gte\" : '
q="$q$bytesize  } } } } } }"
echo $bytesize
echo $q


echo $query
#ssh aykohli@156.56.6.121 ls
#request_output=$(ssh aykohli@156.56.6.121 "curl -i -XGET http://localhost:9200/filebeat*/_search?pretty -d \"$query\"")
ssh aykohli@156.56.6.121 "curl -i -XGET http://localhost:9200/filebeat*/_search?pretty -d \"$q\"" > SAMPLE
request_success=$(grep -c "200 OK" SAMPLE)
#more SAMPLE
#request_success=$(echo "${request_output}" | grep -c "200 OK") 
if [ $request_success -eq 1 ];
	then
		echo "Successful GET request"
		#echo "Removing first  5-1 lines of file"
		cat SAMPLE | tail -n +5 > remote_curl
		rm SAMPLE 
		#echo "Removed"
		echo "Created file remote_curl to be parsed"
		echo "Running flow_profiles.py"
		
		echo "Finished running flow_profiles.py
	else
		echo "Failed GET request"
		exit
		echo "THIS SHOULD NOT BE PRINTED"
fi
#echo $request_output > Sample_output

