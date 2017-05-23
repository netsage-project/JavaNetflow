#!/usr/bin/env python
"""
TESTING PARSING.THIS IS LATER IMPLEMENTED IN flow_procsub.py

"""
#Purpose: Scrape relevant data from JSON file

#Libraries
import csv
import json
import sys
import time
#from pprint import pprint

#Constants
DEBUG = False
"""
query='{
 "_source" : ["date_start","date_end","source_ip","dest_ip","source_port","dest_port","protocol","input_byte"],
 "from" : 0, "size" : 10000,
      "query" : {
        "constant_score" : {
          "filter" : {
            "range" : {
              "input_byte" : {
                "gte" : 10737412824
              }
            }
          }
        }
      }
  }'
"""

def debug(str):
    """Fancy print
    INPUT: String
    OUTPUT: Console output
    """
    if DEBUG:
        print(str)

def json_to_py(file):
    """JSON -> Python native
    INPUT:File to jsonify
    OUTPUT:Jsonified file
    """
    #fp=open(file,"r").read()
    #print fp
    #print type(fp)
    return json.load((open(file)))

debug("BEFORE OPENING JSON")
debug(json_to_py("SAMPLE2"))
debug("AFTER OPENING JSON FILE")
data = json_to_py("SAMPLE2")
#(data.keys())
#debug(pprint(data['hits']['hits']))

actual_content = [content for content in data['hits']['hits']]
#csv_output=open(sys.argv[1],'wt')
#writer=csv.writer(csv_output)
for result in actual_content:
    #pprint(result)

    debug((result['_id']))
    values = [data for data in result['_source'].values()]
    values.insert(0,result['_id'])
    debug(result['_source'])
    #writer.writerow(values)
    debug(len(values))
    print values
#csv_output.close()
