#parsing routing table from gloriad
#run in bash, and create outputfile in same folder

#ONLY WRITES BEST ROUTES in output file
#output file used by historyG.py later

import sys
import os
import time

file=sys.argv[1]
inputFileName = os.getcwd() +'/'+file
outputName=file.split('-')[-1]              #timestamp
timestamp=outputName.split('.')[0]

with open ((os.getcwd() +'/'+outputName,'w') as outputFile:
    with open (inputFileName,'r') as file:
        newLine=False
        
        for line in file:
            if (line.strip()[0:7]=='Network'):
                indexP=line.find('Path')     #index where path starts
                indexN=line.find('Network')
                indexNext=line.find('Next')
                indexM=line.find('Metric') 
                break
            
        for line in file:
            
            if '/' in line:                 #else: don't update network (same as above)
                network = line[indexN:indexNext].strip().split('/')   

            if '>' in line:                 #only deal with best route
                prefix=network[0]
                length=network[1]
                nextHop=line[indexNext:indexM].strip()
                path=line[indexP:-3].strip()#strip status field
                
                route=','.join([prefix,length,nextHop,path,timestamp])
    
                if (newLine):
                    outputFile.write('\n')
                outputFile.write(route)
                newLine=True                #write \n after first line
        
        
    

                
           
           
