#parsing routing table from snmp
#run in bash, and create outputfile in same folder

#output file used by RT_comp2.py and history.py

import sys
import os, time

fileName = os.getcwd() +'/'+sys.argv[1]
time= time.strftime('%Y-%m-%d %H:%M:%S',time.gmtime(os.stat(fileName).st_birthtime) )  #UTC time

with open (os.getcwd() +'/'+'output_'+sys.argv[1],'w') as outputFile:
    with open (fileName,'r') as file:
        newLine=False
        for line in file:
            if (line.strip()==''): break
            line=line[56:]
            l= line.split('"')
            path =l[-2]
            li=line.split()[0].split('.')
            prefix='.'.join(li[0:4])
            length=li[4]
            nexthop='.'.join(li[7:11])

            if (newLine): outputFile.write('\n')
            outputFile.write(prefix+','+length+','+nexthop+','+path+','+time)
            newLine=True
        
    

                
           
           
