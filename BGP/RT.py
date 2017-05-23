#parsing routing table from terminal
#run in bash, and create outputfile in same folder

import sys
import os, time

fileName = os.getcwd() +'/'+sys.argv[1]
#UTC time in mysql format
time= time.strftime('%Y-%m-%d %H:%M:%S',time.gmtime(os.stat(fileName).st_birthtime) )  

with open (os.getcwd() +'/'+'output_'+sys.argv[1],'w') as outputFile:
    with open (fileName,'r') as file:
        for line in file:
            l=line.strip()
            if (l[0:6]=='Prefix'):
                break
        for line in file:
            l= line.strip().strip("\\")
            if (l[0:2]!='AS'):  #first line
                r =l.split()
            else:               #second line
                r.append(l[9:]) #add path
                if (len(r)!=8):
                    r.insert(3,'')
                r.append(time)
                routing = ','.join(r).replace('/',',')
                if (routing[0:2]!='1,'):
                    outputFile.write('\n')
                outputFile.write(routing)
        
        
    

                
           
           
