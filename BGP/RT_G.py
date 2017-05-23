#parsing routing table from gloriad
#run in bash, and create outputfile in same folder

import sys
import os

fileName = os.getcwd() +'/'+sys.argv[1]
with open (os.getcwd() +'/'+'output_'+sys.argv[1],'w') as outputFile:
    with open (fileName,'r') as file:
        for line in file:
            if (line.strip()[0:7]=='Network'):
                break
        i=0
        for line in file:
            i=i+1
            l=line.split()
            
            best= False
            if '>' in l[0]:
                best = True

            if '/' in l[1]:
                network = l[1]
            else: l.insert(1,'')    #same network as previous
                
            r=network.split('/')    #network&length
            r.insert(0,str(i))      #ID
            r.append(l[2])          #nexthop
            
            if (line[40:50].strip()==''):
                l.insert(3,'')
            r.extend(l[3:6])        #MED locP Weight

            if (best): r.append('B')
            else: r.append('')      #status

            path = ' '.join(l[6:len(l)-1])
            r.append(path)          #path
    
            routing = ','.join(r)
            if (routing[0:2]!='1,'):
                outputFile.write('\n')
            outputFile.write(routing)
        
        
    

                
           
           
