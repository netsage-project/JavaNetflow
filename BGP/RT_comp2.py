#compare the parsed output file from snmp and terminal routing tables
#run in bash, "python RT_comp2.py file1 file2" and print results
# file1 is output file from terminal, file2 is from snmp

import sys
import os
import time
start=time.time()
fileName = os.getcwd() +'/'+sys.argv[1]
fileNameSnmp= os.getcwd() +'/'+sys.argv[2]
with open (fileNameSnmp,'r') as snmpFile:
    with open (fileName,'r') as file:
        prefix1=''
        prefixr1=''
        dr=0
        m=0
        s=0
        d=0
        n=0             #num of prefix not found
        r=0
        b=0
        noNextHop=0     #find prefix, not find nextHop
        notmatch=0
        
        for lines in snmpFile:
            s=s+1
            ls = lines.split(',')
            prefix = ls[0]+'/'+ls[1]    #prefix+/+length
            if ( prefix != prefix1):
                d=d+1   #distinct in snmp
                nextHop = ls[2]
                
                nextHopFound= False
                prefixFound=False
                for line in file:
                    l = line.split(',')
                    if (l[1]+'/'+l[2]==prefix):     #prefix+/+length
                        prefixFound=True
                        if (l[3]==nextHop):
                            nextHopFound = True
                            if 'B' in l[7]:
                                m=m+1
                                break   #go find next prefix in snmp
                            else:       #find route but not best
                                print("Not matching: "+line+'\n'+lines)
                                notmatch=notmatch+1
                                break
                if ( not nextHopFound):
                    noNextHop=noNextHop+1
                    #print("nextHop not found: "+ lines)
                    file.seek(0) 
                if ( not prefixFound):
                    n=n+1
                    print ('prefix not found: '+lines)                               
                    file.seek(0)                                          
            prefix1 = prefix
            
with open (fileName,'r') as file: #count in bgp tables
    for line in file:
        r=r+1
        l = line.split(',')
        if ('B' in l[7]):
            b=b+1
        prefixr=l[1]+'/'+l[2]
        if ( prefixr != prefixr1):
            dr=dr+1
        prefixr1=prefixr

            
end =time.time()
print ('All records in terminal: '+str(r) +'\nAll records in snmp: '+str(s)
        +'\nDistinct prefix in terminal: '+str(dr)+'\nDistinct prefix in snmp: '+str(d)
       +'\nAll best in terminal: '+str(b)
        +'\nMatch: '+str(m)+'\n not match :'+str(notmatch)
        +'\n prefix not found: '+str(n)
        +'\n next hop not found: ' +str(noNextHop) 
       +'\nRuntime: '+str(end-start)+'s')
            
