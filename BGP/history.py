#use connector module to connect mysql, download if needed
#read through output file from RT_snmp.py
#update table "prefix" and "history" in mysql
#prefix stores all unique networks and num of best paths appeared
#history stores the datetime of best pathes

# "python history.py parsedFile"
#must run on files in chronological order



import mysql.connector
import sys
import os
import time
start=time.time()

fileN=sys.argv[1]
fileName=os.getcwd() +'/'+fileN

#connect to mysql database
db = mysql.connector.connect(user='root',password="Mysql@123",host='localhost',database="routingTable",buffered=True)
cursor = db.cursor()

#import prefix and length fields. Table: prefix2
sql= '''load data local infile '%s' into table prefix fields terminated by ',' (prefix,length); ''' %(fileN)
cursor.execute(sql)

#store current status in map (H_ID to current status)
sql='select H_ID, current from history ;'
cursor.execute(sql) 
results = cursor.fetchall()
d={'':''}
for row in results:
    d[row[0]]=row[1]
    
#set all current to false
sql='update history set current = 0; '
cursor.execute(sql) 

pre1=''
a=0
with open (fileName,'r') as file:       #new file
    for line in file:
        l=line.strip('\n').split(',')
        prefix=l[0]
        length=l[1]
        pre=prefix +'/'+length

        if pre != pre1:                 #only unique prefix (first is best)
            
            nextHop=l[2]
            path=l[3]
            date=l[4]
            if a%100 ==0 : print(a)
            
            sql1 = 'select P_ID, numOfNH from prefix where prefix= "%s" and length=%s ;' % (prefix, length)
            cursor.execute(sql1)        #search for foreign ID
            result=cursor.fetchone()    #returns a tuple
            P_ID=str(result[0])
            numOfNH=str(result[1])
            #print (P_ID)
            
            sql2= 'select history,current from history where P_ID =%s ;' % (P_ID)
            cursor.execute(sql2)        #check if such prefix exist
           
            if cursor.rowcount==0:      #CASE1: new prefix, add new route
                sql25='insert into history values (%s,"%s","%s","%s~%s",1,null);' % (P_ID, nextHop, path, date, date)
                cursor.execute(sql25)
                                
            
            else:                       #prefix already exist
                sql3='select history, H_ID from history where P_ID =%s and nextHop="%s"; ' % (P_ID, nextHop)
                cursor.execute(sql3)

                if cursor.rowcount==0:  #CASE2: new best route, write in
                    numOfNH=str(int(numOfNH)+1)
                    sql5='update prefix set numOfNH = %s where prefix= "%s" and length=%s ;' % (numOfNH, prefix, length)
                    cursor.execute(sql5)#update numOfNH in prefix
                    
                    sql6='insert into history values (%s,"%s","%s","%s~%s",1,null) ;' % (P_ID, nextHop, path, date,date)
                    cursor.execute(sql6)#write new route in history

                else:                   #CASE3: existing nextHop, update history              
                    result=cursor.fetchone()
                    history = result[0]
                    isCurrent = d[result[1]] #H_ID is key to current in map d
            
                    if isCurrent :      #Case3.1 continue to be best route
                        h=history.split(', ')
                        lastStart=h[-1].split('~')[0]
                        preHis=', '.join(h[0:-1])
                        history = preHis+ lastStart+'~'+date  # update after~
                    else:               #Case3.2 restart to be best route
                        history = history +', '+ date+'~'+date #add after ,
                    
                    sql4='update history set history ="%s", current =1 where P_ID =%s and nextHop="%s";' %(history, P_ID, nextHop)
                    cursor.execute(sql4)#update history string in table history
                    
 
                  
            a=a+1
##            if a==0: break
        
        pre1=pre                        #record current prefix to compare with next one



db.commit()

db.close()
end = time.time()
print ('Runtime: '+str(end-start)+'s')
