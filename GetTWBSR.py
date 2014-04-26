# -*- coding: utf-8 -*-

import re
import urllib2,urllib
import sys
import csv
from datetime import datetime
import os
import threading
import Queue
from time import strftime
from time import sleep
from time import time

from types import *

# TSE : Taiwan Stock Exchange , 台灣證交所 （上市）
# OTC : Over-the-Counter , 櫃檯中心 （上櫃）
# BSR : Buy Sell Report , 分公司買賣進出表

global ERR_RESET_PEER
global RESETPEER_CNT, TIMEOUT_CNT
global strikeOutAry
global giveupAry
RESETPEER_CNT = 0
TIMEOUT_CNT = 0
ERR_RESET_PEER = 104
strikeOutAry = []
giveupAry = []

class ThreadingDownloadBot(threading.Thread):
    def __init__(self,pid,queue):
        threading.Thread.__init__(self)
        self.queue = queue
        self.pid = pid  
    def run(self):                
	while(True):
	    if self.queue.qsize()==0 :
		print "[PID:%d]all job is done"%(self.pid)
		break
	    Code = self.queue.get()
	    retry = 0
	    if len(Code) >= 5:
		retry = int(Code[4])
		Code = Code[0:4]
	    while retry < 3 :
		print '[%d]Process:[%s] Left:%d retry:%d'%(self.pid,Code,self.queue.qsize(),retry)
		sleep( 1 ) #[]== don't hurry up
		ret = self.RunImp(Code)		
		if ret == None: #lol, what the hell?
		    retry +=1	
		    if retry == 3 :
			print "Got a three strike problem, sleep 5 secs then put back"
			#sleep( 5 ) #[]== you should sleep here.
			#self.queue.put(Code)
			iFound = False
			for e in strikeOutAry:
			    if e == Code :
				print "found strikeOutAlready, give up currently"				
				giveupAry.append(Code)
				iFound = True
			        break
			if iFound == False :
			    self.queue.put(Code)
			    strikeOutAry.append(Code)		
			break
		    retryCode = Code+str(retry)
		    print '********fail******* %d' %(self.pid)
		    sleep( 1 ) #[]== sleep 1 sec.
		elif ret == "104":
                    global RESETPEER_CNT
                    RESETPEER_CNT += 1
		    print "Got a reset error, sleep 5 secs then put back"
		    sleep( 5 ) #[]== you should sleep here.
		    self.queue.put(Code)
		    break
		elif ret == "110":
		    print "Got a time out error, sleep 5 secs then put back"
		    sleep( 5 ) #[]== you should sleep here.
                    global TIMEOUT_CNT
                    TIMEOUT_CNT += 1
		    self.queue.put(Code)
		    break

		else:
		    print '\t(%d)Write %s Finish...'%(self.pid,Code)	
		    break		  
	    #print "task_done! step 1 %d" % (self.queue.qsize())
	    self.queue.task_done()
	    #print "task_done! step 2 %d" % (self.queue.qsize())	   
        
class DownloadTSEBot(ThreadingDownloadBot):
    def __init__(self,pid,queue):
        super(DownloadTSEBot, self).__init__(pid,queue)
        self.name = "TSE BSR Download Bot."
    def RunImp(self,Code):

        # step 1. GetMaxPage and POST data
	# []== I got to change this approach
        def GetDateAndspPage(Code):
            try:
                base_url = 'http://bsr.twse.com.tw/bshtm/bsMenu.aspx'
                req = urllib2.Request(base_url)
                response = urllib2.urlopen(req)
                html = response.read()
                __VIEWSTATE = re.findall(u'id="__VIEWSTATE" value="(.*)" />',html)[0]
                __EVENTVALIDATION = re.findall(u'id="__EVENTVALIDATION" value="(.*)" />',html)[0]
                HiddenField_spDate = re.findall(u'id="sp_Date" name="sp_Date" style="display: none;">(.*)</span>',html)[0]
                
                PostDataDict = {'__EVENTTARGET':''
                                , '__EVENTARGUMENT':''
                                ,'HiddenField_page':'PAGE_BS'
                                ,'txtTASKNO':Code
                                ,'hidTASKNO':Code
                                ,'__VIEWSTATE': __VIEWSTATE
                                ,'__EVENTVALIDATION':__EVENTVALIDATION
                                ,'HiddenField_spDate':HiddenField_spDate
                                ,'btnOK':'%E6%9F%A5%E8%A9%A2'}
           
                postData = urllib.urlencode( PostDataDict)
                req = urllib2.Request( base_url , postData)
                response = urllib2.urlopen( req)
                html = response.read()
                sp_ListCount = re.findall(u'<span id="sp_ListCount">(.*)</span>',html)[0]
                return (HiddenField_spDate,sp_ListCount)
            except Exception,e:
		print e
                #print dir(e)
		if e.errno == 104 : #reset by peer
		   return ("ERR","104")
	        elif e.errno == 110 : #connection timeout
		   return ("ERR","110")
	        else :
                   return (None,None)
        
        # step 2. GetRawData
        def GetBSRawData(Code,MaxPageNum):
            try:
                url = 'http://bsr.twse.com.tw/bshtm/bsContent.aspx?StartNumber=%s&FocusIndex=All_%s'%(Code,MaxPageNum)
                req = urllib2.Request(url)
                response = urllib2.urlopen(req)
                html = response.read()
                return html
            except Exception , e:
                return None
        
        # step 3. RawToCSV
        def BSRawToCSV(BSRaw):
            
            #取得資料表title 
            '''
            <tr class='column_title_1'>     <td>序</td>     <td>證券商</td>     <td>成交單價</td>     <td>買進股數</td>     <td>賣出股數</td>   </tr>
            '''
            title_tr_pattern = u"<tr class='column_title_1'>(.*?)<\/tr>"
            title_tr = re.compile(title_tr_pattern)
            result_tr = title_tr.findall(BSRaw)
            title_td_pattern = u'<td *>\B(.*?)</td>'
            title_td = re.compile(title_td_pattern)
            result_td = title_td.findall(result_tr[0])            
            title = ','.join(title.decode('utf-8').encode('cp950') for title in result_td)
            #取得各分公司買賣內容
            td = '''
                    <td class='column_value_center'>               1</td>        <td class='column_value_left'>                 1233  彰銀台中</td>        <td class='column_value_right'>               8.65</td>        <td class='column_value_right'>               0</td>        <td class='column_value_right'>               20,000</td>     
            '''
            content_tr_pattern = u"<tr class='column_value_price_[23]'>(.*?)<\/tr>"
            content_tr = re.compile(content_tr_pattern)
            result_tr_content = content_tr.findall(BSRaw)
            content_td_pattern = u"<td \S*>(.*?)</td>"
            content_td = re.compile(content_td_pattern)
            content_list = []
            for tr in result_tr_content:
                result_td = content_td.findall(tr)
                row =  ','.join(td.replace(',','').strip() for td in result_td if td.strip()[0] not in ['<','&'])
                if len(row) == 0:
                    continue
                content_list.append(row.decode('utf-8').encode('cp950'))
            sortedlist = sorted(content_list,key = lambda s: int(s.split(',')[0]))
            #將Title加入資料首列
            sortedlist.insert(0,title)
            return sortedlist
        
        def CSVToFile(CSVData,filename):	    
	    retryCnt = 0
            csvfile = None
	    while csvfile == None :
	       csvfile = open('BSR/'+filename, 'wb')
	       retryCnt+=1
	       if retryCnt == 3:
		  print "I can't open file.%s" %(csvfile)
	          break
	    if csvfile != None :
		content = '\n'.join(row for row in CSVData)		    
		csvfile.write(content)

        self.RawBSR = "TSE"
        self.date,MaxPageNum = GetDateAndspPage(Code)
	if self.date == "ERR" :
	    return MaxPageNum #what a fool name, I should fix the whole architecture
        print Code , self.date , MaxPageNum
        if None == MaxPageNum or "" == MaxPageNum:
            return None
        BSRawData = GetBSRawData(Code, MaxPageNum)
        if None == BSRawData:
            return None
        filename = "%s_%s.csv"%(Code,self.date) 
        CSVData = BSRawToCSV(BSRawData)
        CSVToFile(CSVData, filename)
        return True
      
class DownloadOTCBot(ThreadingDownloadBot):
    def __init__(self,pid,queue):
        super(DownloadOTCBot, self).__init__(pid,queue)
        self.name = "OTC BSR Download Bot."
    
    def RunImp(self,Code):
        
        def DownloadOTC(Code,filename,otcdate):
            try:
                base_url = 'http://www.gretai.org.tw/ch/stock/aftertrading/broker_trading/download_ALLCSV.php'
                PostDataDict = {'curstk':Code
                                , 'fromw':'0'
                                ,'numbern':'100'
                                ,'stk_date':otcdate
                                }
            
                postData = urllib.urlencode( PostDataDict)
                req = urllib2.request( base_url , postdata)
                response = urllib2.urlopen( req)
                html = response.read()
            except exception , e:
                return none
            with open('bsr/'+filename, 'wb') as csvfile:
                content = '\n'.join(row for row in html.split(',,')[1:])
                csvfile.write(content)
            return true
        
        def getotcdate(code):
            baseurl = "http://www.gretai.org.tw/ch/stock/aftertrading/broker_trading/brokerbs.php"
            postdatadict = {
                'stk_code' : code
            }
            postdata = urllib.urlencode( postdatadict)
            req = urllib2.request( baseurl , postdata)
            response = urllib2.urlopen(req)
            html = response.read()    
            date_list = re.findall(u'<input type="hidden" name="stk_date" value=(.*)>',html)
            for date in date_list:
                return date
            return none
        	
        self.rawbsr = "otc"
        otcdate = getotcdate(code)	
        if otcDate == None:
            return None
        filename = "%s_%d%s.csv"%(Code,int(otcDate[0:3])+1911,otcDate[3:]) 
        ret = DownloadOTC(Code,filename,otcDate)
        if None == ret:
            return None
        return True

def getCodeDict():
    CodeDict = {'TSE' : [] , 'OTC': [] } 
    with open('data/smast.dat','r') as f:
        for row in f:
            try:
                code = row[:6].strip()
                row = row.decode('utf-8').encode('cp950')
                if len(code)== 4 : #忽略權證,公司債
                    print row[:13].decode('cp950').encode('utf-8')
                    if row[12] == '0': #TSE_上市
                        CodeDict['TSE'].append(row[:4])
                    if row[12] == '1': #OTC_上櫃
                        CodeDict['OTC'].append(row[:4]) 
            except IndexError:
                print 'You have an empty row'    
        sleep(5)
    return CodeDict
        
if __name__ == '__main__':
    if not os.path.exists('BSR'):
        os.makedirs('BSR')    
    print 'Start...'
    CodeDict = getCodeDict()
    print 'TSE:%d OTC:%d'%(len(CodeDict['TSE']),len(CodeDict['OTC']))
    tStart = time()

    #OTCqueue = Queue.Queue() 
    #for i in range(20):
    #    t = DownloadOTCBot(i,OTCqueue)
    #    t.setDaemon(True)
    #    t.start()
    TSEqueue = Queue.Queue()

    for Code in CodeDict['TSE']:
        TSEqueue.put(Code)

    for i in range(50):
        t = DownloadTSEBot(i,TSEqueue)
        t.setDaemon(True)
        t.start()
	sleep(0.3)

    #for Code in CodeDict['OTC']:
    #    OTCqueue.put(Code)         

    #for Code in CodeDict['TSE']:
     #   TSEqueue.put(Code)

    #OTCqueue.join()
    TSEqueue.join()
    
    tEndTSE = time()

    print 'End...Total(%f)'%(tEndTSE-tStart)
    print "Reset peer count (%d)" %(RESETPEER_CNT)
    print "Time out count (%d)" %(TIMEOUT_CNT)    
    print "strikeOutAry is:"
    for aEntry in strikeOutAry:
	print aEntry
    print "================"
    print "giveupAry is:"
    for aEntry in giveupAry:
	print aEntry

    print "[]== Have A Nice Day"
