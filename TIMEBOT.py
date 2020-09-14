import ccxt
import json
import pandas as pd
import csv
from csv import DictWriter
import time as t
import requests
from datetime import *; from dateutil.relativedelta import *
from datetime import datetime
import calendar
import numpy as np
import matplotlib.pyplot as plt
import gspread
 
def getBalance():
  dfBalance = pd.DataFrame(exchange.fetch_balance(),columns=['USD','BTC','BTC-1225','free','used','total'])
  dfBalanceList = dfBalance.values.tolist()
  print(dfBalance)
 
def getJsonPrice():
  pairTicker = json.dumps(exchange.fetch_ticker(pair))
  JsonPrice = json.loads(pairTicker)
  print(pair,'=',JsonPrice['last'],'$')
  print(' ')
  return JsonPrice['last']
 
def getPendingOrder():
  print("=============================================")
  print(" ")
  print("PENDING ORDER") 
  dfPendingOrder = pd.DataFrame(exchange.fetch_open_orders(pair),columns=['id','datetime','symbol','type','side','price','amount','filled','remaining','cost'])
  print(dfPendingOrder)
  print(" ")
 
def getMatchBuyOrder():
  print("=============================================")
  print(" ")
  print("MATCH BUY ORDER")
  dfMatchBuy = pd.DataFrame(filter(lambda x:x['side'] == "buy",exchange.fetch_my_trades(pair,timeStamp,1000)),columns=['id','datetime','symbol','side','price','amount','cost'])
  print(dfMatchBuy)
  print(" ")
 
def getMatchSellOrder():
  print("=============================================")
  print(" ")
  print("MATCH SELL ORDER")
  dfMatchSell = pd.DataFrame(filter(lambda x:x['side'] == "sell",exchange.fetch_my_trades(pair,timeStamp,1000)),columns=['id','datetime','symbol','side','price','amount','cost'])
  print(dfMatchSell)
  print(" ")
 
def getTime():
  localTime = t.localtime()
  Time = t.strftime("%m/%d/%Y, %H:%M:%S",localTime)
  print(Time)

def getmarkUpExposure():
  dfExcelmarkUpExposure = pd.read_csv(markupFile)
  body = dfExcelmarkUpExposure.values.tolist()
 
  markUpExposure = []
 
  for i in range (len(body)):
    if jsonPrice > body[i][1] and jsonPrice <= body[i][2] :
      markUpExposure.append(round(body[i][4],3))
      markUpExposure.append(body[i][1])
      markUpExposure.append(body[i][2])
      
  return markUpExposure

def getsumExposure() :

  sumExposure = []
  totalSellExposure = 0
  totalBuyExposure  = 0
  countSellExposure = 0
  countBuyExposure  = 0
  netSize1 = 0
  notional = 0
  real     = 0
  unreal   = 0

  netSize = json.dumps(exchange.fetch_trading_fees())  # เช็คว่าเปิดไปแล้วกี่ btc
  netSize = json.loads(netSize)
  netSize = json.dumps(netSize["info"]["result"]['positions'])
  netSize = json.loads(netSize)

  for i in range (len(netSize)) :
    if netSize[i]['future'] == pair :
      netSize1 = netSize[i]['netSize']
      notional = netSize[i]['cost']
      real     = netSize[i]['realizedPnl']
      unreal   = netSize[i]['unrealizedPnl']
 
  with open(tradingLog, newline='') as f:
      reader = csv.reader(f)
      data = list(reader)

  for i in range (len(data)):
    if data[i][4] == "sell":
      totalSellExposure += float(data[i][7])  # Collumn "cost"
      countSellExposure += 1
    elif data[i][4] == "buy":
      totalBuyExposure += float(data[i][7])   # Collumn "cost"
      countBuyExposure += 1
  sumExposureValue = totalBuyExposure-totalSellExposure
  totalSellLimitExposure = 0
  totalBuyLimitExposure  = 0
  countBuyLimitExposure  = 0
  countSellLimitExposure = 0

  dfMyOrderList = getUpdatePending()
 
  for i in range (len(dfMyOrderList)) :
    if dfMyOrderList[i][4] == "sell" :
      totalSellLimitExposure += dfMyOrderList[i][6]
      countSellLimitExposure += 1
    elif dfMyOrderList[i][4] == "buy" :
      totalBuyLimitExposure += dfMyOrderList[i][6]
      countBuyLimitExposure += 1
 
  sumExposureValue = totalBuyExposure-totalSellExposure
  sumExposurePending = totalBuyLimitExposure-totalSellLimitExposure
  allExposure = sumExposureValue+sumExposurePending
 
  # เก็บ DATA เอาไว้ใน LIST เพื่อดึงไปใช้งานใน FUNCTION อื่นๆ
 
  sumExposure.append(round(sumExposureValue,3))        #sumExposure[0]
  sumExposure.append(round(totalBuyExposure,3))        #sumExposure[1]
  sumExposure.append(round(totalSellExposure,3))       #sumExposure[2]
  sumExposure.append(countSellExposure)                #sumExposure[3]
  sumExposure.append(countBuyExposure)                 #sumExposure[4]
  sumExposure.append(round(sumExposurePending,3))      #sumExposure[5]
  sumExposure.append(round(totalBuyLimitExposure,3))   #sumExposure[6]
  sumExposure.append(countBuyLimitExposure)            #sumExposure[7]
  sumExposure.append(round(totalSellLimitExposure,3))  #sumExposure[8]
  sumExposure.append(countSellLimitExposure)           #sumExposure[9]
  sumExposure.append(round(allExposure,3))             #sumExposure[10]
  sumExposure.append(round(netSize1,3))                #sumExposure[11]
  sumExposure.append(round(notional,3))                #sumExposure[12]
  sumExposure.append(round(real,3))                    #sumExposure[13]
  sumExposure.append(round(unreal,3))                  #sumExposure[14]
 
  return sumExposure
 
def getUpdateRecord() :
 
  checkIDincsv = []
 
  with open(tradingLog, newline='') as f:
    reader = csv.reader(f)
    data = list(reader)
 
  for i in range (len(data)):
    checkIDincsv.append(data[i][0])
 
  dfMyTrade = pd.DataFrame(exchange.fetch_my_trades(pair,timeStamp,1000),columns=['id','timestamp','datetime','symbol','side','price','amount','cost'])
  dfMyTradeList = dfMyTrade.values.tolist()

  dfMyTradeFee = pd.DataFrame(exchange.fetch_my_trades(pair,timeStamp,1000),columns=['id','fee'])
  dfMyTradeFeeList = dfMyTradeFee.values.tolist()
 
  for i in range (len(dfMyTradeList)):
    dfMyTradeList[i].append((dfMyTradeFeeList[i][1])['cost'])
    if dfMyTradeList[i][0] not in checkIDincsv :
      with open(tradingLog, "a+", newline='') as fp:
        wr = csv.writer(fp, dialect='excel')
        wr.writerow(dfMyTradeList[i])
        print(dfMyTradeList[i][0],dfMyTradeList[i][2],dfMyTradeList[i][3],dfMyTradeList[i][4],dfMyTradeList[i][5],dfMyTradeList[i][6],(dfMyTradeFeeList[i][1])['cost'])
# ===================================== Gsheet Record ===================================== #

def getUpdateDataFromGsheet():

  gc = gspread.service_account(filename='Real-AddData-credentials.json')
  sh = gc.open_by_key('1g0K-Q2kGGEcnpWCzc7hNffWJu7utnmipfJphAEL1lJE')
  worksheet1 = sh.sheet1

  res = worksheet1.get_all_records()
  dfRes = pd.DataFrame(res)
  return dfRes

def getUpdateGGRecord():

  gc = gspread.service_account(filename='Real-AddData-credentials.json')
  sh = gc.open_by_key('1g0K-Q2kGGEcnpWCzc7hNffWJu7utnmipfJphAEL1lJE')
  worksheet1 = sh.sheet1

  recordInGsheet = getUpdateDataFromGsheet()['ID'].tolist()
  dfMyTrade = pd.DataFrame(exchange.fetch_my_trades(pair,timeStamp,1000),columns=['id','timestamp','datetime','symbol','side','price','amount','cost'])
  dfMyTradeList = dfMyTrade.values.tolist()

  dfMyTradeFee = pd.DataFrame(exchange.fetch_my_trades(pair,timeStamp,1000),columns=['id','fee'])
  dfMyTradeFeeList = dfMyTradeFee.values.tolist()

  for i in range (len(dfMyTradeList)):
      dfMyTradeList[i].append((dfMyTradeFeeList[i][1])['cost'])
      if int(dfMyTradeList[i][0]) not in recordInGsheet :
          worksheet1.insert_row(dfMyTradeList[i],(len(getUpdateDataFromGsheet())+2))

#==============================================================================
# UPDATE PENDING ORDER เพื่อป้องกันการเปิด POSITION เกินกำหนด

def getUpdatePending():
 
  dfMyOrder = pd.DataFrame(exchange.fetch_open_orders(pair),columns=['id','timestamp','datetime','symbol','side','price','amount','cost'])
  dfMyOrderList = dfMyOrder.values.tolist()
 
  return dfMyOrderList

def getCancelPending():
  dfMyOrderList = getUpdatePending()
 
  for i in range (len(dfMyOrderList)):
    
    if (dfMyOrderList[i][5]-jsonPrice) > CANCEL_PENDING_RANGE and dfMyOrderList[i][4] == "sell" :
      exchange.cancel_order(dfMyOrderList[i][0])
      print("CANCELED :",dfMyOrderList[i])
      print(" ")
    elif (jsonPrice-dfMyOrderList[i][5]) > CANCEL_PENDING_RANGE and dfMyOrderList[i][4] == "buy" :
      exchange.cancel_order(dfMyOrderList[i][0])
      print("CANCELED :",dfMyOrderList[i])
      print(" ")
  return  

def lastTradeTimeLocal():
  dfMyTrade = pd.DataFrame(exchange.fetch_my_trades(pair,timeStamp,1000),columns=['id','timestamp','datetime','symbol','side','price','amount','cost'])
  dfMyTradeList = dfMyTrade.values.tolist()

  lastTradeTimeLocal = 0
  lastOrderTimeStamp = 0

  if len(dfMyTradeList) > 0 :
    print('Last trade time cal.')
    lastOrderTimeStamp = int(dfMyTradeList[-1][1]/1000)
    lastTradeTimeLocal = datetime.fromtimestamp(lastOrderTimeStamp)
    lastTradeTimeLocal = datetime.fromtimestamp(lastOrderTimeStamp)+relativedelta(hours=+0)

    if len(dfMyTradeList) > 3 :
      for i in range (-1,-4,-1) : #print last 3 orders
        print(dfMyTradeList[i])
    elif len(dfMyTradeList) < 3 :
      for i in range (len(dfMyTradeList)) : #print last 3 orders
        print(dfMyTradeList[i])
 
  return lastTradeTimeLocal

def getPrintDetails():
  sumExposure = getsumExposure()
  markupExposure = getmarkUpExposure()

  print(" ")
  print("TOTAL BUY  EXPOSURE =","%.4f" % sumExposure[1],"BTC")
  print("TOTAL SELL EXPOSURE =","%.4f" % sumExposure[2],"BTC")
  print("TOTAL BUY LIMIT  EXPOSURE =","%.4f" % sumExposure[6],"BTC",',','QTY =',sumExposure[7])
  print("TOTAL SELL LIMIT EXPOSURE =","%.4f" % sumExposure[8],"BTC",',','QTY =',sumExposure[9])
  print("SUM EXPOSURE =","%.4f" % sumExposure[0],"/","%.4f" % sumExposure[10],"BTC")
  print("MAX EXPOSURE =","%.4f" % markupExposure[0],"BTC","(MARK UP)")
  print(" ")
  print("TOTAL BUY  TRANSACTION =",sumExposure[4])
  print("TOTAL SELL TRANSACTION =",sumExposure[3])
  print("SUM TRANSACTION =",sumExposure[3]+sumExposure[4])
  print(" ")
  print("NOTIONAL FTX         =","%.2f"%sumExposure[12],'$')
  print("REALIZE PNL FTX      =","%.2f"%sumExposure[13],'$')
  print("UNREALIZE PNL FTX    =","%.2f"%sumExposure[14],'$')
  print(" ")

def getExecute():
  
  sumExposure = getsumExposure()
  getCancelPending()
  markupExposure = getmarkUpExposure()

  if sumExposure[0] != 0 :
    print("DIFF FROM ACTUAL (%) =","%.2f"%((((sumExposure[12]-sumExposure[13]-sumExposure[14])-sumExposure[0])*100)/sumExposure[0]),"%")
    print(" ")

  if markupExposure[0] > float(sumExposure[0]) and float(markupExposure[0]-float(sumExposure[0])) > minRebalance :
    print("BOT TRADE IN BUY SIDE")
    print("WITH MAXIMUM BUY EXPOSURE =",(float(markupExposure[0]-float(sumExposure[0])),'$'))
    usd = float(markupExposure[0]-(sumExposure[0]))
    price = markupExposure[2]
    size_order = round(usd/price,4)
    print(pair,"limit","buy",price,usd,'$','(',size_order,'BTC',')')
    exchange.create_order(pair,"limit","buy",size_order, price)
    getUpdatePending()

    token = lineToken
    url = 'https://notify-api.line.me/api/notify'
    headers = {'content-type':'application/x-www-form-urlencoded','Authorization':'Bearer '+token}
    msg = "BOT SEND BUY LIMIT"+str(" ")+str(pair)+str(" ")+str(size_order)+str(" ")+str(price)
    r = requests.post(url, headers=headers, data = {'message':msg})

  elif markupExposure[0] < float(sumExposure[0]) and (float(sumExposure[0]))-float(markupExposure[0]) > minRebalance :
    print("BOT TRADE IN SELL SIDE")
    print("WITH MAXIMUM SELL EXPOSURE =",(float(sumExposure[0]))-float(markupExposure[0]),'$')
    usd = float((sumExposure[0])-markupExposure[0])
    price = markupExposure[1]
    size_order = round(usd/price,4)
    print(pair,"limit","sell",price,usd,'$','(',size_order,'BTC',')')
    exchange.create_order(pair, "limit" , "sell", size_order, price)
    getUpdatePending()

    token = lineToken
    url = 'https://notify-api.line.me/api/notify'
    headers = {'content-type':'application/x-www-form-urlencoded','Authorization':'Bearer '+token}
    msg = "BOT SEND SELL LIMIT"+str(" ")+str(pair)+str(" ")+str(size_order)+str(" ")+str(price)
    r = requests.post(url, headers=headers, data = {'message':msg})

# ============================== ACCOUNT PARAMETER ============================== #

apiKey = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"   
secret = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"   

exchange = ccxt.ftx({'apiKey':apiKey, 'secret':secret, 'enableRateLimit':True})
exchange.headers = {'FTX-SUBACCOUNT':'XXXXXXXX',}        

markupFile = "ETimeDelay1_markupFile.csv"
tradingLog = "ETimeDelay1_tradingLog.csv"
lineToken = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"

token = lineToken
url = 'https://notify-api.line.me/api/notify'
headers = {'content-type':'application/x-www-form-urlencoded','Authorization':'Bearer '+token}
msg = "BOT START !"
r = requests.post(url, headers=headers, data = {'message':msg})
# ============================== PRODUCT PARAMETER ============================== #

pair = "BTC-PERP"
timeStamp = 1595638800000     #25/07/20
getUpdateGGRecord()
getUpdateRecord()
tradingTimePeriod = "minute"  #tradingTime hour or minute
minRebalance = 2              # $ หมายถึง Exposure input - Exposure markup เช่นหากเรา imput = 5 แต่ Markup = 8 จะทำการ Rebalance = 3
CANCEL_PENDING_RANGE = 20     # ระยะห่างของ Pending price กับ jsonPrice
theListNumber = [2,9,10,18,20,28,29,37,39,47,51,56,65,65,69,74,76,79,84,87,93,93,95,103,110,114,121,122,125,130,132,138,144,146,150,159,166,173,178,185,187,191,198,198,207,210,216,225,234,243,248,257,262,269,273,282,288,294,303,309,316,322,324,331,338,340,344,344,351,357,363,366,366,369,374,377,382,386,393,398,398,407,411,416,423,424,427,435,437,438,445,453,458,460,465,466,472,478,482,484,491,495,497,504,508,514,520,523,532,533,542,545,547,547,547,550,550,555,564,573,575,576,584,585,592,596,597,600,605,614,620,626,628,637,637,641,644,649,656,658,667,667,667,670,673,677,679,688,693,695,701,701,706,715,720,726,729,729,736,739,747,748,751,753,756,758,766,772,774,781,790,794,797,801,810,810,817,823,826,828,831,834,842,844,853,861,869,869,876,881,884,885,894,899,901,906,907,907,908,917,917,918,919,924,931,934,942,945,949,950,958,965,974,977,977,984,984,986,987,992,996,996,1004,1013,1014,1018,1027,1036,1039,1043,1051,1059,1063,1064,1070,1077,1082,1082,1091,1093,1097,1101,1108,1114,1115,1119,1125,1125,1131,1137,1145,1145,1153,1155,1157,1163,1167,1175,1175,1175,1176,1182,1190,1194,1201,1208,1212,1213,1214,1222,1227,1230,1237,1241,1243,1246,1250,1255,1259,1263,1265,1269,1272,1279,1280,1280,1287,1292,1295,1304,1304,1311,1318,1325,1329,1333,1342,1351,1353,1353,1359,1368,1373,1378,1379,1386,1386,1388,1395,1401,1402,1410,1413,1421,1427,1427,1433,1435, 1441, 1442, 1445, 1448, 1449, 1452, 1460]


while True :
  try :
    getTime()
    jsonPrice = getJsonPrice()
    signal = "Waiting time ..."
    nowDateTime = datetime.now()
    currentHr = nowDateTime.hour
    currentMin = nowDateTime.minute
    totalCurrentMin = (currentHr*60)+currentMin

    lastTradeTimeLocal2 = lastTradeTimeLocal()
    getUpdateGGRecord()
    getUpdateRecord()
    getPrintDetails()

    lastTradeTimeLocalHr = ' '
    TotalLastTradeTimeLocalMin = ' '
    nextTime = []

    if lastTradeTimeLocal2 != 0 :
      lastTradeTimeLocalHr = lastTradeTimeLocal2.hour
      lastTradeTimeLocalMin = lastTradeTimeLocal2.minute
      TotalLastTradeTimeLocalMin = (lastTradeTimeLocalHr*60)+lastTradeTimeLocalMin

    if tradingTimePeriod == "hour" :
      print('Last.  trade number =','(',lastTradeTimeLocalHr,')',lastTradeTimeLocal2)
      print('Current time number =','(',currentHr,')',nowDateTime)
      for i in range (len(theListNumber)) :
        if int(currentHr) < theListNumber[i]:
          nextTime.append(theListNumber[i])
      print("The next number is  =",'(',nextTime[0],')')
      print(" ")
      for i in range (len(theListNumber)) :
        if currentHr == int(theListNumber[i]) and currentHr != lastTradeTimeLocalHr :
          signal = "Time to execute !"
          dfMyOrderList = getUpdatePending()
          getUpdateGGRecord()
          getUpdateRecord()
          getExecute()

    elif tradingTimePeriod == "minute" :
      print('Last.  trade number =','(',TotalLastTradeTimeLocalMin,')',lastTradeTimeLocal2)
      print('Current time number =','(',totalCurrentMin,')',nowDateTime)
      for i in range (len(theListNumber)) :
        if int(totalCurrentMin) < theListNumber[i]:
          nextTime.append(theListNumber[i])
      print("The next number is  =",'(',nextTime[0],')')
      print(" ")
      for i in range (len(theListNumber)) :
        if totalCurrentMin == int(theListNumber[i]) and totalCurrentMin != TotalLastTradeTimeLocalMin :
          signal = "Time to execute !"
          dfMyOrderList = getUpdatePending()
          getUpdateGGRecord()
          getUpdateRecord()
          getExecute()

    print(signal)
    print("========================================================")

    if tradingTimePeriod == "hour" :
      t.sleep(3600)
    elif tradingTimePeriod == "minute" :
      t.sleep(50)

  except :
    print("MAYBE THE CONNECTION ERROR !")
    print("================================")
    t.sleep(30)
