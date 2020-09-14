import pandas as pd
import json
import csv
from csv import DictWriter
import time
import ccxt
import gspread
from datetime import datetime

# กำหนด PARAMETER ที่จำเป็น
pair = "BTC-PERP"
timeFrame = "15m"                                             
apiKey = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"     # ELEPHBOT1
secret = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"     # ELEPHBOT1
exchange = ccxt.ftx({'apiKey' : apiKey ,'secret' : secret ,'enableRateLimit': True})
exchange.headers = {'FTX-SUBACCOUNT':'XXXXXXXXX',}      # ELEPHBOT1
reduceOnly = False                                      # ปิดโพซิชั่นเท่าจำนวนที่มีเท่านั้น (CREDIT : TY)
postOnly =  True                                        # วางโพซิชั่นเป็น MAKER เท่านั้น
ioc = False                                             # immidate or cancel เช่น ส่งคำสั่งไป Long 1000 market ถ้าไม่ได้ 1000 ก็ไม่เอา เช่นอาจจะเป็น 500 สองตัวก็ไม่เอา
series = [2,7,1,8,2,8,1,8,2,8]                          # ชุดอนุกรมที่ต้องการ
RecordFileName = 'tradingLog.csv'
MarkupFileName = 'markupFile.csv'
timestampRecord = 1599471023000
portName = "EULER ELEPHBOT1"

### GET FROM EXCHANGE -------------------------------------------------------------------------------------------------------------------

def getTime():  # เวลาปัจจุบัน
    named_tuple = time.localtime() # get struct_time
    Time = time.strftime("%m/%d/%Y, %H:%M:%S", named_tuple)
    return Time

def getPrice():
    PRICE = json.dumps(exchange.fetch_ticker(pair)['last']) # ราคา btc-perp ล่าสุด
    PRICE = (float(PRICE))
    return PRICE

def getUpdatePending():
    dfMyOrderList = pd.DataFrame(exchange.fetch_open_orders(pair),
                    columns=['id','datetime','symbol','type','side','price','amount','filled','remaining','cost'])
    return dfMyOrderList

def getPendingBuy():
    dfPendingBuy  = pd.DataFrame(filter(lambda x:x['side'] == "buy",exchange.fetch_open_orders(pair)),
                    columns=['id','datetime','symbol','type','side','price','amount','filled','remaining','cost'])
    return dfPendingBuy

def getPendingSell():
    dfPendingSell = pd.DataFrame(filter(lambda x:x['side'] == "sell",exchange.fetch_open_orders(pair)),
                    columns=['id','datetime','symbol','type','side','price','amount','filled','remaining','cost'])
    return dfPendingSell

def getTradeHistory():
    dfHistory     = pd.DataFrame(exchange.fetchMyTrades(pair,timestampRecord,1000),
                    columns=['id','datetime','side','price','amount','fee'])
    cost=[]
    for i in range(len(dfHistory)):
        cost.append((dfHistory['fee'][i]['cost']))  # ใน fee เอาแค่ cost
    dfHistory['fee'] = cost
    return dfHistory

def getLastSide():
    dfHistory = getTradeHistory()
    lastside = "NO HISTORY"
    if len(dfHistory) > 0 :
        lastside = dfHistory.iloc[-1][2]
    return lastside  

def getLastPrice():
    dfHistory = getTradeHistory()
    lastprice = 0
    if len(dfHistory) > 0 :
        lastprice = getTradeHistory().iloc[-1][3]
    return lastprice

def getDiffFromLastTrade():
    diff = getPrice()-getLastPrice()
    return diff

def getNowExposure():
    netSize = 0
    netSize = json.dumps(exchange.private_get_account())  # เช็คว่าเปิดไปแล้วกี่ btc
    netSize = json.loads(netSize)
    netSize = json.dumps(netSize["result"]['positions'])
    netSize = json.loads(netSize)
    for i in range (len(netSize)) :
        if netSize[i]['future'] == pair :
            netSize1 = netSize[i]['netSize']
            cost1    = netSize[i]['cost']
    return netSize1

def getCheckNewMatch():
    newmatch = "NO"
    lastrecord = pd.read_csv(RecordFileName)
    if len(lastrecord) > 0 :
        lastrecord = int(lastrecord.iloc[-1][0])
        if int(getTradeHistory().iloc[-1][0]) - lastrecord == 0:
            newmatch = "NO"
        elif int(getTradeHistory().iloc[-1][0]) - lastrecord > 0:
            newmatch = "YES"
    return newmatch

### GET FROM CSV ------------------------------------------------------------------------------------------------------------------------

def getUpdateRecord():
    dfData           = getRecordData()
    checkIDincsv     = dfData['ID'].values.tolist()
    dfMyTrade        = pd.DataFrame(exchange.fetchMyTrades(pair,timestampRecord,1000),
                       columns=['id','timestamp','datetime','symbol','side','price','amount','cost'])
    dfMyTradeList    = dfMyTrade.values.tolist()
    dfMyTradeFee     = pd.DataFrame(exchange.fetchMyTrades(pair,timestampRecord,1000),
                       columns=['id','fee'])
    dfMyTradeFeeList = dfMyTradeFee.values.tolist()

    for i in range (len(dfMyTradeList)):    
        dfMyTradeList[i].append((dfMyTradeFeeList[i][1])['cost'])
        if int(dfMyTradeList[i][0]) not in checkIDincsv :
            with open(RecordFileName, "a+", newline='') as fp:
                wr = csv.writer(fp, dialect='excel')
                wr.writerow(dfMyTradeList[i])

def getRecordData() :
    dfData = pd.read_csv(RecordFileName)
    return dfData

def getDetails() :
    price = getPrice()
    dfData = getRecordData()
    dfPendingBuy = getPendingBuy()
    dfPendingSell = getPendingSell()
    markup = pd.read_csv(MarkupFileName)
    for i in range (len(markup)):
        if price > markup.iloc[i,1] and price <= markup.iloc[i,2] :
            markUpExposure = markup.iloc[i,3]
    print(" ")
    print("TOTAL BUY  EXPOSURE =","%.2f" % dfData[dfData['SIDE']=='buy']['QTY($)'].sum(),"USD")
    print("TOTAL SELL EXPOSURE =","%.2f" % dfData[dfData['SIDE']=='sell']['QTY($)'].sum(),"USD")
    print("TOTAL BUY LIMIT  EXPOSURE =","%.2f" % dfPendingBuy['amount'].sum(),"USD",',','QTY =',len(dfPendingBuy))
    print("TOTAL SELL LIMIT EXPOSURE =","%.2f" % dfPendingSell['amount'].sum(),"USD",',','QTY =',len(dfPendingSell))
    print("SUM EXPOSURE =","%.2f" % ((dfData[dfData['SIDE']=='buy']['QTY($)'].sum())-(dfData[dfData['SIDE']=='sell']['QTY($)'].sum())),'(',"%.2f" % getNowExposure(),')')
    print("MAX EXPOSURE =","%.2f" % markUpExposure,"USD","(MARK UP)")
    print("TOTAL BUY  TRANSACTION =",dfData[dfData['SIDE']=='buy']['QTY($)'].count())
    print("TOTAL SELL TRANSACTION =",dfData[dfData['SIDE']=='sell']['QTY($)'].count())
    print("SUM TRANSACTION =",len(dfData))
    print(" ")

def updatepaired():

        dfpaired        = pd.read_csv("pairedOrder.csv")
        dfpositionBuy   = pd.read_csv("holdBuy.csv")
        dfpositionSell  = pd.read_csv("holdSell.csv")

        Loop = True

        while Loop :

            BUYQTY = dfpositionBuy['QTY($)'].sum()
            SELLQTY = dfpositionSell['QTY($)'].sum()
            if SELLQTY == 0 or BUYQTY == 0 :
                Loop = False
            elif SELLQTY != 0 and BUYQTY != 0 :
                if SELLQTY >= BUYQTY :
                    for i in range (len(dftradinglog)) :
                        if dfpositionBuy['QTY($)'].iloc[i] > dfpositionSell['QTY($)'].iloc[-i-1]  :  
                            a =  round(dfpositionBuy['QTY($)'].iloc[i] - dfpositionSell['QTY($)'].iloc[-i-1],4)
                            qty = round(dfpositionBuy.iat[i,6],4)
                            cost = round(dfpositionBuy.iat[i,7],4)   #
                            fee = round(dfpositionBuy.iat[i,8],4)    #
                            dfpositionBuy.iat[i,6] = round(dfpositionSell['QTY($)'].iloc[-i-1],4)
                            dfpositionBuy.iat[i,7] = round(dfpositionBuy.iat[i,6]*dfpositionBuy['PRICE'].iloc[i],4)      #
                            dfpositionBuy.iat[i,8] = round((dfpositionBuy.iat[i,8]/qty)*dfpositionSell['QTY($)'].iloc[-i-1],4)        #
                            with open("pairedOrder.csv", "a+", newline='') as fp:
                                wr = csv.writer(fp, dialect='excel')
                                wr.writerow(dfpositionSell.iloc[-i-1])
                                wr.writerow(dfpositionBuy.iloc[i])
                            dfpositionBuy.iat[i,6] = a
                            dfpositionBuy.iat[i,7] = round(cost-dfpositionBuy.iat[i,7],4)            #
                            dfpositionBuy.iat[i,8] = round(fee-dfpositionBuy.iat[i,8],4)            #
                            dfpositionSell.drop(dfpositionSell.index[-i-1],inplace = True)
                            dfpositionBuy.to_csv("holdBuy.csv",index = False, encoding = 'utf8')
                            dfpositionSell.to_csv("holdSell.csv",index = False, encoding = 'utf8')
                            break    
                        elif dfpositionBuy['QTY($)'].iloc[i] < dfpositionSell['QTY($)'].iloc[-i-1]  : 
                            a =  round(dfpositionSell['QTY($)'].iloc[-i-1] - dfpositionBuy['QTY($)'].iloc[i],4)
                            qty = round(dfpositionSell.iat[-i-1,6],4)
                            cost = round(dfpositionSell.iat[-i-1,7],4)   #
                            fee = round(dfpositionSell.iat[-i-1,8],4)    #
                            dfpositionSell.iat[-i-1,6] = round(dfpositionBuy['QTY($)'].iloc[i],4)
                            dfpositionSell.iat[-i-1,7] = round(dfpositionSell.iat[-i-1,6]*dfpositionSell['PRICE'].iloc[-i-1],4)      #
                            dfpositionSell.iat[-i-1,8] = round((dfpositionSell.iat[-i-1,8]/qty)*dfpositionBuy['QTY($)'].iloc[i],4)       #
                            with open("pairedOrder.csv", "a+", newline='') as fp:
                                wr = csv.writer(fp, dialect='excel')
                                wr.writerow(dfpositionSell.iloc[-i-1])
                                wr.writerow(dfpositionBuy.iloc[i])
                            dfpositionSell.iat[-i-1,6] = a
                            dfpositionSell.iat[-i-1,7] = round(cost-dfpositionSell.iat[-i-1,7],4)            #
                            dfpositionSell.iat[-i-1,8] = round(fee-dfpositionSell.iat[-i-1,8],4)             #
                            dfpositionBuy.drop(dfpositionBuy.index[i],inplace = True)
                            dfpositionBuy.to_csv("holdBuy.csv",index = False, encoding = 'utf8')
                            dfpositionSell.to_csv("holdSell.csv",index = False, encoding = 'utf8')
                            break    
                        elif dfpositionBuy['QTY($)'].iloc[i] == dfpositionSell['QTY($)'].iloc[-i-1]  :  
                            with open("pairedOrder.csv", "a+", newline='') as fp:
                                wr = csv.writer(fp, dialect='excel')
                                wr.writerow(dfpositionSell.iloc[-i-1])
                                wr.writerow(dfpositionBuy.iloc[i])
                            dfpositionBuy.drop(dfpositionBuy.index[i],inplace = True)
                            dfpositionSell.drop(dfpositionSell.index[-i-1],inplace = True)
                            dfpositionBuy.to_csv("holdBuy.csv",index = False, encoding = 'utf8')
                            dfpositionSell.to_csv("holdSell.csv",index = False, encoding = 'utf8')
                            break

                if SELLQTY < BUYQTY :
                    for i in range (len(dftradinglog)) :
                        if dfpositionBuy['QTY($)'].iloc[-i-1] > dfpositionSell['QTY($)'].iloc[i]  :  
                            a =  round(dfpositionBuy['QTY($)'].iloc[-i-1] - dfpositionSell['QTY($)'].iloc[i],4)
                            qty = round(dfpositionBuy.iat[-i-1,6],4)
                            cost = round(dfpositionBuy.iat[-i-1,7],4)   #
                            fee = round(dfpositionBuy.iat[-i-1,8],4)    #
                            dfpositionBuy.iat[-i-1,6] = round(dfpositionSell['QTY($)'].iloc[i],4)
                            dfpositionBuy.iat[-i-1,7] = round(dfpositionBuy.iat[-i-1,6]*dfpositionBuy['PRICE'].iloc[-i-1],4)      #
                            dfpositionBuy.iat[-i-1,8] = round((dfpositionBuy.iat[-i-1,8]/qty)*dfpositionSell['QTY($)'].iloc[i],4)        #
                            with open("pairedOrder.csv", "a+", newline='') as fp:
                                wr = csv.writer(fp, dialect='excel')
                                wr.writerow(dfpositionSell.iloc[i])
                                wr.writerow(dfpositionBuy.iloc[-i-1])
                            dfpositionBuy.iat[-i-1,6] = a
                            dfpositionBuy.iat[-i-1,7] = round(cost-dfpositionBuy.iat[-i-1,7],4)            #
                            dfpositionBuy.iat[-i-1,8] = round(fee-dfpositionBuy.iat[-i-1,8],4)             #
                            dfpositionSell.drop(dfpositionSell.index[i],inplace = True)
                            dfpositionBuy.to_csv("holdBuy.csv",index = False, encoding = 'utf8')
                            dfpositionSell.to_csv("holdSell.csv",index = False, encoding = 'utf8')
                            break    
                        elif dfpositionBuy['QTY($)'].iloc[-i-1] < dfpositionSell['QTY($)'].iloc[i]  : 
                            a =  round(dfpositionSell['QTY($)'].iloc[i] - dfpositionBuy['QTY($)'].iloc[-i-1],4)
                            qty = round(dfpositionSell.iat[i,6],4)
                            cost = round(dfpositionSell.iat[i,7],4)   #
                            fee = round(dfpositionSell.iat[i,8],4)    #
                            dfpositionSell.iat[i,6] = round(dfpositionBuy['QTY($)'].iloc[-i-1],4)
                            dfpositionSell.iat[i,7] = round(dfpositionSell.iat[i,6]*dfpositionSell['PRICE'].iloc[i],4)      #
                            dfpositionSell.iat[i,8] = round((dfpositionSell.iat[i,8]/qty)*dfpositionBuy['QTY($)'].iloc[-i-1],4)        #
                            with open("pairedOrder.csv", "a+", newline='') as fp:
                                wr = csv.writer(fp, dialect='excel')
                                wr.writerow(dfpositionSell.iloc[i])
                                wr.writerow(dfpositionBuy.iloc[-i-1])
                            dfpositionSell.iat[i,6] = a
                            dfpositionSell.iat[i,7] = round(cost-dfpositionSell.iat[i,7],4)            #
                            dfpositionSell.iat[i,8] = round(fee-dfpositionSell.iat[i,8],4)             #
                            dfpositionBuy.drop(dfpositionBuy.index[-i-1],inplace = True)
                            dfpositionBuy.to_csv("holdBuy.csv",index = False, encoding = 'utf8')
                            dfpositionSell.to_csv("holdSell.csv",index = False, encoding = 'utf8')
                            break    
                        elif dfpositionBuy['QTY($)'].iloc[-i-1] == dfpositionSell['QTY($)'].iloc[i]  :  
                            with open("pairedOrder.csv", "a+", newline='') as fp:
                                wr = csv.writer(fp, dialect='excel')
                                wr.writerow(dfpositionSell.iloc[i])
                                wr.writerow(dfpositionBuy.iloc[-i-1])
                            dfpositionBuy.drop(dfpositionBuy.index[-i-1],inplace = True)
                            dfpositionSell.drop(dfpositionSell.index[i],inplace = True)
                            dfpositionBuy.to_csv("holdBuy.csv",index = False, encoding = 'utf8')
                            dfpositionSell.to_csv("holdSell.csv",index = False, encoding = 'utf8')
                            break

def updateposition():

    QTYneworders = 0

    for i in range (MarkupFuction,len(dftradinglog),1):
        if((dftradinglog['ID'][i] not in (dfpositionBuy['ID'].tolist())) and (dftradinglog['ID'][i] not in (dfpositionSell['ID'].tolist())) and (dftradinglog['ID'][i] not in (dfpaired['ID']).tolist())):
            QTYneworders += 1
    print("QTY NEW ORDERS =",QTYneworders)

    for i in range (MarkupFuction,len(dftradinglog),1):  # update tradinglog ใหม่ล่าสุด ลงใน position
        if((dftradinglog['ID'][i] not in (dfpositionBuy['ID'].tolist())) and (dftradinglog['ID'][i] not in (dfpositionSell['ID'].tolist())) and (dftradinglog['ID'][i] not in (dfpaired['ID']).tolist())):
            if dftradinglog['SIDE'][i] == "buy" :
                with open("holdBuy.csv", "a+", newline='') as fp:
                    wr = csv.writer(fp, dialect='excel')
                    wr.writerow(dftradinglog.loc[i])       
            elif dftradinglog['SIDE'][i] == "sell" :
                with open("holdSell.csv", "a+", newline='') as fp:
                    wr = csv.writer(fp, dialect='excel')
                    wr.writerow(dftradinglog.loc[i])
            updatepaired()

def getCalculateCF() :
    print("CALCULATING PNL","(",portName,")")
    dfPaired    = pd.read_csv("pairedOrder.csv")
    dfCalculate = pd.DataFrame([])

    dfPairedBuy = dfPaired.loc[(dfPaired['SIDE'] == 'buy')].reset_index()
    dfPairedSell = dfPaired.loc[(dfPaired['SIDE'] == 'sell')].reset_index()

    dfCalculate['TIMESTAMP']  = dfPairedSell['TIMESTAMP']
    dfCalculate['PRICE BUY']  = dfPairedBuy['PRICE']
    dfCalculate['PRICE SELL'] = dfPairedSell['PRICE']
    dfCalculate['QTY($)']     = dfPairedSell['QTY($)']
    dfCalculate['PNL($)']     = (dfPairedSell['PRICE']-dfPairedBuy['PRICE'])*dfPairedSell['QTY($)']-(dfPairedSell['FEE($)']+dfPairedBuy['FEE($)'])

    dfCalculate['TIMESTAMP'] = pd.to_datetime(dfCalculate['TIMESTAMP'], unit='ms')

    PNL = dfCalculate[dfCalculate['TIMESTAMP']>datetime(2020,8,6)]
    SUMPNL0 = PNL.resample('D', on='TIMESTAMP')['PNL($)'].sum()
    SUMPNL = SUMPNL0.to_frame('SUM').reset_index()
    ACCUMPNL = SUMPNL0.cumsum()                           # ได้ตารางออกมาเป็น Type series
    ACCUMPNL = ACCUMPNL.to_frame('ACCUM').reset_index()   # แปลงกลับมาให้เป็น DataFrame
    if len(ACCUMPNL['ACCUM']) > 0 :
        print("ACC.CF =",round(ACCUMPNL['ACCUM'].iloc[-1],6))
    else :
        print("NO TRANSACTION")

### CREATE ORDER ------------------------------------------------------------------------------------------------------------------------

def createAllPendingSell():
    print("CREATE PENDING SELL")
    price = getPrice()
    for i in range (len(series)):
        pendingsellprice = sum(series[0:i+1])*10
        priceSeries = round(price+pendingsellprice,4)
        markup = pd.read_csv(MarkupFileName)
        for i in range (len(markup)):
            if priceSeries > markup.iloc[i,1] and priceSeries <= markup.iloc[i,2] :
                markUpExposure = markup.iloc[i,3]
                size = round(-(markUpExposure - getNowExposure() + getPendingSell()['amount'].sum()),4)
                if size > 0 :
                    print ('CREATE S LMT PRICE :',priceSeries,', SIZE :',size,', COST :',round(priceSeries*size,2))
                    exchange.create_limit_sell_order(pair,size,priceSeries)
                    
def createAllPendingBuy():
    print("CREATE PENDING BUY")
    price = getPrice()
    for i in range (len(series)):
        pendingbuyprice = sum(series[0:i+1])*10
        priceSeries = round(price-pendingbuyprice,4)
        markup = pd.read_csv(MarkupFileName)
        for i in range (len(markup)):
            if priceSeries > markup.iloc[i,1] and priceSeries <= markup.iloc[i,2] :
                markUpExposure = markup.iloc[i,3]
                size = round(markUpExposure - getNowExposure() - getPendingBuy()['amount'].sum(),4)
                if size > 0 :
                    print('CREATE B LMT PRICE :',priceSeries,'SIZE :',size,'COST :',round(priceSeries*size,2))
                    exchange.create_limit_buy_order(pair,size,priceSeries)

def createNewOrder(): # เช็คว่ามีmatch orderใหม่ก่อน
    msgNewOrder = ''
    if getCheckNewMatch() == "YES" and getLastSide() == 'sell':
        cancelallpendingbuy()
        createAllPendingBuy()
    elif getCheckNewMatch() == "YES" and getLastSide() == 'buy': 
        cancelallpendingsell()
        createAllPendingSell()
    elif getCheckNewMatch() == "NO":
        msgNewOrder = 'NO ACTION'

def createRebalance() :
    price = getPrice()
    markup = pd.read_csv(MarkupFileName)
    for i in range (len(markup)):
        if price > markup.iloc[i,1] and price <= markup.iloc[i,2] :
            LowerPrice = markup.iloc[i,1]
            UpperPrice = markup.iloc[i,2]
            markUpExposure = markup.iloc[i,3]
    if getNowExposure()-markUpExposure > 0.0001 :
        size = round(getNowExposure()-markUpExposure,4)
        price = LowerPrice
        if size > 0.0001 :
            print('REBALANCE SELL PRICE :',price,'SIZE :',size)
            exchange.create_limit_sell_order(pair,size,price)
    elif markUpExposure-getNowExposure() > 0.0001 :
        size = round(markUpExposure - getNowExposure(),4)
        price = UpperPrice
        if size > 0.0001 :
            print('REBALANCE BUY PRICE :',price,'SIZE :',size)
            exchange.create_limit_buy_order(pair,size,price)
    else :
        print('NO REBALANCE')
    getUpdateRecord()

### CANCEL ORDER ---------------------------------------------------------------------------------------------------------------------------

def cancelallpendingbuy():
    checkpendingbuylist = getPendingBuy().values.tolist()
    for i in range (len(checkpendingbuylist)):
        exchange.cancel_order(checkpendingbuylist[i][0])
        print("CANCELED :",checkpendingbuylist[i])
        print(" ")

def cancelallpendingsell():
    checkpendingselllist = getPendingSell().values.tolist()
    for i in range (len(checkpendingselllist)):
        exchange.cancel_order(checkpendingselllist[i][0])
        print("CANCELED :",checkpendingselllist[i])
        print(" ")

### MAIN LOOP ---------------------------------------------------------------------------------------------------------------------------

print("CHECK REBALANCE & CREATE NEW PENDING")
print(pair)
exchange.cancel_all_orders()
createRebalance()                 # Rebalance ครั้งแรกให้ Exposure = Market price
createAllPendingSell()
createAllPendingBuy()
MAINLOOP = True

while MAINLOOP:
    #try:
        print(portName)
        print(getTime())
        print(pair,':',getPrice())
        print('DIFF FROM LAST TRADE :',round(getDiffFromLastTrade(),4))
        createNewOrder()
        getUpdateRecord()
        getDetails()

        ### CALCULATE CF LOOP

        MarkupFuction = 0
        CALCFLOOP = True
        while CALCFLOOP :
            
            dftradinglog    = pd.read_csv("tradingLog.csv")
            dfpaired        = pd.read_csv("pairedOrder.csv")
            dfpositionBuy   = pd.read_csv("holdBuy.csv")
            dfpositionSell  = pd.read_csv("holdSell.csv")

            BUYQTY = dfpositionBuy['QTY($)'].sum()
            SELLQTY = dfpositionSell['QTY($)'].sum()

            #print("UPDATE AND PAIRING POSITION PLEASE WAIT A MINUTE :)","(",portName,")")
            
            if BUYQTY != 0 and SELLQTY != 0 :
                updatepaired()
            elif BUYQTY == 0 or SELLQTY == 0 :             
                updateposition()
                for i in range (0,len(dftradinglog),1):
                    if ((dftradinglog['ID'][i] in (dfpositionBuy['ID'].tolist())) or (dftradinglog['ID'][i] in (dfpositionSell['ID'].tolist())) or (dftradinglog['ID'][i] in (dfpaired['ID']).tolist())) :
                        getCalculateCF()
                        CALCFLOOP = False
                        break
        print("-----------------------------------")
        time.sleep(30)
    #except:
        print('SOMETHING WRONG ! TRY TO RESTART ...')
        time.sleep(50)


