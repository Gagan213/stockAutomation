# from MarketDataSocketClient import MDSocket_io
import xlwings
import iiflLogin,radheUtils
import pathlib,datetime,time,threading,json,configparser,csv,socket

EXCHANGESEGMENT={'NSECM': 1, 'NSEFO': 2, 'NSECD': 3, 'NSECO': 4, 'SLBM': 5, 'BSECM': 11, 'BSEFO': 12, 'BSECD': 13, 'BSECO': 14, 'NCDEX': 21, 'MSECM': 41, 'MSEFO': 42, 'MSECD': 43, 'MCXFO': 51, 1: 'NSECM', 2: 'NSEFO', 3: 'NSECD', 4: 'NSECO', 5: 'SLBM', 11: 'BSECM', 12: 'BSEFO', 13: 'BSECD', 14: 'BSECO', 21: 'NCDEX', 41: 'MSECM', 42: 'MSEFO', 43: 'MSECD', 51: 'MCXFO'}
location=pathlib.Path(__file__).parent
excelFileName='Buy Above IIFL.xlsm'
excelFileLocation=location.joinpath(excelFileName)
userExcelFile=location.joinpath('Users.xlsx')
processLists={}
rowNoQueue=[]
tokenDict={}
outputQueue=[]

semaORDERAPI=threading.Semaphore(3)
isTickerConnected=0
startExcelPointer=7
HOST='127.0.0.1'
PORT=7010
PORT2=7011
subscriptionList=[]
wb=xlwings.Book(excelFileLocation)
ws=wb.sheets['Buy Above']
sleep=threading.Event()
HEADER=16

print("Starting...")
# liveDataUserId=ws.range('I4').value
excelRef={
    'command':'Z',
    'serial':'J',
    'userId':'K',
    'tradingSymbol':'L',
    'timePrice':'M',
    'toleranceRange':'N',
    'product':'P',
    'quantity':'Q',  #Lot
    
    'checkPriceBase':'O',
    'firstTrade':'S',
    'buyAboveD':'T',
    'sellBelowD':'U',
    
    'miniMoveMore':'V',
    'miniStopLoss':'W',
    
    'moveMorePercent':'X',
    'newStopLossPercent':'Y',
    # 'order_type':'AA',
    # 'variety':'AB',
    # 'validity':'AC',
}

excelOutputRef={
    # 'tradedPrice':'V',
    # 'OpenPosition':'W',
    # 'OpenPositionType':'X', 
    'response':'AA',
    'orderId':'AB',
    'message':'AC',
    'buyAbove':'AD',
    'sellBelow':'AE',
    'moveMore':'AF',
    'newTrailingStopLoss':'AG',
}
excelStaticOutputRef={
    'ltp':'R',
}

# def logged(user):
#     result=LoggedUsers.get(user)
#     if result==None:
#         loginResult=iiflLogin.loginEasy(user,userExcelFile)
#         if loginResult.get('status'):
#             kite=loginResult.get('blaze')
#             LoggedUsers[user]=kite
#             return {'status':1,'kite':kite}
#         else:
#             return loginResult
#     print("Already Logged Found")
#     return {'status':1,'kite':result}

# def loadInstrumentFile():
#     #Not Started Yet Not used
#     with open(file,'r') as f:
#     header=['ExchangeSegment','ExchangeInstrumentID','InstrumentType','Name','Description','Series','NameWithSeries','InstrumentID']
#     read=csv.DictReader(f,fieldnames=header,delimiter="|")

def client():
    global tokenDict,isTickerConnected
    ADDR=(HOST,PORT)
    c=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    c.connect(ADDR)
    while True:
        data=radheUtils.advanceReceive(c)
        try:
            data=json.loads(data)
            try:
                isTickerConnected=data.pop('isTickerConnected')
            except:
                pass
            tokenDict=data
            # print('liveData Updated')
        except:
            print(f'Json Object Not received {data}')
            continue

                               
threading.Thread(target=client).start()

def subscribe(instrumentDual):
    key=f'{instrumentDual.get("exchangeInstrumentID")}{instrumentDual.get("exchangeSegment")}'
    while True:
        try:
            if type(tokenDict.get(key).get('ltp')) in [float,int]:
                print("Successfully instrument token subscribed.")
                break
        except Exception as e:
            print("Waiting to get live data...")
            time.sleep(0.4)
        
            
   
def subscribeClient():
    
    
    while True:
        find=0
        try:
            popped=subscriptionList.pop(0)
            find=1
        except:
            time.sleep(1)
        if find==1:
            subscribeSocket=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            ADDR2=(HOST,PORT2)
            subscribeSocket.connect(ADDR2)
            
            msg=json.dumps(popped)
            radheUtils.advanceSend(subscribeSocket,msg,HEADER)
            # data=radheUtils.advanceReceive(subscribeSocket)
            # if data=='1':
            #     print(f'Subscribed {popped}')
            # elif data=='0':
            #     print(f'Unable to subscribe {popped}')
            # else:
            #     print("Unknown response")
            subscribeSocket.close()
            # popped['result']=data
                
        
        
threading.Thread(target=subscribeClient).start()
# Callback for tick reception.
def orderDecoder():
    wb1=xlwings.Book(excelFileLocation)
    ws1=wb1.sheets['Buy Above']
    while True:
        try:
            find=0
            popped=None
            invalidDataFlag=0
            try:
                popped=rowNoQueue.pop(0)
                find=1
            except:
                time.sleep(1)
            if find==1:
                command=popped[1]
                excelRowId=popped[0]
                if command in ['0',0,'c']:
                    print(f"c command received from row no {excelRowId}")
                                    
                    #Copy the row from excel
                    item={}
                    item['excelRowId']=excelRowId
                    for i in excelRef.keys():
                        item[f'{i}']=radheUtils.upp(ws1.range(f'{excelRef.get(i)}{excelRowId}').value)    
    
                    item['orderType']='MARKET'
                    item['validity']='DAY'
                    item['command']=command
                    item['instrument']=item.get('tradingSymbol') #item.get('base')
                    # if item.get('LTPSource')!=None:
                    #     item['instrument']=
                        
                    result=radheUtils.search(item.get('instrument'))
                    print(result)
                    # print(item.get('instrument'))
                    # print(item.get('base'))
                    # print(result)
                    if result==0 or item.get('firstTrade') not in ['BUY','SELL']:
                        invalidDataFlag=1
                    else:
                        if type(result)==dict:
                            item['ltpDual']=result
                            item.update(result)
                            item['ltpKey']=f'{result.get("exchangeInstrumentID")}{result.get("exchangeSegment")}'
                        else:
                            invalidDataFlag=1
                    print(item)

                    item['semaphoreOrderAPI']=semaORDERAPI
                    output={}
                    for i in excelOutputRef.values():
                        output[i]=''
                    outputQueue.append({'excelRowId':item.get('excelRowId'),'data':output})                   
                    if invalidDataFlag==0:
                        #Killing if any previous thread running

                        rowId=item.get('excelRowId')
                        if processLists.get(item.get('excelRowId'))!=None:
                            print(f"Killing the existing thread associated with {rowId}")
                            processLists[rowId]['stopFlag2']=True
                            processLists[rowId]['stopFlag']=True
                            if processLists[rowId].get('wait')!=None:
                                try:
                                    processLists[rowId].get('wait').set()
                                except:
                                    pass
                            processLists.pop(rowId)
                        processLists[rowId]={}
                        processLists[rowId]['stopFlag']=False
                        processLists[rowId]['stopFlag2']=False
                        print(processLists)
                        copyItem=item.copy()
                        threading.Thread(target=priceOrder, args=(copyItem,processLists.get(rowId))).start()
                    else:
                        outputQueue.append({'excelRowId':item.get('excelRowId'),'data':{excelOutputRef.get('response'):'Error',excelOutputRef.get('message'):'Invalid Data Found'}})
                        # print(f"Invalid Data in row No. {item.get('excelRowId')}")
                elif command in ['9',9,'x']:
                    print(f"x/stop command received from row no {popped[0]}")
                    try:
                        dictObject=processLists[excelRowId]
                        dictObject['stopFlag2']=True
                        dictObject['stopFlag']=True
                        if dictObject.get('wait')!=None:
                            dictObject.get('wait').set()
                        processLists.pop(excelRowId)
                        outputQueue.append({'excelRowId':excelRowId,'data':{excelOutputRef.get('response'):'Stopping Thread',excelOutputRef.get('message'):'Stopping The Thread'}})
                    except Exception as e:
                        outputQueue.append({'excelRowId':excelRowId,'data':{excelOutputRef.get('response'):'No Active Thread'}})
        
                elif command=='r':
                    threading.Thread(target=updateLTP).start()
        except Exception as e:
            print(e)


def updateLTP():
    pnt=startExcelPointer
    wb=xlwings.Book(excelFileLocation)
    ws=wb.sheets['Buy Above']
    while ws.range(f"{excelRef.get('serial')}{pnt}").value!=None:
        try:
            colName=excelRef.get("tradingSymbol")                
                
            result=radheUtils.search(ws.range(f'{colName}{pnt}').value)
            if result!=0:
                key=f'{result.get("exchangeInstrumentID")}{result.get("exchangeSegment")}'
                try:
                    ltp=tokenDict[key]['ltp']
                except KeyError:
                    subscriptionList.append(result)
                    subscribe(result)
                    ltp=tokenDict[key]['ltp']
                outputQueue.append({'excelRowId':pnt,'data':{excelStaticOutputRef.get('ltp'):ltp}})
            else:
                outputQueue.append({'excelRowId':pnt,'data':{excelStaticOutputRef.get('ltp'):''}})
            pnt+=1
        except Exception as e:
            print(e)
                
                               
def priceOrder(item,dictObject):
    if isTickerConnected:
        def hi():
            pass
        temp=str(item.get('timePrice'))
        if temp.find(':')!=-1:
            #it is a time state
            timeDiff=radheUtils.getTimeFromString(temp)
            if timeDiff==None or timeDiff<0:
                outputQueue.append({'excelRowId':item.get('excelRowId'),'data':{excelOutputRef.get('response'):'Error',excelOutputRef.get('message'):'Invalid Time or time has already been passed..'}})
                print('Invalid Time or time has already been passed.')
                return 0
            else:
                timeDiff=timeDiff-5
                outputQueue.append({'excelRowId':item.get('excelRowId'),'data':{excelOutputRef.get('response'):'Time Waiting',excelOutputRef.get('message'):f'Remaining Time is {timeDiff}'}})
                print(f'Remaining Time in Seconds = {timeDiff}')
                processLists[item.get('excelRowId')]['wait']=threading.Event()
                processLists[item.get('excelRowId')]['wait'].wait(timeDiff)
                if dictObject.get('stopFlag'):
                    outputQueue.append({'excelRowId':item.get('excelRowId'),'data':{excelOutputRef.get('response'):'Stopped',excelOutputRef.get('message'):'Stopped Before Time Passed.'}})
                    return 0
                
                subscriptionList.append(item.get('ltpDual'))
                subscribe(item.get('ltpDual'))
                ltp=tokenDict.get(item.get('ltpKey'),{}).get('ltp')
                print(ltp)
        else:
            try:
                item['timePrice']=float(item.get('timePrice'))
                floatFlag=1
            except:
                floatFlag=0
            if floatFlag==1:
                #It is a Price  State
                # subscribe(liveTicker,item.get('ltpDual'))
                subscriptionList.append(item.get('ltpDual'))
                subscribe(item.get('ltpDual'))

                try:
                    positiveValue=abs(item.get('toleranceRange'))
                except:
                    print("Range is not defined properly")
                    outputQueue.append({'excelRowId':item.get('excelRowId'),'data':{excelOutputRef.get('response'):'Error',excelOutputRef.get('message'):'Range is not defined Properly'}})
                    return 0
                    
                Lcheck=item.get('timePrice')-positiveValue
                Ucheck=Lcheck + (positiveValue *2)
                print(f'Lower Check = {Lcheck}, Upper Check = {Ucheck}')
                #Let's wait for condition activation
                def CheckFunction():
                    ltp=tokenDict.get(item.get('ltpKey'),{}).get('ltp')
                    return (ltp>=Lcheck and ltp<=Ucheck) or dictObject.get('stopFlag')
                radheUtils.conditionStopper(CheckFunction,hi)
                if dictObject.get('stopFlag'):
                    print('Program Stoppped Before Condition Activation')
                    outputQueue.append({'excelRowId':item.get('excelRowId'),'data':{excelOutputRef.get('response'):'Error',excelOutputRef.get('message'):'Program Stoppped Before Condition Activation or in tolerance range'}})
                    return 0
                # item['buyAbove']=item.get('timePrice') + item.get('buyAboveD')
                # item['sellBelow']=item.get('timePrice') + item.get('sellBelowD')
                print("Condition Activated.")
                outputQueue.append({'excelRowId':item.get('excelRowId'),'data':{excelOutputRef.get('response'):'Condition Activated'}})
            else:
                outputQueue.append({'excelRowId':item.get('excelRowId'),'data':{excelOutputRef.get('response'):'Error',excelOutputRef.get('message'):'Neither Price nor time Condition Detected.'}})
                return 0

        
        def baseFunc():
            return tokenDict.get(item.get('ltpKey')).get('ltp')
        # def baseFunc():
        #     return tempLTP
        if radheUtils.low(item.get('checkPriceBase'))=='ask-bid':
            
            def bid():
                return tokenDict.get(item.get('ltpKey')).get('depthB')
            def offer():
                return tokenDict.get(item.get('ltpKey')).get('depthS')
            item['buyLtpFunction']=offer
            item['sellLtpFunction']=bid
        else:            
            item['buyLtpFunction']=baseFunc
            item['sellLtpFunction']=baseFunc        
        
        ltp=item['buyLtpFunction']()
        item['buyAbove']= ltp + item.get('buyAboveD')
        item['sellBelow']=ltp - item.get('sellBelowD')
        outputQueue.append({'excelRowId':item.get('excelRowId'),'data':{f'{excelOutputRef.get("buyAbove")}':item['buyAbove'],f'{excelOutputRef.get("sellBelow")}': item['sellBelow']}})
        
        
        while True:
            item['transaction_type']=item.get('firstTrade')
            outputQueue.append({'excelRowId':item.get('excelRowId'),'data':{excelOutputRef.get('response'):'Waiting',excelOutputRef.get('message'):'Waiting for First Trade Condition to happen'}})

            transaction='SELL' if item.get('firstTrade')=='BUY' else 'BUY'
            dictObject['stopFlag2']=False #Ensure That stopflag2 is false 
            threading.Thread(target=trailingStopLoss, args=(item,transaction,dictObject)).start() 
            priceOrderHeart(item,dictObject,item.get('transaction_type'))
            dictObject['stopFlag2']=True  #This will stop trailing stop loss (of reverse trade)
            if dictObject.get('stopFlag'):
                outputQueue.append({'excelRowId':item.get('excelRowId'),'data':{excelOutputRef.get('response'):'Stopped',excelOutputRef.get('message'):'Stopped Before First Trade'}})
                return 0
            
            #Place an order and check position
            print("Let's Place an Order")
            outputQueue.append({'excelRowId':item.get('excelRowId'),'data':{excelOutputRef.get('response'):'Condition True',excelOutputRef.get('message'):'Login Process Starts'}})
            # z=zerodhaLogin.loginThroughFile(item.get('userId'),wb2)
            # loginResult=logged(item.get('userId'))
            loginResult=iiflLogin.loginWithToken(item.get('userId'),userExcelFile)
            print(loginResult)
            if loginResult.get('status'):
                outputQueue.append({'excelRowId':item.get('excelRowId'),'data':{excelOutputRef.get('message'):'Login Done'}})
                item['kite']=loginResult.get('blaze')
                # item['order_type']='MARKET' # Already order_type is 'MARKET'
                if dictObject.get('stopFlag'):
                    outputQueue.append({'excelRowId':item.get('excelRowId'),'data':{excelOutputRef.get('response'):'Stopped',excelOutputRef.get('message'):'Stopped Before First Trade'}})
                    return 0
                res=iiflLogin.placeOrderGiveConfirmation(item) #placeOrderGiveConfirmation(item)
                if res.get('confirm')=='Filled':
                    outputQueue.append({'excelRowId':item.get('excelRowId'),'data':{excelOutputRef.get('response'):'Order Placed',excelOutputRef.get('message'):'First Order Executed',excelOutputRef.get('orderId'):res.get('orderId')}})
                    print("First Trade Executed")
                    #First Order Is Successfully COMPLETE
                    dictObject['stopFlag2']=False #Ensure That stopflag2 is false 
                    threading.Thread(target=miniTrailingStopLoss, args=(item,item.get('firstTrade'),dictObject)).start()

                    #Let's Start the stoploss.
                    if item.get('firstTrade')=='BUY':
                        item['transaction_type']='SELL'
                    elif item.get('firstTrade')=='SELL':
                        item['transaction_type']='BUY'
                    outputQueue.append({'excelRowId':item.get('excelRowId'),'data':{excelOutputRef.get('response'):'Waiting',excelOutputRef.get('message'):'Waiting for Stop Loss'}})
                    priceOrderHeart(item,dictObject,item.get('transaction_type'))
                    dictObject['stopFlag2']=True #This will stop the trailingStopLoss Thread
                    if dictObject.get('stopFlag'):
                        outputQueue.append({'excelRowId':item.get('excelRowId'),'data':{excelOutputRef.get('response'):'Stopped',excelOutputRef.get('message'):'Stopped Before Stop Loss Trade'}})
                        return 0
                    #IF program is here StopLoss Triggered
                    stopLossResult=iiflLogin.placeOrderGiveConfirmation(item)
                    
                    if stopLossResult.get('confirm')=='Filled':
                        outputQueue.append({'excelRowId':item.get('excelRowId'),'data':{excelOutputRef.get('response'):'Stop Loss Triggered',excelOutputRef.get('message'):'Base Position Exit'}})
                        #Stop Loss is successful so place pair order
                    else:
                        outputQueue.append({'excelRowId':item.get('excelRowId'),'data':{excelOutputRef.get('response'):'Error',excelOutputRef.get('message'):f'Unable to Place StopLoss Order {stopLossResult.get("confirm")} {stopLossResult.get("msg")}'}})
                        print("Stop Loss Order Unable to execute")
                        return 0

                else:
                    outputQueue.append({'excelRowId':item.get('excelRowId'),'data':{excelOutputRef.get('response'):'Error',excelOutputRef.get('message'):f'Unable to Place Order {res.get("confirm")} {res.get("msg")}'}})
                    print(f"Order is Not Placed {res.get('msg')}")
                    print("Order is Rejected/Cancelled")
                    return 0
            else:
                outputQueue.append({'excelRowId':item.get('excelRowId'),'data':{excelOutputRef.get('response'):'Stopped',excelOutputRef.get('message'):f'Login Failed Process Stopped {loginResult.get("msg")}'}})
                # print(f"Unable to Login {item.get('UserId')}")
                return 0
    else:
        outputQueue.append({'excelRowId':item.get('excelRowId'),'data':{excelOutputRef.get('response'):'Not Connected To Ticker'}})                
        print("Not Connected to ticker")



def trailingStopLoss(item,transaction_type,dictObject):
    def trueFun():
            pass
    while True:        
        if transaction_type=='BUY':
            item['moveMore']=round(item.get('buyAbove') * item.get('moveMorePercent') /100)
            item['newStopLoss']=round(item.get('buyAbove') * item.get('newStopLossPercent') /100)
            outputQueue.append({'excelRowId':item.get('excelRowId'),'data':{f'{excelOutputRef.get("moveMore")}':item['moveMore'],f'{excelOutputRef.get("newTrailingStopLoss")}': item['newStopLoss']}})
            checkValue=item.get('buyAbove')+item.get('moveMore')
            def Bcondition():
                ltp=item.get('buyLtpFunction')() #tokenDict.get(item.get('instrument_token')).get('ltp')
                # print(f'checkValue={checkValue}  ltp={ltp}')
                return ltp>=checkValue or dictObject.get('stopFlag2')
            radheUtils.conditionStopper(Bcondition,trueFun,1)
            if dictObject.get('stopFlag2'):
                return 0
            item['buyAbove']=item.get('buyAbove')+item.get('newStopLoss')
            item['sellBelow']=item.get('sellBelow')+item.get('newStopLoss')
            outputQueue.append({'excelRowId':item.get('excelRowId'),'data':{f'{excelOutputRef.get("buyAbove")}':item['buyAbove'],f'{excelOutputRef.get("sellBelow")}': item['sellBelow']}})
        elif transaction_type=='SELL':
            item['moveMore']=round(item.get('sellBelow') * item.get('moveMorePercent') /100)
            item['newStopLoss']=round(item.get('sellBelow') * item.get('newStopLossPercent') /100)
            outputQueue.append({'excelRowId':item.get('excelRowId'),'data':{f'{excelOutputRef.get("moveMore")}':item['moveMore'],f'{excelOutputRef.get("newTrailingStopLoss")}': item['newStopLoss']}})
            checkValue=item.get('sellBelow')-item.get('moveMore')
            def Scondition():
                ltp=ltp=item.get('sellLtpFunction')()
                # print(f'checkValue={checkValue}  ltp={ltp}')
                return (ltp<=checkValue and ltp!=0) or dictObject.get('stopFlag2')
            radheUtils.conditionStopper(Scondition,trueFun,1)
            if dictObject.get('stopFlag2'):
                return 0
            item['sellBelow']=item.get('sellBelow')-item.get('newStopLoss')
            item['buyAbove']=item.get('buyAbove')-item.get('newStopLoss')
            outputQueue.append({'excelRowId':item.get('excelRowId'),'data':{f'{excelOutputRef.get("buyAbove")}':item['buyAbove'],f'{excelOutputRef.get("sellBelow")}': item['sellBelow']}})
        print(item)
        print(f"Main Stop Loss Trailed... excel row id {item.get('excelRowId')} = {datetime.datetime.now()}")
        outputQueue.append({'excelRowId':item.get('excelRowId'),'data':{excelOutputRef.get('response'):'Main Stop Loss Trailed'}})
        time.sleep(1)



def miniTrailingStopLoss(item,transaction_type,dictObject):
    print("Mini Trailing Stop Loss Started...")
    def tFun():
            pass
    if transaction_type=='BUY':
        checkValue=item.get('buyAbove')+item.get('miniMoveMore')
        print(f'Check Value for Mini Stop Loss {checkValue}')
        def Bcondition():
            ltp=item.get('buyLtpFunction')() #tokenDict.get(item.get('instrument_token')).get('ltp')
            # print(f'checkValue={checkValue}  ltp={ltp}')
            return ltp>=checkValue or dictObject.get('stopFlag2')
        radheUtils.conditionStopper(Bcondition,tFun,1)
        if dictObject.get('stopFlag2'):
            return 0
        item['buyAbove']=item.get('buyAbove')+item.get('miniStopLoss')
        item['sellBelow']=item.get('sellBelow')+item.get('miniStopLoss')
        
    elif transaction_type=='SELL':
        checkValue=item.get('sellBelow')-item.get('miniMoveMore')
        print(f'Check Value for Mini Stop Loss {checkValue}')
        def Scondition():
            ltp=ltp=item.get('sellLtpFunction')()
            # print(f'checkValue={checkValue}  ltp={ltp}')
            return (ltp<=checkValue and ltp!=0) or dictObject.get('stopFlag2')
        radheUtils.conditionStopper(Scondition,tFun,1)
        if dictObject.get('stopFlag2'):
            return 0
        item['sellBelow']=item.get('sellBelow')-item.get('miniStopLoss')
        item['buyAbove']=item.get('buyAbove')-item.get('miniStopLoss')
    print(f"Mini Stop Loss Trailed... excel row id {item.get('excelRowId')} = {datetime.datetime.now()}")
    outputQueue.append({'excelRowId':item.get('excelRowId'),'data':{f'{excelOutputRef.get("buyAbove")}':item['buyAbove'],excelOutputRef.get('response'):'Mini StopLoss Trailed',f'{excelOutputRef.get("sellBelow")}': item['sellBelow']}})
    
    print("Let's Start Main Trailing Stop Loss")
    trailingStopLoss(item,item.get('firstTrade'),dictObject)
    
    
    

    
def priceOrderHeart(item,dictObject,transaction):
    def trueFunc():
        pass
    if transaction=='BUY':
        print("I am buy")
        def buyCon():
            ltp=item.get('buyLtpFunction')()#tokenDict.get(item.get('instrument_token')).get('ltp')
            # print(f"offer price {ltp}")
            return ltp>=item.get('buyAbove') or dictObject.get('stopFlag')
        radheUtils.conditionStopper(buyCon,trueFunc)
    elif transaction=='SELL':

        print('I am sell')
        def sellCon():
            ltp=item.get('sellLtpFunction')() #tokenDict.get(item.get('instrument_token')).get('ltp',0)
            # print(f"bid price {ltp}")
            return (ltp<=item.get('sellBelow') and ltp!=0) or dictObject.get('stopFlag')
        radheUtils.conditionStopper(sellCon,trueFunc)




def positions():
    wb=xlwings.Book(excelFileLocation)
    ws=wb.sheets['Buy Above']
    while True:
        print("Positions updating")
        if radheUtils.is_connected():
            try:
                pnt=startExcelPointer
                lst={}
                dLst=[]
                while ws.range(f'{excelRef.get("serial")}{pnt}').value!=None:
                    try:
                        lst[ws.range(f'{excelRef.get("userId")}{pnt}').value].append(pnt)
                    except KeyError:
                        lst[ws.range(f'{excelRef.get("userId")}{pnt}').value]=[]
                        lst[ws.range(f'{excelRef.get("userId")}{pnt}').value].append(pnt)
                    pnt+=1
                print(lst)
                for i in lst.keys():
                    lgnRes=zerodhaLogin.loginThroughFile(i,wb)
                    if lgnRes.get('status')==1:
                        pos=lgnRes.get('kite').positions().get('net')
                        if pos!=None:
                            for j in lst.get(i):
                                for p in pos:
                                    if p.get('tradingsymbol')==ws.range(f'{excelRef.get("tradingsymbol")}{j}').value:
                                        #UPdate the Rows
                                        qty=p.get('buy_quantity')-p.get('sell_quantity')
                                        ws.range(f'{excelOutputRef.get("tradedPrice")}{j}').value=p.get('average_price')
                                        ws.range(f'{excelOutputRef.get("OpenPosition")}{j}').value=qty
                                        if qty>0:
                                            ws.range(f'{excelOutputRef.get("OpenPositionType")}{j}').value='BUY'
                                        elif qty<0:
                                            ws.range(f'{excelOutputRef.get("OpenPositionType")}{j}').value='SELL'
                                        dLst.append(j)
                                        break
                
                for m in range(startExcelPointer,pnt):
                    if m not in dLst:
                        ws.range(f'{excelOutputRef.get("tradedPrice")}{m}').value=''
                        ws.range(f'{excelOutputRef.get("OpenPosition")}{m}').value=''
                        ws.range(f'{excelOutputRef.get("OpenPositionType")}{m}').value=''

            except Exception as e:
                print(e)
                
        sleep.wait(600) #10 Minutes wait after timeout

def outputThread():
    try:
        wb=xlwings.Book(excelFileLocation)
        ws=wb.sheets['Buy Above']
        while True:
            try:
                pop=outputQueue.pop(0)
                writeOutput(ws,pop.get('excelRowId'),pop.get('data'))
            except:
                time.sleep(1)
    except Exception as e:
        print(f'Error in Output Thread {e}')
        pass
def writeOutput(ws,pnt,datas):
    for data in datas.keys():
        ws.range(f'{data}{pnt}').value=datas.get(data)
    # if data[0]!=None:
    #     ws.range(f'AA{pnt}').value=data[0]
    # if data[1]!=None:
    #     ws.range(f'AB{pnt}').value=data[1]
    # if data[2]!=None:
    #     ws.range(f'AC{pnt}').value=data[2]

threading.Thread(target=orderDecoder).start()
threading.Thread(target=outputThread).start()
# threading.Thread(target=positions).start()

pointer=startExcelPointer
ws.range(f'{excelRef.get("command")}{pointer}:{excelOutputRef.get("newTrailingStopLoss")}100').value=''


while True:
    try:
        # print("Server is running...")
        if ws.range(f'{excelRef.get("serial")}{pointer}').value==None:
            pointer=startExcelPointer
            continue
        command=radheUtils.low(ws.range(f'{excelRef.get("command")}{pointer}').value)
        if command in [0,9,'0','9','r','c','x']:
            # outputQueue.append({'excelRowId':pointer,'data':['','','']})
            rowNoQueue.append([pointer,command])
            ws.range(f'{excelRef.get("command")}{pointer}').value=f'Detected {command} on {datetime.datetime.now().strftime("%H:%M:%S")}'
        pointer+=1
    except Exception as e:
        
        print(e)