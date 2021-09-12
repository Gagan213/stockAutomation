import datetime
import json
import pathlib
import pickle
import socket
import threading
import time

import radheUtils
import xlwings

PORT=10000
HEADER=10
processId=1
isTickerConnected=0
startExcelPointer=10
SERVER='127.0.0.1'
ADDR=(SERVER,PORT)

rowNoQueue=[]
outputQueue=[]
ReceivedData=[]

tokenDict={}
processLists={}
processedData={}
instrumentsTemp={}

sendLock=threading.Lock()
processLock=threading.Lock()
receiveEvent=threading.Event()
location=pathlib.Path(__file__).parent

excelFileName='BuyAbove IIFL.xlsm'
excelSheetName='Buy Above IIFL'
excelFileLocation=location.joinpath(excelFileName)
wb=xlwings.Book(excelFileLocation)
ws=wb.sheets[excelSheetName]
instrumentsFile=location.joinpath('instrument.txt')
print("Loading Instrumets File...")

with open(instrumentsFile,'rb') as fp:
	instruments=pickle.load(fp)

print("Instruments file loaded successfully")

excelRef={
    'command':'R',
    'baseLTP':'N',
    'baseExchange':'O',
    'serial':'I',
    'askBidDiff':'L',
    'userId':'M',
    'tradingSymbol':'S',
    'exchange':'T',
    'timePrice':'P',
    'toleranceRange':'Q',
    'product':'V',
    'quantity':'W',
    'checkPriceBase':'U',
    'firstTrade':'X',
    'buyAboveD':'Y',
    'sellBelowD':'Z',
    'miniMoveMore':'AA',
    'miniStopLoss':'AB',
    'moveMorePercent':'AC',
    'newStopLossPercent':'AD',
    # 'order_type':'AA',
    # 'variety':'AB',
    # 'validity':'AC',
}

excelOutputRef={
    # 'tradedPrice':'V',
    # 'OpenPosition':'W',
    # 'OpenPositionType':'X', 
    'response':'AE',
    'orderId':'AF',
    'message':'AF',
    'buyAbove':'AG',
    'sellBelow':'AH',
    'moveMore':'AI',
    'newTrailingStopLoss':'AJ',
}

excelStaticOutputRef={
    'ltp':'J',
    'askBidDiff':'K',
}

def send(jsonData,usedProcessId=0):
    try:
        sendLock.acquire()
        returnData=None
        global processId
        if type(jsonData)==dict:
            if usedProcessId==0:
                processLock.acquire()
                temp=processId
                processId=processId+1
                processLock.release()
            else:
                temp=usedProcessId
            jsonData['processId']=temp
            jsonData=json.dumps(jsonData)
            radheUtils.advanceSend(client,jsonData,HEADER)
            returnData=temp
        else:
            print(f'{jsonData} = Json data can be sent only')
            returnData=None
    except Exception as e:
        print(e)
        returnData=None
    finally:
        sendLock.release()
        return returnData
    

def receive():
    global ReceivedData
    while 1:
        data=radheUtils.advanceReceive(client)
        if data==b'':
            print("Receiver Closed")
            break
        else:
            ReceivedData.append(data)
            receiveEvent.set()


def receiveHandler():
    global ReceivedData, isTickerConnected, tokenDict
    while 1:
        try:
            popped=ReceivedData.pop(0)
            # print(popped)
        except:
            receiveEvent.clear()
            receiveEvent.wait(30)
            continue
        data=json.loads(popped)        
        if type(data)==dict:
            temp=data.get('processId')
            if temp==0:
                print(data)
                isTickerConnected=data.get('flag')
                tokenDict=data.get('tickData')
                # print(tokenDict)
            elif temp>0 or temp==-1: 
                processedData[temp]=data
            else:
                print(f"Unexpected data {data}")
        else:
            print(f'Json Data Expected {data}')


def excelRowValidation(item):
    result={}
    result['status']=0
    if type(item.get('askBidDiff')) not in [int,float]:
        result['msg']=f'Number Data is expected in {excelRef.get("askBidDiff")} Column'
    
    elif item.get('serial')==None:
        result['msg']=f'{excelRef.get("serial")} column can\'t be empty'
    elif item.get('firstTrade') not in ['BUY','SELL']:
        result['msg']=f'BUY/SELL Keyword is expected in {excelRef.get("firstTrade")} Column'
    elif ( type(item.get('buyAboveD')) not in [int,float] ) or ( type(item.get('sellBelowD')) not in [int,float] ) or ( type(item.get('miniMoveMore')) not in [int,float] ) or ( type(item.get('miniStopLoss')) not in [int,float] ) or ( type(item.get('moveMorePercent')) not in [int,float] ) or ( type(item.get('newStopLossPercent')) not in [int,float] ):
        result['msg']='All these columns must contain numeric value. (Buy Above, Sell Below, Mini Stop Loss, Mini Move More, TSL Move More, TSL Value'        
    else:
        result['status']=1
    return result

  
def orderDecoder():
    wb1=xlwings.Book(excelFileLocation)
    ws1=wb1.sheets[excelSheetName]
    while 1:
        try:
            find=0
            popped=None
            invalidDataFlag=0
            try:
                popped=rowNoQueue.pop(0)
            except:
                time.sleep(1)
                continue
            
            command=popped[1]
            if command in ['0',0,'c']:
                print(f"c command received from row no {popped[0]}")
                output={}
                for i in excelOutputRef.values():
                    output[i]=''
                outputQueue.append({'excelRowId':popped[0],'data':output})  
                
                
                #Copy the row from excel
                item={}
                item['excelRowId']=popped[0]
                for i in excelRef.keys():
                    item[f'{i}']=radheUtils.upp(ws1.range(f'{excelRef.get(i)}{popped[0]}').value)    
                
                item['order_type']='MARKET'
                item['variety']='regular'
                item['validity']='DAY'
                item['command']=command

                    
                result=excelRowValidation(item)
                if result.get('status')==0:
                    outputQueue.append({'excelRowId':item.get('excelRowId'),'data':{excelOutputRef.get('response'):'Invalid Data',excelOutputRef.get('message'):result.get('msg')}})
                    continue
                result=radheUtils.search(instruments,item.get('tradingSymbol'),item.get('exchange'))
                result2=radheUtils.search(instruments,item.get('baseLTP'),item.get('baseExchange'))
                
                print(result)
                if result==0 or result2==0:
                    invalidDataFlag=1
                    outputQueue.append({'excelRowId':item.get('excelRowId'),'data':{excelOutputRef.get('response'):'Invalid Data',excelOutputRef.get('message'):'Instrument is invalid'}})
                    continue
                else:
                    item['instrument_token']=result
                    item['baseToken']=result2
                
                print(item)                  
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

            elif command in ['9',9,'x']:
                print(f"x/stop command received from row no {popped[0]}")
                try:
                    dictObject=processLists[popped[0]]
                    dictObject['stopFlag2']=True
                    dictObject['stopFlag']=True
                    if dictObject.get('wait')!=None:
                        dictObject.get('wait').set()
                    processLists.pop(popped[0])
                    outputQueue.append({'excelRowId':popped[0],'data':{excelOutputRef.get('response'):'Stopping Thread',excelOutputRef.get('message'):'Stopping The Thread'}})
                except Exception as e:
                    outputQueue.append({'excelRowId':popped[0],'data':{excelOutputRef.get('response'):'No Active Thread'}})
    
            elif command=='r':
                threading.Thread(target=updateLTP).start()
        except Exception as e:
            print(e)


def updateLTP():
    pnt=startExcelPointer
    wb=xlwings.Book(excelFileLocation)
    ws=wb.sheets[excelSheetName]
    while ws.range(f"{excelRef.get('serial')}{pnt}").value!=None:
        # print("Welcome to Update LTP FUnction ")
        try:                
            tradingS=ws.range(f'{excelRef.get("tradingSymbol")}{pnt}').value
            result=instrumentsTemp.get(tradingS,-1)
            if result==-1:
                print("via Search")
                result=radheUtils.search(instruments,tradingS,ws.range(f'{excelRef.get("exchange")}{pnt}').value)
                instrumentsTemp[tradingS]=result
            if result!=0:
                try:
                    ltp=tokenDict[result]['depthS']
                except KeyError:
                    subscribe(result,"FULL")
                    ltp=tokenDict[result]['depthS']
                askBidDiff=tokenDict.get(result,{}).get('depthS',0) - tokenDict.get(result,{}).get('depthB',0)
                outputQueue.append({'excelRowId':pnt,'data':{excelStaticOutputRef.get('ltp'):ltp,excelStaticOutputRef.get('askBidDiff'):askBidDiff}})
            else:
                outputQueue.append({'excelRowId':pnt,'data':{excelStaticOutputRef.get('ltp'):'',excelStaticOutputRef.get('askBidDiff'):''}})
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
                subscribe(item.get('baseToken'))
        else:
            try:
                item['timePrice']=float(item.get('timePrice'))
                floatFlag=1
            except:
                floatFlag=0
            if floatFlag==1:
                #It is a Price  State
                subscribe(item.get('baseToken'))
                try:
                    positiveValue=abs(item.get('toleranceRange'))
                except:
                    print("Range is not defined properly")
                    outputQueue.append({'excelRowId':item.get('excelRowId'),'data':{excelOutputRef.get('response'):'Error',excelOutputRef.get('message'):'Range is not defined Properly'}})
                    return 0
                    
                Lcheck=item.get('timePrice')-positiveValue
                Ucheck=Lcheck + (positiveValue *2)
                print(Lcheck)
                print(Ucheck)
                #Let's wait for condition activation
                def CheckFunction():
                    ltp=tokenDict.get(item.get('baseToken'),{}).get('ltp')
                    print(ltp)
                    return (ltp>=Lcheck and ltp<=Ucheck) or dictObject.get('stopFlag')
                outputQueue.append({'excelRowId':item.get('excelRowId'),'data':{excelOutputRef.get('response'):'Range Tolerance Waiting',excelOutputRef.get('message'):'Waiting for Range tolerance to complete'}})
                radheUtils.conditionStopper(CheckFunction,hi)
                if dictObject.get('stopFlag'):
                    print('Program Stoppped Before Condition Activation')
                    outputQueue.append({'excelRowId':item.get('excelRowId'),'data':{excelOutputRef.get('response'):'Error',excelOutputRef.get('message'):'Program Stoppped Before Condition Activation or in tolerance range'}})
                    return 0
                # item['buyAbove']=item.get('timePrice') + item.get('buyAboveD')
                # item['sellBelow']=item.get('timePrice') + item.get('sellBelowD')
                
            else:
                outputQueue.append({'excelRowId':item.get('excelRowId'),'data':{excelOutputRef.get('response'):'Error',excelOutputRef.get('message'):'Neither Price nor time Condition Detected.'}})
                return 0


        def baseFunc():
            return tokenDict.get(item.get('instrument_token')).get('ltp')
        # def baseFunc():
        #     return tempLTP
        if radheUtils.low(item.get('checkPriceBase'))=='ask-bid':
            subscribe(item.get('instrument_token'),'FULL')
            def bid():
                return tokenDict.get(item.get('instrument_token')).get('depthB')
            def offer():
                return tokenDict.get(item.get('instrument_token')).get('depthS')
            item['buyLtpFunction']=offer
            item['sellLtpFunction']=offer
        else:            
            subscribe(item.get('instrument_token'))
            item['buyLtpFunction']=baseFunc
            item['sellLtpFunction']=baseFunc        
        
        ltp=item['buyLtpFunction']()
        item['buyAbove']= ltp + item.get('buyAboveD')
        item['sellBelow']=ltp - item.get('sellBelowD')
        outputQueue.append({'excelRowId':item.get('excelRowId'),'data':{f'{excelOutputRef.get("buyAbove")}':item.get('buyAbove'),f'{excelOutputRef.get("sellBelow")}': item.get('sellBelow')}})
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
            found=0
            for _ in range(0,2):
                diff= tokenDict.get(item.get('instrument_token')).get('depthS') - tokenDict.get(item.get('instrument_token')).get('depthB')
                if diff<=item.get('askBidDiff'):
                    found=1 
                    time.sleep(1)
            if found==1:    
                result=placeOrderToLocalServer(item)
                if result.get('status')==1:
                    if result.get('orderStatus')=='COMPLETE':

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
                        
                        stopLossResult=placeOrderToLocalServer(item)
                        if stopLossResult.get('status')==1:
                            if stopLossResult.get('orderStatus')=='COMPLETE':
                                outputQueue.append({'excelRowId':item.get('excelRowId'),'data':{excelOutputRef.get('response'):'Stop Loss Triggered',excelOutputRef.get('message'):'Base Position Exit'}})
                            else:
                                #Warning
                                print(f"Order is {stopLossResult.get('orderStatus')}")
                                outputQueue.append({'excelRowId':item.get('excelRowId'),'data':{excelOutputRef.get('response'):'Error',excelOutputRef.get('message'):f'Unable to Place StopLoss Order {stopLossResult.get("msg")}'}})
                                return 0
                        else:
                            #Warning
                            outputQueue.append({'excelRowId':item.get('excelRowId'),'data':{excelOutputRef.get('response'):'Error',excelOutputRef.get('message'):f'Unable to Place StopLoss Order {stopLossResult.get("msg")}'}})
                            print("Stop Loss Order Unable to execute")
                            return 0
                    else:
                        outputQueue.append({'excelRowId':item.get('excelRowId'),'data':{excelOutputRef.get('response'):'Stopped',excelOutputRef.get('message'):'Order is Rejected / Cancelled'}})
                        return   0
                else:
                    outputQueue.append({'excelRowId':item.get('excelRowId'),'data':{excelOutputRef.get('response'):'Error',excelOutputRef.get('message'):f'Unable to Place Order {result.get("msg")}'}})
                    print(f"Order is Not Placed {result.get('msg')}")
                    return 0
            else:
                outputQueue.append({'excelRowId':item.get('excelRowId'),'data':{excelOutputRef.get('response'):'Error',excelOutputRef.get('message'):f'Ask Bid Diff {diff} is greater than the {item.get("askBidDiff")} diff'}})
                return  0
    else:
        outputQueue.append({'excelRowId':item.get('excelRowId'),'data':{excelOutputRef.get('response'):'Not Connected To Ticker'}})                
        print("Not Connected to ticker")


def placeOrderGiveConfirmation(item):
    # return {'confirm':'COMPLETE'}
    def true():
        pass    
    orderResult=placeOrder.placeAMOOrder(item)
    print(orderResult)
    if orderResult.get('status'):
        def orderCheck():
            sts=radheUtils.fetchOrderOutput(item.get('kite'),orderResult.get('orderId'))
            return sts.get('status') in ['COMPLETE','REJECTED','CANCELLED']
        radheUtils.conditionStopper(orderCheck,true)
        result=radheUtils.fetchOrderOutput(item.get('kite'),orderResult.get('orderId'))
        orderResult['confirm']=result.get('status')
        orderResult['msg']=result.get('msg')
        if orderResult['confirm']=='COMPLETE':
            sleep.set() #Calling Position Function to update excel positions
    return orderResult


def trailingStopLoss(item,transaction_type,dictObject):
    def trueFun():
            pass
    while True: 
        print("IN Trailing Stop Loss Function LOOP")
        print(f'{item.get("buyAbove")} = BuyAbove; {item.get("sellBelow")} = SellBelow')
        print(f'{item.get("moveMorePercent")} = moveMorePercent; {item.get("newStopLossPercent")} = newstoploss percent')
                    
        if transaction_type=='BUY':
            print("in stop loss buy condition")
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
            item['buyAbove']=item.get('buyAbove') + item.get('newStopLoss')
            item['sellBelow']=item.get('sellBelow') + item.get('newStopLoss')
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
            item['buyAbove']=item.get('buyAbove') - item.get('newStopLoss')
            outputQueue.append({'excelRowId':item.get('excelRowId'),'data':{f'{excelOutputRef.get("buyAbove")}':item['buyAbove'],f'{excelOutputRef.get("sellBelow")}': item['sellBelow']}})
        print(item)
        print("Stop Loss Trailed")
        outputQueue.append({'excelRowId':item.get('excelRowId'),'data':{excelOutputRef.get('response'):'Stop Loss Trailed'}})
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
            print(f"offer price {ltp}")
            return ltp>=item.get('buyAbove') or dictObject.get('stopFlag')
        radheUtils.conditionStopper(buyCon,trueFunc)
    elif transaction=='SELL':

        print('I am sell')
        def sellCon():
            ltp=item.get('sellLtpFunction')() #tokenDict.get(item.get('instrument_token')).get('ltp',0)
            print(f"bid price {ltp}")
            return (ltp<=item.get('sellBelow') and ltp!=0) or dictObject.get('stopFlag')
        radheUtils.conditionStopper(sellCon,trueFunc)


def waitToGetServerResponse(processId):
    def hi():
        pass
    def condition():
        return processId in processedData.keys()
    radheUtils.conditionStopper(condition,hi,1)
    return processedData.pop(processId)
    

def subscribe(instrumentToken,mode='FULL'):
    request={}
    request['code']=1
    request['instrumentToken']=instrumentToken
    request['mode']=mode
    processId=send(request)
    if processId!=None:
        data=waitToGetServerResponse(processId)
        print(data)
        print('Subscribed...')
        while True:
            if type(tokenDict.get(instrumentToken,{}).get('ltp')) in [int,float]:
                print("Start Receiving Data")
                break
            else:
                print(type(tokenDict.get(instrumentToken,{}).get('ltp')))
                print('Waiting to get Live Data')
                time.sleep(0.5)
    else:
        print("Error While Sending Subscribe request to local server")
    

def getOrderStatus(orderId,processId):
    request={}
    request['code']=4
    request['orderId']=orderId
    processId=send(request,processId)
    if processId!=None:
        data=waitToGetServerResponse(processId)
        print(data)
        return data


def placeOrderToLocalServer(item):
    request={}
    request['code']=3
    rawItem=item.copy()
    rawItem.pop('buyLtpFunction')
    rawItem.pop('sellLtpFunction')      
        
    request['data']=rawItem
    processId=send(request)
    if processId!=None:
        data=waitToGetServerResponse(processId)
        print(data)
        if data.get('status')==1:
            def condition():
                dataStatus=getOrderStatus(data.get('orderId'),processId)
                # if dataStatus.get('status')==0:
                #     return True
                return dataStatus.get('orderStatus') in ['COMPLETE','REJECTED','CANCELLED']
            def hi():
                pass
            radheUtils.conditionStopper(condition,hi,1)
            dataStatus=getOrderStatus(data.get('orderId'),processId)
            return dataStatus
        else:
            return data
    else:
        print("Error While Sending Order request to local server")
        return {'status':0,'msg':"Error While Sending Request to Local server"}


def outputThread():
    try:
        wb=xlwings.Book(excelFileLocation)
        ws=wb.sheets[excelSheetName]
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


if __name__== "__main__":
    threading.Thread(target=receive).start()
    threading.Thread(target=receiveHandler).start()
    threading.Thread(target=orderDecoder).start()
    threading.Thread(target=outputThread).start()
    try:
        client=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        print(client.connect(ADDR))
        print("Connected to local Server...")
    except ConnectionRefusedError:
        print("Connection request refused by server")
        exit()
    pointer=startExcelPointer
    ws.range(f'{excelRef.get("command")}{pointer}:{excelRef.get("command")}100').value=''
    ws.range(f'{excelOutputRef.get("response")}{pointer}:{excelOutputRef.get("newTrailingStopLoss")}100').value=''

    while 1:
        try:
            if ws.range(f'{excelRef.get("serial")}{pointer}').value==None:
                pointer=startExcelPointer
                time.sleep(1)
                continue
            command=radheUtils.low(ws.range(f'{excelRef.get("command")}{pointer}').value)
            if command in [0,9,'0','9','r','c','x']:
                rowNoQueue.append([pointer,command])
                ws.range(f'{excelRef.get("command")}{pointer}').value=f'Detected {command} on {datetime.datetime.now().strftime("%H:%M:%S")}'
            pointer+=1
        except Exception as e:
            print(e)
