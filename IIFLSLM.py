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
processId=3
defaultRate=50
SERVER='127.0.0.1'
ADDR=(SERVER,PORT)
IIFLUSERID='IIFL6'
isTickerConnected=0
SLMstartExcelPointer=6

rowNoQueue=[]
outputQueue=[]
ReceivedData=[]

tokenDict={}
slmValues={}
processLists={}
processedData={}

sendLock=threading.Lock()
processLock=threading.Lock()
receiveEvent=threading.Event()

location=pathlib.Path(__file__).parent
excelFileName='SLM Orders.xlsx'
SLMexcelSheetName='SL-M CRITERIA'
PendingExcelSheetName='PENDING ORDERS'
excelFileLocation=location.joinpath(excelFileName)
wb=xlwings.Book(excelFileLocation)
ws=wb.sheets[SLMexcelSheetName]


excelRef={
    'defaultSLM':'G',
    'slmName':'I',
    'slmValue':'J',
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
    while True:
        data=radheUtils.advanceReceive(client)
        if data==b'':
            print("Receiver Closed")
            break
        else:
            ReceivedData.append(data)
            receiveEvent.set()


def receiveHandler():
    global ReceivedData, isTickerConnected, tokenDict
    while True:
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
            # print(temp)
            if temp==0:
                isTickerConnected=data.get('flag')
                tokenDict=data.get('tickData')
            elif temp>0 or temp==-1: 
                processedData[temp]=data
            else:
                print(f"Unexpected data {data}")
        else:
            print(f'Json Data Expected {data}')


def waitToGetServerResponse(processId):
    def hi():
        pass
    def condition():
        return processId in processedData.keys()
    radheUtils.conditionStopper(condition,hi,1)
    return processedData.pop(processId)
    

def getOrderStatus(orderId,processId):
    request={}
    request['code']=4
    request['orderId']=orderId
    processId=send(request,processId)
    if processId!=None:
        data=waitToGetServerResponse(processId)
        print(data)
        return data


def getData(code,id=None):
    request={}
    request['code']=code
    if id==None:
        processId=send(request)
    else:
        processId=send(request,id)
    if processId!=None:
        data=waitToGetServerResponse(processId)
        # print(data)
        return data


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


def subscribeInside(instrumentToken,mode='FULL'):
    if type(tokenDict.get(instrumentToken,{}).get('ltp')) in [int,float]:
        pass
    else:
        subscribe(instrumentToken,mode)


def cancelOrder(orderId,variety):
    request={}
    request['code']=7
    request['orderId']=orderId
    variety = variety.lower() if type(variety)==str else variety
    request['variety']=variety
    processId=send(request)
    if processId!=None:
        data=waitToGetServerResponse(processId)
        print(data)
        return data

    
def modifyOrder(orderId,variety,data):
    #quantity, price,orderType,triggerPrice,validity
    request={}
    request['code']=8
    request['orderId']=orderId
    variety = variety.lower() if type(variety)==str else variety
    request['variety']=variety
    request.update(data)
    processId=send(request)
    if processId!=None:
        data=waitToGetServerResponse(processId)
        print(data)
        return data


def placeOrderToLocalServer(item,confirmation=1):
    request={}
    request['code']=9
    rawItem=item.copy()
    # rawItem.pop('buyLtpFunction')
    # rawItem.pop('sellLtpFunction')      
        
    request['data']=rawItem
    processId=send(request)
    if processId!=None:
        data=waitToGetServerResponse(processId)
        if data.get('status')==1:
            if confirmation==1:
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


def sortOrders(orderIds):
    result=[]
    orders=[]
    try:
        for i in orderIds:
            orders.append(int(i))
    except:
        print('OrderIds are not int')
        return []
    orders.sort(reverse=True)
    strOrders=[]
    for i in orders:
        strOrders.append(str(i))
    return strOrders


def findSLMValue(tradingsymbol,instrumentToken):
    try:
        result=slmValues[tradingsymbol]
    except:
        result=tokenDict.get(instrumentToken,{}).get('ltp') * (defaultRate) / 100
    return result


def excelRefresh():
    global defaultRate,slmValues
    wb=xlwings.Book(excelFileLocation)
    ws=wb.sheets[SLMexcelSheetName]
    temp=ws.range(f'{excelRef.get("defaultSLM")}{SLMstartExcelPointer}').value
    #Validation
    try:
        defaultRate=int(temp)
    except:
        print("Basic SLM % Value is not Integer. 50% default is choosen")
        defaultRate=50
    pointer=SLMstartExcelPointer-1
    while True:
        pointer+=1
        itemName=ws.range(f'{excelRef.get("slmName")}{pointer}').value
        if itemName==None:
            break
        value=ws.range(f'{excelRef.get("slmValue")}{pointer}').value
        if type(value) not in [int,float]:
            print(f"Excel Row {pointer} don't have numeric value in SLM Value")
            continue        
        slmValues[itemName]=value
    print(f'Default Rate is = {defaultRate}')
    print(f'SLMValues is {slmValues}')
    

def inputForProgram():
    while True:
        try:
            i=input("")
            i=radheUtils.low(i)
            if i=='r':
                print("Refreshing..")
                excelRefresh()
        except Exception as e:
            print(e)
        

def pendingOnExcel(slmOrders):
    wb=xlwings.Book(excelFileLocation)
    ws=wb.sheets[PendingExcelSheetName]
    

if __name__=='__main__':
    excelRefresh()
    threading.Thread(target=receive).start()
    threading.Thread(target=receiveHandler).start()
    threading.Thread(target=inputForProgram).start()
    try:
        client=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        print(client.connect(ADDR))
        print("Connected to local Server...")
    except ConnectionRefusedError:
        print("Connection request refused by server")
        exit()
    while 1:
        positions=getData(5,1).get('positions',[])
        print(positions)
        netPositions=[]
        for pos in positions:
            if pos.get('quantity')==0 or (pos.get('product')=='CNC' and pos.get('quantity')<0):
                continue
            netPositions.append(pos)
        orders=getData(6,2).get('orders',{})
        slmOrders={}
        for i in orders.keys():
            if orders.get(i,{}).get('orderStatus')=='TRIGGER PENDING':
                slmOrders[i]=orders[i]
        print(slmOrders)
        for position in netPositions:
            tmpOrder={}
            allSlmOrderId=list(slmOrders.keys())
            for y in allSlmOrderId:
                if position.get('tradingsymbol')==slmOrders[y].get('tradingSymbol'):
                    if position.get('transaction_type')!=slmOrders[y].get('transactionType') and position.get('product')==slmOrders[y].get('product'): #REvise it & add product type
                        tmpOrder[y]=slmOrders.pop(y)
            qty=0
            for i in tmpOrder.keys():
                qty+=tmpOrder.get(i,{}).get('quantity')
            #Quantity caculated
            positionQtyAbs=abs(position.get('quantity'))
            if positionQtyAbs>qty:
                #Subscribe for the instrument 
                instrumentToken=str(position.get('instrument_token'))
                subscribeInside(instrumentToken)
                item={}
                item['quantity']=positionQtyAbs-qty
                item['tradingSymbol']=position.get('tradingsymbol')
                item['exchange']=position.get('exchange')
                item['order_type']='SL-M'
                factor=findSLMValue(item.get('tradingSymbol'),instrumentToken)
                if position.get('quantity')>0:
                    #Buy Position
                    item['transaction_type']='SELL'
                    factor=tokenDict.get(instrumentToken,{}).get('ltp') - factor
                else:
                    item['transaction_type']='BUY'
                    factor=tokenDict.get(instrumentToken,{}).get('ltp') + factor
                    # factor=(100+50)/100
                item['triggerPrice']=round(factor,1)
                item['validity']='DAY'
                item['variety']='regular'
                item['product']=position.get('product')
                item['userId']=IIFLUSERID
                print("Orders is Placing...")
                reply=placeOrderToLocalServer(item,0)
                if reply.get('status')==0:
                    print(f"[0x002] Not Able to Place SLM Order for {position.get('tradingsymbol')}")
                    print(reply)
            elif positionQtyAbs<qty:
                #Cancel Some Order
                sortedOrders=sortOrders(list(tmpOrder.keys()))
                for i in sortedOrders:
                    diff=qty-tmpOrder[i].get('quantity')
                    if diff>positionQtyAbs:
                        #Cancel The Order
                        replyC=cancelOrder(i,'regular') #By Default we assume that order is regular (amo & co orders are not accepted here.)
                        if replyC.get('status')==0:
                            print(f"[0x001] Unable to Cancel Order {i}")
                            print(replyC)
                            break
                        qty=diff
                    else:
                        #Modify the order
                        qtyTemp=tmpOrder[i].get('quantity') - (qty-positionQtyAbs)
                        replyM=modifyOrder(i,'regular',{'quantity':qtyTemp})
                        if replyM.get('status')==0:
                            print(f"[0x004] Unable to Modify Order {i}")
                            print(replyM)
        for i in slmOrders.keys():
            #Cancel all these orders
            replyC=cancelOrder(i,'regular')
            if replyC.get('status')==0:
                print(f'[0x003] = Unable to Cancel Order for {i}')
                print(replyC)    
        time.sleep(10)
