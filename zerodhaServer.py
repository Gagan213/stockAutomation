import socket,datetime,threading,time,json
import radheUtils,zerodhaLogin,pdb
import zerodhaServices
import logging,pathlib,os
#GLOBAL variables.

LogFormat='%(asctime)s - %(levelname)s = %(message)s'
location=pathlib.Path(__file__).parent
os.chdir(location)
# try:
#     os.mkdir('logs')
#     print("log folder created...")
# except:
#     pass
# logPath=location.joinpath('logs')
# logName=f'log {datetime.datetime. Qnow().strftime("%d-%m-%Y %H:%M:%S")}.log'
# logFullName=logPath.joinpath(logName)
logging.basicConfig(level=logging.DEBUG,format=LogFormat,filename='debugLogs.log')
logging.debug('Logging Started')
SERVER='127.0.0.1'
PORT=10000
ADDR=(SERVER,PORT)
HEADER=10
tokenDict={}
currentPositions={}
ORDERS={}
DataExchangeHub={}
AllRequests=[]
liveTicker=None
isTickerConnected=0
orderLock=threading.Lock()
UpdatedOrderData=[]
POSITIONS=[]
updateEvent=threading.Event()
ZERODHAUSERID='LL0199'
# userExcelFile=r"G:\Python\projects\jpdelhi\buySell\Users.xlsx"
userExcelFile='Users.xlsx'
kiteResult=zerodhaLogin.loginEasy(ZERODHAUSERID,userExcelFile)
KITE=None
if kiteResult.get('status'):
    KITE=kiteResult.get('kite')
    logging.debug(f'Login Successful {ZERODHAUSERID}')
else:
    logging.debug(f'Login failed {ZERODHAUSERID} = {kiteResult.get("msg")}')
    print('Unable to Login Zerodha Id')
    exit()
    
def on_tick(ws,ticks):
    tickComputation(ticks)



def tickComputation(ticks):
    global tokenDict
    for tick in ticks:
        # print(tick)
        # time.sleep(5)
        token=tick['instrument_token']
        ltp=tick['last_price']
        try:
            tokenDict[token]['ltp']=ltp
        except:
            tokenDict[token]={}
            tokenDict[token]['ltp']=ltp
        if tick.get('mode')=='full' and tick.get('tradable')==True:
            try:
                tokenDict[token]['depthB']=tick['depth']['buy'][0]['price']
                tokenDict[token]['depthS']=tick['depth']['sell'][0]['price']
            except Exception as e:
                print(e)
                print("Unable to get Depth details from ticker.")
        elif tick.get('tradable')==False:
            tokenDict[token]['depthB']=0
            tokenDict[token]['depthS']=0
        # print(tokenDict)


# Callback for successful connection.
def on_connect(ws,response):
    logging.debug(f'Connect Function Triggered')
    connectFunction()

def on_close(ws,code,reason):
    global isTickerConnected
    isTickerConnected=0
    logging.debug('Disconnect Function Triggerred')
    print(f"Ticker Closed {datetime.datetime.now().strftime('%H:%M:%S')}")

def on_error(ws,code,reason):
    global isTickerConnected
    isTickerConnected=0
    print(f"Live Ticker Error {datetime.datetime.now().strftime('%H:%M:%S')}")
    print(f'{code} = {reason}')

def on_message(ws,payload,is_binary):
    # print('Message is received...')
    # print(payload)
    # print('message done')
    pass
    
def on_order_update(ws, data):
    logging.debug(f'Order Update Function Triggered')
    global UpdatedOrderData
    print(f'order update = {data}')
    # pdb.set_trace()
    UpdatedOrderData.append(data)
    updateEvent.set()
    
def liveDataStart():
    global liveTicker
    loginResult=zerodhaLogin.loginEasy(ZERODHAUSERID,userExcelFile,tickerFlag=1)
    logging.debug(f'Ticker Login Started')
    if loginResult.get('status'):
        kws=loginResult.get('ticker')
        logging.debug(f'Ticker Login Success')

    # Assign the callbacks.
    kws.on_ticks = on_tick

    kws.on_connect = on_connect
    kws.on_close = on_close
    kws.on_error = on_error
    kws.on_message= on_message
    kws.on_order_update=on_order_update

    kws.connect(threaded=True)
    liveTicker=kws

def updateOrder():
    logging.debug(f'Update Order Function Started')
    global POSITIONS
    while True:
        try:
            try:
                data=UpdatedOrderData.pop(0) 
            except:
                updateEvent.clear()
                updateEvent.wait(10)
                continue
            orderId=data.get('order_id')
            status=data.get('status')   
            logging.debug(f'Update Order Processing {orderId}={status}')     
            updateOrderUnit(data)
                
            if status=='COMPLETE':
                print('Under Positions Update section')
                try:
                    result=KITE.positions()
                    POSITIONS=result.get('net',[])
                except:
                    print('positions are not fetched...')
                #Update the holdings and positions
                pass
            else:            
                if str(data.get('status_message','')).find('execution range')!=-1:
                    logging.debug(f'Execution Range Error occured')
                    print("Execution Range Error occured...")
                    data['kite']=KITE
                    data['tradingSymbol']=data.get('tradingsymbol')
                    threading.Thread(target=zerodhaServices.placeOrder,args=(data,10)).start()
        except Exception as e:
            logging.debug(f'Error happens in updateOrder Loop Function {e}')
            print(e)
    logging.debug(f'Update Order Function Closed')

def updateOrderUnit(order):
    orderId=order.get('order_id')
    status=order.get('status')
    logging.debug(f'Update Order Unit {orderId}')
    statusX=None
    try:
        orderLock.acquire()
        logging.debug(f'Update Lock Acquired')
        statusX=ORDERS[orderId]['orderStatus']
    except:
        ORDERS[orderId]={}
    if statusX not in ['COMPLETE','REJECTED','CANCELLED']:
        if status!='UPDATE':
            ORDERS[orderId]['orderStatus']=status
            ORDERS[orderId]['tradingSymbol']=order.get('tradingsymbol')
            ORDERS[orderId]['transactionType']=order.get('transaction_type')
            ORDERS[orderId]['quantity']=order.get('quantity')
            ORDERS[orderId]['product']=order.get('product')
            # ORDERS[orderId]['order_type']=order.get('order_type')
    orderLock.release()
    logging.debug(f'Update Lock Released')

def connectFunction():
    global isTickerConnected,POSITIONS
    isTickerConnected=1
    logging.debug(f'Connected to Ticker')
    print(f"Connected... to Ticker on  {datetime.datetime.now().strftime('%H:%M:%S')}")
    try:
        tempOrders=KITE.orders()
        logging.debug(f'Order Fetched.')
        for order in tempOrders:
            # id=order.get('order_id')
            # status=order.get('status')
            updateOrderUnit(order)
        logging.debug(f'All Previous Orders Updated.')
        print('All Previous Orders Updated...')
        tempPositions=KITE.positions()
        # print(tempPositions)
        POSITIONS=tempPositions.get('net',[])
        print(POSITIONS)
        print('All Previous Positions Updated...')
        
    except Exception as e:
        logging.debug(f'Error in Connect Function {e}')
        print('Error in Connect Function')
        return 0


def connectionClosed(conn,addr):
    try:
        e=DataExchangeHub.pop(addr)
        e.get('event').set()
        conn.close()
        print(f'[CONNECTION CLOSED] {datetime.datetime.now().strftime("%H:%M:%S")} Disconnected with {addr}')
    except Exception as e:
        print(e)
        pass
    conn.close()
    logging.debug(f'Connection Closed with {addr}')


def handleSend(conn,addr):
    global DataExchangeHub
    print('send handler is active')
    while addr in DataExchangeHub:
        try:
            rawData=DataExchangeHub[addr]['data'].pop(0)
            radheUtils.advanceSend(conn,rawData,HEADER)
        except IndexError:
            event=DataExchangeHub[addr]['event']
            event.clear()
            event.wait(10)
        except ConnectionAbortedError or ConnectionResetError or KeyError:
            connectionClosed(conn,addr)
    print(f'{addr} Sender End Closed')


def handlerReceive(conn,addr):
    print('receiver handler is active')
    while True:
        print('receiver')
        data=radheUtils.advanceReceive(conn,HEADER)
        print(data)
        if data==b'':
            break
        elif data!=None:
            try:
                jsonData=json.loads(data)
                if type(jsonData)!=dict:
                    raise Exception
            except Exception as e:
                print(e)
                print('Only Json data is valid')
                DataExchangeHub[addr]['data'].append(json.dumps({'status':0,'msg':'Only Json Format is allowed','data':data}))
                DataExchangeHub[addr]['event'].set()
                continue
            jsonData['ip']=addr
            AllRequests.append(jsonData)
    
    connectionClosed(conn,addr)
    print(f'{addr} receiver end closed.')                    
                               


def requestHandler():
    while True:
        try:
            request=AllRequests.pop(0)
            print('Request Found')
            logging.debug(f'Request Received {request}')
            function=None
            argsP=(request,)
            # pdb.set_trace()
            print(request)
        except:
            time.sleep(1)
            continue
        if request.get('code')==1:
            function=subscribe
            # subscribe(request)
        elif request.get('code')==2:
            #Get Live Data
            pass
        elif request.get('code')==3:
            #Place Order and Pass the output to customer.
            # global KITE
            # request.get('userId')
            # request['kite']=KITE
            function=orderPlaceHigh
        elif request.get('code')==4:
            function=orderStatusRequest
        elif request.get('code')==5:
            #Get positions
            function=getPositions
        elif request.get('code')==6:
            function=getOrders
        elif request.get('code')==7:
            #Cancel an Orders
            function=cancelOrderService
        elif request.get('code')==8:
            #modify an order
            function=modifyOrderService
        elif request.get('code')==9:
            #Place Raw Orders
            function=placeRawOrder
        else:
            d={'status':0,'msg':'No such service provided'}
            DataExchangeHub.get(request.get('ip')).get('data').append(json.dumps(d))
            DataExchangeHub.get(request.get('ip')).get('event').set()
            continue
        if function!=None:
            threading.Thread(target=function,args=argsP).start()
 

def broadcaster():
    global isTickerConnected
    logging.debug(f'Broadcaster Function is started...')
    while True:
        # print(ORDERS)
        try:
            for i in DataExchangeHub.keys():
                data={'processId':0,'flag': isTickerConnected,'tickData':tokenDict}
                DataExchangeHub.get(i).get('data').append(json.dumps(data))
                DataExchangeHub.get(i).get('event').set()
            time.sleep(0.4)
        except Exception as e:
            logging.debug(f'Error in Broadcaster {e}')
            print(f'{e} - Error in Broadcaster Function')
            
def registerOrderUpdateService(request):
    pass
def getPositions(request):
    logging.debug(f'Get Positions Request Received')
    response={'processId':request.get('processId',-1) ,'status':1,'positions':POSITIONS}
    DataExchangeHub.get(request.get('ip'),{}).get('data').append(json.dumps(response))
    DataExchangeHub.get(request.get('ip'),{}).get('event').set()

def getOrders(request):
    logging.debug(f'Get ORDERS DATA Request Received')
    response={'processId':request.get('processId',-1) ,'status':1,'orders':ORDERS}
    DataExchangeHub.get(request.get('ip'),{}).get('data').append(json.dumps(response))
    DataExchangeHub.get(request.get('ip'),{}).get('event').set()

def orderStatusRequest(request):
    logging.debug(f'Fetch Order Status Request Received')
    print("Fetching Order Status")
    if request.get('orderId')==None:
        response={'processId':request.get('processId',-1) ,'status':0,'msg':'Invalid Request Format'}
        DataExchangeHub.get(request.get('ip'),{}).get('data').append(json.dumps(response))
        DataExchangeHub.get(request.get('ip'),{}).get('event').set()
        return 0    
    try:
        status=ORDERS[request.get('orderId')].get('orderStatus')
        response={'processId':request.get('processId',-1) ,'status':1,'orderStatus':status}
    except:
        response={'processId':request.get('processId',-1) ,'status':0,'msg':'No Such Order Id Found'}
    logging.debug(f'Fetched {response}')
    DataExchangeHub.get(request.get('ip'),{}).get('data').append(json.dumps(response))
    DataExchangeHub.get(request.get('ip'),{}).get('event').set()


def sortOrders(orderIds):
    result=[]
    orders=[]
    try:
        for i in orderIds:
            orders.append(int(i))
    except:
        print('OrderIds are not int')
        return []
    print(f'sorted orders {orders.sort(reverse=True)}')
    orders.sort(reverse=True)
    return orders


def findSLM(tradingsymbol,transaction,product):
    slmIds=[]
    for i in ORDERS.keys():
        if ORDERS.get(i,{}).get('orderStatus')=='TRIGGER PENDING':
            if tradingsymbol==ORDERS.get(i,{}).get('tradingSymbol') and transaction==ORDERS.get(i,{}).get('transactionType') and product==ORDERS.get(i,{}).get('product'):
                slmIds.append(i)
    return sortOrders(slmIds)


def orderPlaceHigh(request):
    global KITE
    print("Placing Order High Function")
    logging.debug(f'Place ORder Request Received {request}')
    # print(request.get('userId'))
    # print(ZERODHAUSERID)
    if request.get('data').get('userId')!=ZERODHAUSERID:
        response={'processId':request.get('processId',-1) ,'status':0,'msg':'Zerodha Users are different for server and client'}
        logging.debug(f'Response Place Order Function {response}')    
        DataExchangeHub.get(request.get('ip'),{}).get('data').append(json.dumps(response))
        DataExchangeHub.get(request.get('ip'),{}).get('event').set()
        return 0
    item=request.get('data')
    qty=item.get('quantity')
    ## Let's Find out If there is any SLM Order or not
    result=findSLM(item.get('tradingSymbol'),item.get('transaction_type'),item.get('product'))
    result2=[]
    for i in result:
        result2.append(str(i))
    if result2: # It means slm found.
        print("We are in SLM Found section")
        for j in result2:
            if ORDERS.get(j,{}).get('quantity')>=qty:
                # if SLM is more than modify and sell and break
                print('last')
                response=modifyOrderService({'quantity':int(qty),'orderType':'MARKET','orderId':j,'variety':'regular'},0)    
                qty=qty-ORDERS.get(j,{}).get('quantity')
                break
            elif ORDERS.get(j,{}).get('quantity')<qty:
                print('run')
                response=modifyOrderService({'orderType':'MARKET','orderId':j,'variety':'regular'},0)
                qty=qty-ORDERS.get(j,{}).get('quantity')
    if qty!=0:
        item['kite']=KITE
        item['quantity']=qty #Remaining quantity will be placed.
        response=zerodhaServices.placeOrder(item)
        logging.debug(f'Response Place Order Function {response}')    
    response['processId']=request.get('processId',-1)
    DataExchangeHub.get(request.get('ip'),{}).get('data').append(json.dumps(response))
    DataExchangeHub.get(request.get('ip'),{}).get('event').set()
    return 0

def placeRawOrder(request):
    global KITE
    print("Placing Order Raw Function")
    logging.debug(f'Place Oder Raw Request Received {request}')
    # print(request.get('userId'))
    # print(ZERODHAUSERID)
    if request.get('data').get('userId')!=ZERODHAUSERID:
        response={'processId':request.get('processId',-1) ,'status':0,'msg':'Zerodha Users are different for server and client'}
        logging.debug(f'Response Place Order Function {response}')    
        DataExchangeHub.get(request.get('ip'),{}).get('data').append(json.dumps(response))
        DataExchangeHub.get(request.get('ip'),{}).get('event').set()
        return 0
    item=request.get('data')
    item['kite']=KITE
    orderResult=zerodhaServices.placeOrder(item)
    orderResult['processId']=request.get('processId',-1)
    logging.debug(f'Response Place Order Function {orderResult}')    
    DataExchangeHub.get(request.get('ip'),{}).get('data').append(json.dumps(orderResult))
    DataExchangeHub.get(request.get('ip'),{}).get('event').set()
    return 0


def subscribe(request):
    logging.debug(f'Request = Subscribe Request {request}')    
    print('Subscribing started')
    try:
        instrumentToken=int(request['instrumentToken'])
        mode=request.get('mode','LTP')
    except Exception as e:
        response={'processId':request.get('processId',-1) ,'status':0,'msg':'Invalid format of request'}
        DataExchangeHub.get(request.get('ip'),{}).get('data').append(json.dumps(response))
        DataExchangeHub.get(request.get('ip'),{}).get('event').set()
        logging.debug(f'Invalid Subscribe Request {e}')    
        return 0
    global liveTicker
    kws=liveTicker
    token=list()
    token.append(instrumentToken)
    z=kws.subscribe(token)
    logging.debug(f'Subscribe Low Level Response {z}')    
    if mode=='LTP':
        kws.set_mode(kws.MODE_LTP,token)
    elif mode=='FULL':
        kws.set_mode(kws.MODE_FULL,token)
    if z==True:
        response={'processId':request.get('processId',-1) ,'status':1}
    else:
        response={'processId':request.get('processId',-1) ,'status':0,'msg':f'Unable to subscribe {token} response from server={z}'}
    logging.debug(f'Response Subsribe = {response}')    
    DataExchangeHub.get(request.get('ip'),{}).get('data').append(json.dumps(response))
    DataExchangeHub.get(request.get('ip'),{}).get('event').set()

def modifyOrderService(request,service=1):
    #Quantity must be in Integer Format in argument by default.
    logging.debug(f'Get Modify order request Received {request}')
    variety=request.get('variety')
    orderId=request.get('orderId')
    quantity=request.get('quantity')
    price=request.get('price')
    orderType=request.get('orderType')
    triggerPrice=request.get('triggerPrice')
    validity=request.get('validity')
    try:
        result=KITE.modify_order(variety, orderId, quantity=quantity, price=price, order_type=orderType, trigger_price=triggerPrice, validity=validity)
        response={'processId':request.get('processId',-1) ,'status':1,'orderId':orderId}
        logging.debug(f'modify Order Response {result}')
    except Exception as e:
        print(e)
        e=str(e)
        if e.find('Maximum allowed order modifications exceeded')!=-1:
            resultX=cancelAndRecreateOrder(orderId)
            if resultX.get('status')==1:
                response={'processId':request.get('processId',-1) ,'status':1,'orderId':orderId,'msg':f'Cancelled & Recreated Successfully {e}'}
            else:
                response={'processId':request.get('processId',-1) ,'status':0,'msg':f'Error while Cancelling & Recreate {resultX.get("msg")}'}
        else:
            response={'processId':request.get('processId',-1) ,'status':0,'msg':f'Unable to modify Order {e}'}
    
    if service==1:
        DataExchangeHub.get(request.get('ip'),{}).get('data').append(json.dumps(response))
        DataExchangeHub.get(request.get('ip'),{}).get('event').set()
    else:
        return response
 



def cancelOrderService(request,service=1):
    logging.debug(f'Get Cancel Order Request Received {request}')
    if request.get('orderId')==None or request.get('variety')==None:
        response={'processId':request.get('processId',-1) ,'status':0,'msg':'Missing OrderId or variety'}
        DataExchangeHub.get(request.get('ip'),{}).get('data').append(json.dumps(response))
        DataExchangeHub.get(request.get('ip'),{}).get('event').set()
        return 0
    try:
        result=KITE.cancel_order(request.get('variety'),request.get('orderId'))
        response={'processId':request.get('processId',-1) ,'status':1}
        logging.debug(f'Cancel Order Response {result}')
    except Exception as e:
        print(e)
        response={'processId':request.get('processId',-1) ,'status':0,'msg':f'Unable to Cancel Order {e}'}
    if service==1:
        DataExchangeHub.get(request.get('ip'),{}).get('data').append(json.dumps(response))
        DataExchangeHub.get(request.get('ip'),{}).get('event').set()
    else:
        return response


def cancelAndRecreateOrder(orderId):
    #First Fetch Order
    #Then Cancel the orders
    # Replace The Order
    try:
        orderData=KITE.order_history(orderId)[-1]
        print(orderData)
    except Exception as e:
        return {'status':0,'msg':e}
    #Cancel Order
    try:
        KITE.cancel_order(orderData.get('variety'),orderId)
    except Exception as e:    
        logging.debug(f'Cancel & Recreate order stopped. {e}')
        return {'status':0,'msg':e}
    orderData['kite']=KITE
    orderData['tradingSymbol']=orderData.get('tradingsymbol')
    result=zerodhaServices.placeOrder(orderData)
    print(result)
    return result

# def ourInput():
#     while True:
#         x=input()
#         if x=='1':
#             print("Start 1 service")
#         elif x=='2':
#             print("start 2 service")
#         else:
#             pass
# threading.Thread(target=ourInput).start()
def handle_client(conn,addr):
    global DataExchangeHub
    print(f'[CONNECTED] {datetime.datetime.now().strftime("%H:%M:%S")} Server Connected to {addr}')
    logging.debug(f'{addr} Connected.')    
    DataExchangeHub[addr]={}
    DataExchangeHub[addr]['data']=[]
    DataExchangeHub[addr]['event']=threading.Event()
    threading.Thread(target=handleSend,args=(conn,addr)).start()
    threading.Thread(target=handlerReceive,args=(conn,addr)).start()
    
def debugger():
    while True:
        print(tokenDict)
        time.sleep(3)
# threading.Thread(target=debugger).start() # This will be removed.
serverSocket=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
serverSocket.bind(ADDR)
def start():
    serverSocket.listen()
    logging.debug(f'Server Listening')    
    print(f'[LISTENING] {datetime.datetime.now().strftime("%H:%M:%S")} server is listening on {SERVER}:{PORT}')
    liveDataStart()
    while True:
        conn,addr=serverSocket.accept()
        time.sleep(5)
        threading.Thread(target=handle_client,args=(conn,addr)).start()
    
print(f'[STARTING] {datetime.datetime.now().strftime("%H:%M:%S")} Server is starting...')    
# threading.Thread(target=debugger).start() # This will be removed.
threading.Thread(target=requestHandler).start()
threading.Thread(target=broadcaster).start()
threading.Thread(target=updateOrder).start()
start()


