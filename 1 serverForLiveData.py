from XTSConnect import XTSConnect
import configparser,pathlib,datetime,threading,iiflLogin,openpyxl
import time,json
from MarketDataSocketClient import MDSocket_io
import socket,radheUtils


liveTicker=None
isTickerConnected=0
ticksList=[]
tokenDict={}
EXCHANGESEGMENT={'NSECM': 1, 'NSEFO': 2, 'NSECD': 3, 'NSECO': 4, 'SLBM': 5, 'BSECM': 11, 'BSEFO': 12, 'BSECD': 13, 'BSECO': 14, 'NCDEX': 21, 'MSECM': 41, 'MSEFO': 42, 'MSECD': 43, 'MCXFO': 51, 1: 'NSECM', 2: 'NSEFO', 3: 'NSECD', 4: 'NSECO', 5: 'SLBM', 11: 'BSECM', 12: 'BSEFO', 13: 'BSECD', 14: 'BSECO', 21: 'NCDEX', 41: 'MSECM', 42: 'MSEFO', 43: 'MSECD', 51: 'MCXFO'}
liveDataUserId='IIFLJGDP'
instrumentDownloadFlag=False
location=pathlib.Path(__file__).parent
config=configparser.ConfigParser()
config.read(location.joinpath('config.ini'))
downloadDateString=config.get('data','InstrumentDownloadDate',fallback='2020-01-01')
d=datetime.datetime.strptime(downloadDateString,'%Y-%m-%d')
# print(downloadDateString)
today=datetime.date.today()
event=threading.Event() #This is a event which broadcast msg to all client
HEADER=16 #This is a header size which is 16Bytes

if today==d.date():
    # global instrumentDownloadFlag
    instrumentDownloadFlag=True


def liveData():
    print("Live Data Function Started")
    global liveTicker
    excelPointer=2
    lgnResult=iiflLogin.loginEasy(liveDataUserId,'Users.xlsx',1)
    if lgnResult.get('status')==0:
        #"Invalid Login"
        return 0
    elif lgnResult.get('status')==1:
        # print(lgnResult)
        tkn=lgnResult.get('result',{}).get('result',{}).get('result',{}).get('token',0)
        userId=lgnResult.get('result',{}).get('result',{}).get('result',{}).get('userID',0)
        liveTicker=lgnResult.get('blaze')
        soc=MDSocket_io(tkn,userId)
        def on_connect():
            global isTickerConnected,instrumentDownloadFlag
            isTickerConnected=1
            event.set() #Giving Update to all Clients that live data is connected
            print(f"Connected To Ticker {datetime.datetime.now()}")
            if instrumentDownloadFlag==False:
                #Let's Download File
                try:
                    print('Downloading Instrument File')
                    allSegments=['NSECM','NSEFO','NSECD','NSECO','BSECM','BSEFO','BSECD','BSECO','NCDEX','MCXFO']
                    data=liveTicker.get_master(allSegments)
                    with open('IIFLInstruments','w') as f:
                        f.write(data.get('result'))
                    with open(location.joinpath('config.ini'),'w') as fb:
                        todayString=datetime.datetime.now().strftime('%Y-%m-%d')
                        config['data']={'InstrumentDownloadDate':todayString}
                        config.write(fb)
                    instrumentDownloadFlag=True
                    print("Downloading Complete")
                except Exception as e:
                    print(f'Error while Downloading Instrument file - {e} ')
                    
                
        # Callback for message code 1501 & 1504 FULL
        def on_message1501_json_full(data):
            ticksList.append(data)
        def on_message1504_json_full(data):
            ticksList.append(data)
        def on_message1502_json_full(data):
            ticksList.append(data)
        def on_disconnect():
            global isTickerConnected
            isTickerConnected=0
            event.set() #Giving Update to all Clients that live data is connected
            print(f'Market Data Socket disconnected! {datetime.datetime.now()}')
        def on_error(data):
            """Error from the socket."""
            print('Market Data Error', data)

        # Assign the callbacks.
        soc.on_connect = on_connect
        soc.on_message1501_json_full = on_message1501_json_full
        soc.on_message1502_json_full = on_message1502_json_full
        soc.on_message1504_json_full = on_message1504_json_full
        soc.on_disconnect = on_disconnect
        soc.on_error = on_error

        # Event listener
        el = soc.get_emitter()
        el.on('connect', on_connect)
        el.on('1501-json-full', on_message1501_json_full)
        el.on('1502-json-full', on_message1502_json_full)
        el.on('1504-json-full', on_message1504_json_full)
        el.on('disconnect', on_disconnect)
        soc.connect()

def tickComputation():
    global tokenDict,ticksList
    while True:
        flag=0
        result=None
        try:
            result=ticksList.pop(0)
            flag=1
        except:
            time.sleep(0.5)
        if flag==1:
            # print(result.get('TouchLine')).
            # print(result)
            result=json.loads(result)
            if result.get('MessageCode')==1501:
                key=f'{result.get("ExchangeInstrumentID")}{EXCHANGESEGMENT.get(result.get("ExchangeSegment"))}'
                ltp=result.get('Touchline',result).get('LastTradedPrice')
                try:
                    tokenDict[key]['ltp']=ltp
                except:
                    tokenDict[key]={}
                    tokenDict[key]['ltp']=ltp
                tokenDict[key]['depthB']=result.get('Touchline',result).get('BidInfo',{}).get('Price')
                tokenDict[key]['depthS']=result.get('Touchline',result).get('AskInfo',{}).get('Price')
            elif result.get('MessageCode')==1504:
                key=f'{result.get("IndexName")}{EXCHANGESEGMENT.get(result.get("ExchangeSegment"))}'
                ltp=result.get('IndexValue')
                try:
                    tokenDict[key]['ltp']=ltp
                except:
                    tokenDict[key]={}
                    tokenDict[key]['ltp']=ltp
                tokenDict[key]['depthB']=ltp
                tokenDict[key]['depthS']=ltp
            elif result.get('MessageCode')==1502:
                key=f'{result.get("ExchangeInstrumentID")}{EXCHANGESEGMENT.get(result.get("ExchangeSegment"))}'
                ltp=result.get('Touchline',result).get('LastTradedPrice')
                try:
                    tokenDict[key]['ltp']=ltp
                except:
                    tokenDict[key]={}
                    tokenDict[key]['ltp']=ltp
                tokenDict[key]['Bids']=result.get('Bids')
                tokenDict[key]['Asks']=result.get('Asks')
                # tokenDict[key]['depthS']=result.get('Touchline',result).get('AskInfo',{}).get('Price')
            event.set() #Triggers the event to send the data to client
            
            # print(tokenDict)

threading.Thread(target=liveData).start()
threading.Thread(target=tickComputation).start()


# Let's Start Server Programming

def subscribe(kws,instrumentDual,mode=1501):
    key=f'{instrumentDual.get("exchangeInstrumentID")}{instrumentDual.get("exchangeSegment")}'
    if tokenDict.get(key)!=None:
        return 0
    token=list()
    print(instrumentDual)
    try:
        mode=instrumentDual.pop('mode')
    except:
        pass
    token.append(instrumentDual)    
    z=kws.send_subscription(token, mode)
    # print(f'{z} subscription return value')
    # key=f'{instrumentDual.get("exchangeInstrumentID")}{instrumentDual.get("exchangeSegment")}'
    # while True:
    #     try:
    #         if type(tokenDict.get(key).get('ltp')) in [float,int]:
    #             print("Successfully instrument token subscribed.")
    #             return 1
    #     except Exception as e:
    #         print("Waiting to get live data...")
    #         time.sleep(0.4)
               


HOST='127.0.0.1'
PORT=7010
RECV_PORT=7011
ADDR=(HOST,PORT)
ADDR2=(HOST,RECV_PORT)
server=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
server.bind(ADDR)

def subscriptionReceiver():
    with socket.socket(socket.AF_INET,socket.SOCK_STREAM) as receiver:
        receiver.bind(ADDR2)
        receiver.listen()
        print(f'[STARTING RECEIVER] {ADDR2}')
        while True:
            if isTickerConnected:
                try:
                    print(f'[RESTARTED] Receiver Port Restarted...')
                    conn,addr=receiver.accept()
                    if not isTickerConnected:
                        # radheUtils.advanceSend(conn,'0')
                        continue
                    print(f'[CONNECTED] Receiver port connected to {addr}')
                    data=radheUtils.advanceReceive(conn,HEADER)
                    try:
                        instrument=json.loads(data)
                    except:
                        print(f'[CONNECTION CLOSED] {addr} Only Json object is permitted')
                        # radheUtils.advanceSend(conn,'0')
                        # conn.close()
                        continue
                    result=subscribe(liveTicker,instrument)
                    # radheUtils.advanceSend(conn,'1')
                    # conn.close()
                except Exception as e:
                    print(f"Error in subscriptionReceiver {e}")
                finally:
                    conn.close()
            
            
            else:
                print('[WAITING] Receiver Port Waiting to reconnect to live Data')
                event.clear()
                event.wait(timeout=10)
                
    


def handle_client(conn,addr):
    global event,tokenDict
    print(f'[NEW CONNECTION] {addr} is connected.')
    while True:
        try:
            tokenDict['isTickerConnected']=isTickerConnected
            msg=json.dumps(tokenDict)
        except Exception as e:
            print(e)
            print(f'[CONNECTION CLOSED] {addr} closed due to json error')
            return 0
        # print(msg)
        if msg:
            radheUtils.advanceSend(conn,msg)
        event.clear()
        event.wait(timeout=30)
        
    print(f'[CONNECTION CLOSED] {addr}')

def start():
    server.listen()
    print(f'[LISTENING] {datetime.datetime.now()} Server is listening on {HOST}:{PORT}')
    while True:
        conn,addr=server.accept()
        threading.Thread(target=handle_client,args=(conn,addr)).start()
        print(f'[ACTIVE CONNECTIONS] {datetime.datetime.now()} = {threading.activeCount()}')
threading.Thread(target=subscriptionReceiver).start()
print(f'[STARTING] {datetime.datetime.now()} Server is Starting...')        
start()
