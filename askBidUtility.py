import socket,threading,time,json
import radheUtils, xlwings,pathlib

timeInterval=3

EXCHANGESEGMENT={'NSECM': 1, 'NSEFO': 2, 'NSECD': 3, 'NSECO': 4, 'SLBM': 5, 'BSECM': 11, 'BSEFO': 12, 'BSECD': 13, 'BSECO': 14, 'NCDEX': 21, 'MSECM': 41, 'MSEFO': 42, 'MSECD': 43, 'MCXFO': 51, 1: 'NSECM', 2: 'NSEFO', 3: 'NSECD', 4: 'NSECO', 5: 'SLBM', 11: 'BSECM', 12: 'BSEFO', 13: 'BSECD', 14: 'BSECO', 21: 'NCDEX', 41: 'MSECM', 42: 'MSEFO', 43: 'MSECD', 51: 'MCXFO'}
location=pathlib.Path(__file__).parent
print(location)
excelFileName='ASK BID PRICE QTY.xlsm'
excelFileLocation=location.joinpath(excelFileName)
userExcelFile=location.joinpath('Users.xlsx')

rowNoQueue=[]
tokenDict={}
outputQueue=[]

isTickerConnected=0
startExcelPointer=9
HOST='127.0.0.1'
PORT=7010
PORT2=7011
subscriptionList=[]
wb=xlwings.Book(excelFileLocation)
ws=wb.sheets['Depth']

HEADER=16
excelRef={
    'tradingSymbol':'N',
    'ltp':'M',
    'bid1':'L',
    'bid2':'K',
    'bid3':'J',
    'bq1':'I', #bidqty =bq
    'bq2':'H',
    'bq3':'G',
    'ask1':'O',
    'ask2':'P',
    'ask3':'Q',
    'aq1':'R',
    'aq2':'S',
    'aq3':'T',
    'timeInterval':'K4',
    
}
print("Starting...")
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
    # if subscribe
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
matchingTable={}
def update():
    
    while True:
        pointer=startExcelPointer
        if ws.range(f'{excelRef.get("tradingSymbol")}{pointer}').value==1:
            while True:
                pointer+=1
                data=ws.range(f'{excelRef.get("tradingSymbol")}{pointer}').value
                if data==None:
                    break
                try:
                    dual=matchingTable[data]
                except:
                    dual=radheUtils.search(data)
                if dual==0:
                    continue
                elif type(dual)==dict:
                    matchingTable[data]=dual
                    key=f'{dual.get("exchangeInstrumentID")}{dual.get("exchangeSegment")}'
                    try:
                        temp=tokenDict[key] 
                    except:
                        dual['mode']=1502
                        subscriptionList.append(dual)
                        subscribe(dual)
                        temp=tokenDict[key]
                        print(tokenDict)
                        
                    ws.range(f'{excelRef.get("ltp")}{pointer}').value=temp.get('ltp')
                    ws.range(f'{excelRef.get("bid1")}{pointer}').value=temp.get('Bids')[0].get('Price')
                    ws.range(f'{excelRef.get("bid2")}{pointer}').value=temp.get('Bids')[1].get('Price')
                    ws.range(f'{excelRef.get("bid3")}{pointer}').value=temp.get('Bids')[2].get('Price')
                    ws.range(f'{excelRef.get("bq1")}{pointer}').value=temp.get('Bids')[0].get('Size')
                    ws.range(f'{excelRef.get("bq2")}{pointer}').value=temp.get('Bids')[1].get('Size')
                    ws.range(f'{excelRef.get("bq3")}{pointer}').value=temp.get('Bids')[1].get('Size')
                    
                    ws.range(f'{excelRef.get("ask1")}{pointer}').value=temp.get('Asks')[0].get('Price')
                    ws.range(f'{excelRef.get("ask2")}{pointer}').value=temp.get('Asks')[1].get('Price')
                    ws.range(f'{excelRef.get("ask3")}{pointer}').value=temp.get('Asks')[2].get('Price')
                    ws.range(f'{excelRef.get("aq1")}{pointer}').value=temp.get('Asks')[0].get('Size')
                    ws.range(f'{excelRef.get("aq2")}{pointer}').value=temp.get('Asks')[1].get('Size')
                    ws.range(f'{excelRef.get("aq3")}{pointer}').value=temp.get('Asks')[2].get('Size')
                
            # data=ws.range(f'{excelRef.get('tradingSymbol')}{pointer}').value
        time.sleep(timeInterval) #Interval


tempInterval=ws.range(excelRef.get('timeInterval')).value
                      
if type(tempInterval) in [int,float] :
    print(f"time Interval Updated {tempInterval}")
    timeInterval=tempInterval


    

update()
    
    