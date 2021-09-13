import csv,datetime,pathlib,time,socket
def upp(string):
    if type(string)==str:
        return string.upper()
    return string

def low(string):
    if type(string)==str:
        return string.lower()
    return string

def conditionStopper(condition,trueFunc,sleep=0.3):
    while True:
        if condition():
            trueFunc()
            break
        else:
            # print("in false condition")
            time.sleep(sleep)

def getTimeFromString(timeString):
    # Create Time From String.
    timeUnits=timeString.split(':')
    invalidDataFlag=0
    for i in range(0,len(timeUnits)):
        try:
            timeUnits[i]=int(timeUnits[i])
        except:
            invalidDataFlag=1
    seconds=0   #Time may be 14:20  or 14:20:17 
    if len(timeUnits)==3:
        seconds=timeUnits[2]
    now=datetime.datetime.now()
    try:
        executeAt=now.replace(hour=timeUnits[0],minute=timeUnits[1],second=seconds)
    except:
        invalidDataFlag=1
    if invalidDataFlag==0:
        waitTime=(executeAt-now).total_seconds()
        return waitTime
    else:
        print("Invalid time")
        return None
        
def roundSpecial(number,factor):
    number=round(number)
    remainder=number%factor
    if remainder > factor/2:
        number=number+(factor-remainder)
    else:
        number=number-remainder
    return number
            
          
def advanceReceive(conn,HEADER=16):
    len=conn.recv(HEADER)
    len=len.decode()
    try:
        len=int(len)
    except:
        print("Invalid Header (Not Int)")
        return None
    return conn.recv(len).decode()
    
def advanceSend(conn,stringMsg,HEADER=16):
    msg=stringMsg.encode('utf-8')
    lengthMsg=str(len(msg)).encode('utf-8')
    lengthMsg+= b' ' * (HEADER-len(lengthMsg))
    conn.sendall(lengthMsg)
    conn.sendall(msg)


def search(tradingSymbol,file='IIFLInstruments'): 
    tradingSymbol=upp(tradingSymbol)
    if tradingSymbol=='NIFTY BANK':
        return {'exchangeSegment':'NSECM','exchangeInstrumentID':'NIFTY BANK','mode':1504}
    elif tradingSymbol=='NIFTY 50':
        return {'exchangeSegment':'NSECM','exchangeInstrumentID':'NIFTY 50', 'mode':1504}
    with open(file,'r') as f:
        header=['ExchangeSegment','ExchangeInstrumentID','InstrumentType','Name','Description','Series','NameWithSeries','InstrumentID']
        read=csv.DictReader(f,fieldnames=header,delimiter="|")
        for row in read:
            if row.get('Description')==tradingSymbol:
            # if row.get('Description').find(tradingSymbol)!=-1:
                return {'exchangeSegment':row.get('ExchangeSegment'),'exchangeInstrumentID':row.get('ExchangeInstrumentID')}
        return 0
        
        # print(z)
if __name__ == "__main__":    
    z=search('Reliance-EQ')
    print(z)