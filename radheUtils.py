import time,datetime

def advanceReceive(conn,HEADER=10):
    try:
        data=conn.recv(HEADER)
        if data==b'':
            return b''
        len=data.decode()
        try:
            len=int(len)
        except:
            print(len)
            print("Invalid Header (Not Int)")
            return None
        return conn.recv(len).decode()
    except ConnectionResetError:
        return b''
        
    
def advanceSend(conn,stringMsg,HEADER=10):
    msg=stringMsg.encode('utf-8')
    lengthMsg=str(len(msg)).encode('utf-8')
    lengthMsg+= b' ' * (HEADER-len(lengthMsg))
    # print(f'Length {lengthMsg}')
    # print(stringMsg)
    if conn.sendall(lengthMsg)==None:
        conn.sendall(msg)



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


def search(instruments,tradingSymbol,exchange='NFO'):
    result=0
    # #Testing
    # for i in instruments:
    #     if i.get('exchange')=='MCX':
    #         print(i)
    # #Testing CLosed
    tradingSymbol=tradingSymbol.upper() if type(tradingSymbol)==str else tradingSymbol
    if tradingSymbol=='NIFTY':
        result='256265'
    elif tradingSymbol=='BANKNIFTY':
        result='260105' #Bank NIFTY Token 
    for i in instruments:
        # print(i)
        if i.get('tradingsymbol')==tradingSymbol and i.get('exchange')==exchange:
            result=str(i.get('instrument_token'))
            
            break
    return result




def upp(string):
    if type(string)==str:
        return string.upper()
    return string

def low(string):
    if type(string)==str:
        return string.lower()
    return string


def conditionStopper(condition,trueFunc,sleep=0.5):
    while True:
        if condition():
            trueFunc()
            break
        else:
            # print("in false condition")
            time.sleep(sleep)
