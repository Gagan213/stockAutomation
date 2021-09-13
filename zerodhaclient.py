import socket,json,datetime,time,threading
import radheUtils

SERVER='127.0.0.1'
PORT=10000
ADDR=(SERVER,PORT)
HEADER=10

client=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
print(client.connect(ADDR))

def send():
    d={}
    d['code']=1
    d['instrumentToken']=58046727 #58158599 #58158087
    d['mode']='LTP'
    
    
    
    item={}
    item['code']=3
    item['tradingsymbol']='CRUDEOIL21JUNFUT'
    item['exchange']='MCX'
    item['quantity']='1'
    item['transaction_type']='BUY'
    item['order_type']='LIMIT'
    item['product']='NRML'
    item['variety']='regular'
    item['validity']='DAY'
    item['price']='4900'
    item['tag']='Tarun Placed Order'
    
    # item['code']=3
    # item['tradingsymbol']='RELIANCE'
    # item['exchange']='NSE'
    # item['quantity']=1
    # item['transaction_type']='BUY'
    # item['order_type']='LIMIT'
    # item['product']='CNC'
    # item['variety']='amo'
    # item['validity']='DAY'
    # item['price']='2100'
    # item['tag']='Tarun Placed Order'
    
    
    # d='tarun'
    # print(type(json.dumps(d)))
    print(d)
    for _ in range(0,10):
        radheUtils.advanceSend(client,json.dumps(item),10)
        time.sleep(1)
def receive():
    while True:
        data=radheUtils.advanceReceive(client)
        if data==b'':
            break
        else:
            print(data)
threading.Thread(target=send).start()
threading.Thread(target=receive).start()


