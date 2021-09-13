import time
def placeOrder(item,second=0):
    time.sleep(second)
    print('Placing an order')
    if type(item)!=dict:
        return {'status':0,'msg':'Invalid data structure for place order sent'}
    tradingsymbol=item.get('tradingSymbol')
    exchange=item.get('exchange')
    quantity=item.get('quantity')
    transaction_type=item.get('transaction_type')
    order_type=item.get('order_type')
    product=item.get('product')
    variety=item.get('variety')
    validity=item.get('validity')
    price=item.get('price')
    triggerPrice=item.get('triggerPrice')
    # if item.get('transaction_type')=='BUY':
    #     price=item.get('buyAbove')
    #     triggerPrice=item.get('buyAbove')+0.1
    # elif item.get('transaction_type')=='SELL':
    #     price=item.get('sellBelow')
    #     triggerPrice=item.get('sellBelow')+0.1
    kite=item.get('kite')
    tag=item.get('tag')
    # print(price)
    # print(triggerPrice)
    count=1
    result={}
    while count<4:
        try:
            quantity=int(quantity)
            z= kite.place_order(tradingsymbol=tradingsymbol,exchange=exchange,quantity=quantity,transaction_type=transaction_type,order_type=order_type,product=product,variety=variety,validity=validity,price=price,trigger_price=triggerPrice,tag=tag)
            print('success')
            print(z)
            result={'status':1,'orderId':str(z)}
            break
        except Exception as e:
            result={'status':0,'msg':f"{e} - Error While Placing Order"}
            count+=1
            time.sleep(1)
    return result

