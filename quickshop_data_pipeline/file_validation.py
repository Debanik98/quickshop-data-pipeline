import pandas as pd
import datetime as dt


def perform_validation(row):
    reason=[]
    print(list(row))
    if row['quantity'] * row['price'] != row['sales']:
        reason.append('Sale Amount do not match')
    
    if row['price'] is None:
        reason.append('Product not present')
    
    if row['order_date'] > dt.date.today():
        reason.append('Order date in future')
    
    if (None in list(row) ):
        reason.append('field is null')
    
    if row['city'].strip().lower() not in ['mumbai','bangalore']:
        reason.append('city not correct')

    return ','.join(reason) if len(reason) > 0 else None

