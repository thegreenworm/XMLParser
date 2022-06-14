



from xml.dom import minidom
# from numpy import dtype, fromstring, string_
import pandas as p
import xml.etree.cElementTree as et
import re
# from pandas.core.frame import DataFrame
from io import StringIO
# import pymssql as ms
import sqlalchemy as sql
import pyodbc
import sqlite3
# import sqlalchemy_turbodbc as st
# import xml.dom.minidom


root = et.parse('customers.xml')
newroot = root.getroot()

dfcols = ['CustomerId','Name','Email','Age']
rows = []
ns = '{http://schemas.datacontract.org/2004/07/DataGenerator}'


for actor in newroot.findall(ns+'Customer'):
    name = actor.find(ns+'Name').text
    for ci in actor.findall(ns+'CustomerId'):
        customerId = ci.text
        for e in actor.findall(ns+'Email'):
            email = e.text
            for a in actor.findall(ns+'Age'):
                age = a.text
                rows.append({'CustomerId': customerId, 'Name': name,
             'Email': email, 'Age': age})

out_df = p.DataFrame(rows, columns= dfcols)


ordersDFcols = ['OrderId','CustomerId','Total']
ordersrows = []

for cus in newroot.findall(ns+'Customer'):
    for order in cus[4].findall(ns+'Order'):
        orCustomerId = order.find(ns+'CustomerId').text
        for oi in order.findall(ns+'OrderId'):
            orderId = oi.text
            for t in order.findall(ns+'Total'):
                total = t.text
                ordersrows.append({'OrderId':orderId, 'CustomerId':orCustomerId,
                'Total': total})

out_orders = p.DataFrame(ordersrows, columns=ordersDFcols)
# print(out_orders)


orderLinesDFcols = ['OrderLineId','OrderId','Qty','Price','LineTotal','ProductId']
orderLinesRows = []

for cus in newroot.findall(ns+'Customer'):
    for lorder in cus[4].findall(ns+'Order'):
        lorderId = lorder.find(ns+'OrderId').text
        for line in lorder[1].findall(ns+'OrderLine'):
            orderLineId = line.find(ns+'OrderLineId').text
            for q in line.findall(ns+'Qty'):
                quantity = q.text
                for pri in line.findall(ns+'Price'):
                    price = pri.text
                    for lt in line.findall(ns+'Total'):
                        lineTotal = lt.text
                        for product in line.findall(ns+'ProductId'):
                            productId = product.text
                            orderLinesRows.append({'OrderLineId':orderLineId, 'OrderId':lorderId, 'Qty': quantity,
                                                    'Price': price, 'LineTotal': lineTotal, 'ProductId': productId})

out_lines = p.DataFrame(orderLinesRows, columns=orderLinesDFcols)
# print(out_lines)



params = 'Driver={SQL Server Native Client 11.0};'+'Server=DESKTOP-9KU3Q6C;'+'Database=Lab 2;'+ 'Trusted_Connection=yes;'
conn = pyodbc.connect(params)
c = conn.cursor()



c.fast_executemany = True
for row_count in range(0,out_df.shape[0]):
    chunk = out_df.iloc[row_count:row_count + 1,:].values.tolist()
    tuple_of_tuples = tuple(tuple(x) for x in chunk)
    c.executemany("insert into Customers" + " ([CustomerId], [Name],[Email],[Age]) values (?,?,?,?)",tuple_of_tuples)
c.commit()

c.fast_executemany = True
for row_count in range(0,out_orders.shape[0]):
    chunk = out_orders.iloc[row_count:row_count + 1,:].values.tolist()
    tuple_of_tuples = tuple(tuple(x) for x in chunk)
    c.executemany("insert into Orders" + " ([OrderId], [CustomerId],[Total]) values (?,?,?)",tuple_of_tuples)
c.commit()

c.fast_executemany = True
for row_count in range(0,out_lines.shape[0]):
    chunk = out_lines.iloc[row_count:row_count + 1,:].values.tolist()
    tuple_of_tuples = tuple(tuple(x) for x in chunk)
    c.executemany("insert into OrderLines" + " ([OrderLineId], [OrderId],[Qty],[Price],[LineTotal],[ProductId]) values (?,?,?,?,?,?)",tuple_of_tuples)
c.commit()


