#!/usr/bin/env python
# coding: utf-8

# In[1]:


#!/usr/bin/env python
# coding: utf-8

# In[404]:


import os
import sys
import pandas as pd
import numpy as np
#import plotly.express as px
import psycopg2
from sqlalchemy import create_engine
import pymssql
#import plotly.graph_objs as go
from pandasql import sqldf
pysqldf = lambda q: sqldf(q, globals())
import datetime
import json

import smtplib
from os.path import basename
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate

from pandas import HDFStore, DataFrame, read_csv, concat, options
import warnings
import tables
options.mode.chained_assignment = None
warnings.filterwarnings("ignore", category=tables.NaturalNameWarning)

print('imports completed..')


# In[405]:



conMB = pymssql.connect(host='medibuddy-prod.ccvogfvw9jim.ap-south-1.rds.amazonaws.com',user='deepak.nagar',password='sbNwu7NN4A',database='medimarket')
print('connected to conMB')


# In[2]:


enddate = datetime.date.today()
startdate = datetime.date.today() - datetime.timedelta(days=7)
print(startdate)
print(enddate)

#yesterday = (enddate - datetime.timedelta(days=1)).strftime('%d-%b-%Y')


# In[3]:


a="""SELECT  sum(OOP_GMV) as OOP_GMV

FROM (SELECT *,
       CASE
           WHEN (PMEntityId != '1018900'
                 OR PMEntityId IS NULL)
                AND (PaymentReceivedByPG > 0
                     OR Cash>0)
                AND (CorporateName IN ('Tata Consultancy Services Ltd',
                                       'Intel Technology India Pvt Ltd',
                                       'Tata Motors Limited',
                                       'Synopsys India Pvt Ltd-1',
                                       'Medi Buddy',
                                       'J P Morgan Services India Pvt Ltd',
                                       'WM Global Technology Services Pvt Ltd',
                                       'Ovhtech R&D (India) Private Ltd',
                                       'Synamedia Technologies(INDIA) Pvt Ltd',
                                       'J P Morgan Chase Bank',
                                       'Synopsys India Pvt Ltd-2',
                                       'Synopsys India Pvt Ltd',
                                       'Goproducts Engineering India LLP',
                                       'Synopsys (India) Pvt Ltd',
                                       'Tata Consulting Engineers Limited',
                                       'Indian Institute Of Management Bangalore',
                                       'Katerra India Private Limited',
                                       'J P Morgan India Pvt Limited',
                                       'Rakuten India Enterprise Pvt Ltd',
                                       'One.In Digitech Media Private Limited',
                                       'DocsApp',
                                       'Medi Assist',
                                       'Greynium Information Technologies Pvt Ltd',
                                       'Medibuddy',
                                       'NTT DATA',
                                       'Infoblox Technical Support & SWDVPT Pvt Ltd',
                                       'Bharti Airtel Limited & Its Subsidiaries/ Associate',
                                       'Quess Corp Ltd',
                                       'Verse Innovation Private Limited',
                                       'WM Global Technology Services Pvt Ltd-1',
                                       'Tata Chemicals Ltd-1',
                                       'Lowe%s Services India Pvt Ltd',
                                       'Hyperverge',
                                       'Intuit',
                                       'WM GLOBAL Sourcing India Private Limited')
                     OR MAWallet >0
                     OR CorpWallet >0) THEN 'OOP with Wallet'
           WHEN (PMEntityId != '1018900'
                 OR PMEntityId IS NULL)
                AND (PaymentReceivedByPG > 0
                     OR Cash>0) THEN 'OOP with NoWallet'
           WHEN PMEntityId = '1018900' THEN 'Pure Retail'
           ELSE 'Corporate'
       END AS Flag,
       PaymentReceivedByPG + Cash + MBWallet AS OOP_Ordervalue,
       MAWallet + CorpWallet AS Corp_Ordervalue,
       TotalAmount*(PaymentReceivedByPG + Cash + MBWallet)/NetPaidAmount AS OOP_GMV,
                                                           TotalAmount*(MAWallet + CorpWallet)/NetPaidAmount AS Corp_GMV
FROM
  (SELECT DISTINCT RequestId,
                   JSON_VALUE(CONVERT(varchar(MAX), Properties), '$.additionals.PMEntityId') AS PMEntityId,
                   CorporateName,
                   AppointmentStatusId,
                   AppointmentStatus,
                   CASE
                       WHEN rf.RF_Id IS NOT NULL THEN convert(date, rf.RF_CreatedOn)
                   END AS DeliveryTime0
   FROM MediMarket.rpt.MediMarketRequest (nolock)
   LEFT JOIN
     (SELECT RF_R_ID,
             RF_Id ,
             RF_IsActive,
             RF_StatusId,
             RF_CreatedOn
      FROM MediMarket..tblRequestFlow(nolock)
      WHERE RF_CreatedOn >='{startDate}'
        AND RF_CreatedOn < '{endDate}') rf ON RequestId = rf.RF_R_ID
   AND RF_IsActive = 1
   AND RF_StatusId IN (86,
                       87,
                       57)
   WHERE rf.RF_CreatedOn >='{startDate}'
     AND rf.RF_CreatedOn < '{endDate}'
     AND ContractTypeId IN (10307,
                            10390)
     AND (CorporateName != 'MA Board'
          OR CorporateName IS NULL)
     AND (EmployeeID != 'Non_Board_DA'
          OR EmployeeID IS NULL)) mmr
LEFT JOIN
  (SELECT PI_R_Id,
          sum(PI_quantity*PI_unitPrice) AS TotalAmount,
          sum(PI_discountAmount) AS TotalDiscount,
          sum(PI_quantity*PI_unitPrice) - sum(PI_discountAmount) AS DiscountedAmount,
          sum(PI_cashLessComponent + PI_maComponent + PI_cashComponent) AS NetPaidAmount,
          sum(CASE WHEN WalletName LIKE 'MAWallet' THEN PI_cashLessComponent ELSE 0 END) AS MAWallet,
          sum(CASE WHEN WalletName LIKE 'CorpWallet' THEN PI_cashLessComponent ELSE 0 END) AS CorpWallet,
          sum(PI_maComponent) AS MBWallet,
          sum(CASE WHEN WalletName LIKE 'PaymentReceivedByPG' THEN PI_cashLessComponent ELSE 0 END) AS PaymentReceivedByPG,
          sum(PI_cashComponent) AS Cash
   FROM
     (SELECT PI_id,
             PI_R_Id,
             PI_quantity,
             PI_unitPrice,
             PI_discountAmount,
             PI_cashLessComponent,
             PI_maComponent,
             PI_cashComponent,
             JSON_VALUE(CONVERT(varchar(MAX), PI_walletBlob), '$[0].WalletName') AS WalletName
      FROM MediMarket.Provider.PharmacyItem (nolock)) x
      
   GROUP BY PI_R_Id) pi ON mmr.RequestId = pi.PI_R_Id) D WHERE Flag in ( 'OOP with Wallet','OOP with NoWallet')""".format(startDate = startdate, endDate = enddate)

AA = pd.read_sql(a, conMB)

AA


# In[4]:


a="""select 'Total_GMV' AS tag, sum(OOP_GMV+Corp_GMV) as Count_, convert(date,DeliveryTime0) as d1 from (SELECT *,
       CASE
           WHEN (PMEntityId != '1018900'
                 OR PMEntityId IS NULL)
                AND (PaymentReceivedByPG > 0
                     OR Cash>0)
                AND (CorporateName IN ('Tata Consultancy Services Ltd',
                                       'Intel Technology India Pvt Ltd',
                                       'Tata Motors Limited',
                                       'Synopsys India Pvt Ltd-1',
                                       'Medi Buddy',
                                       'J P Morgan Services India Pvt Ltd',
                                       'WM Global Technology Services Pvt Ltd',
                                       'Ovhtech R&D (India) Private Ltd',
                                       'Synamedia Technologies(INDIA) Pvt Ltd',
                                       'J P Morgan Chase Bank',
                                       'Synopsys India Pvt Ltd-2',
                                       'Synopsys India Pvt Ltd',
                                       'Goproducts Engineering India LLP',
                                       'Synopsys (India) Pvt Ltd',
                                       'Tata Consulting Engineers Limited',
                                       'Indian Institute Of Management Bangalore',
                                       'Katerra India Private Limited',
                                       'J P Morgan India Pvt Limited',
                                       'Rakuten India Enterprise Pvt Ltd',
                                       'One.In Digitech Media Private Limited',
                                       'DocsApp',
                                       'Medi Assist',
                                       'Greynium Information Technologies Pvt Ltd',
                                       'Medibuddy',
                                       'NTT DATA',
                                       'Infoblox Technical Support & SWDVPT Pvt Ltd',
                                       'Bharti Airtel Limited & Its Subsidiaries/ Associate',
                                       'Quess Corp Ltd',
                                       'Verse Innovation Private Limited',
                                       'WM Global Technology Services Pvt Ltd-1',
                                       'Tata Chemicals Ltd-1',
                                       'Lowe%s Services India Pvt Ltd',
                                       'Hyperverge',
                                       'Intuit',
                                       'WM GLOBAL Sourcing India Private Limited')
                     OR MAWallet >0
                     OR CorpWallet >0) THEN 'OOP with Wallet'
           WHEN (PMEntityId != '1018900'
                 OR PMEntityId IS NULL)
                AND (PaymentReceivedByPG > 0
                     OR Cash>0) THEN 'OOP with NoWallet'
           WHEN PMEntityId = '1018900' THEN 'Pure Retail'
           ELSE 'Corporate'
       END AS Flag,
       PaymentReceivedByPG + Cash + MBWallet AS OOP_Ordervalue,
       MAWallet + CorpWallet AS Corp_Ordervalue,
       TotalAmount*(PaymentReceivedByPG + Cash + MBWallet)/NetPaidAmount AS OOP_GMV,
                                                           TotalAmount*(MAWallet + CorpWallet)/NetPaidAmount AS Corp_GMV
FROM
  (SELECT DISTINCT RequestId,
                   JSON_VALUE(CONVERT(varchar(MAX), Properties), '$.additionals.PMEntityId') AS PMEntityId,
                   CorporateName,
                   AppointmentStatusId,
                   AppointmentStatus,
                   CASE
                       WHEN rf.RF_Id IS NOT NULL THEN convert(date, rf.RF_CreatedOn)
                   END AS DeliveryTime0
   FROM MediMarket.rpt.MediMarketRequest (nolock)
   LEFT JOIN
     (SELECT RF_R_ID,
             RF_Id ,
             RF_IsActive,
             RF_StatusId,
             RF_CreatedOn
      FROM MediMarket..tblRequestFlow(nolock)
      WHERE RF_CreatedOn >='{startDate}'
        AND RF_CreatedOn < '{endDate}') rf ON RequestId = rf.RF_R_ID
   AND RF_IsActive = 1
   AND RF_StatusId IN (86,
                       87,
                       57)
   WHERE rf.RF_CreatedOn >='{startDate}'
     AND rf.RF_CreatedOn < '{endDate}'
     AND ContractTypeId IN (10307,
                            10390)
     AND (CorporateName != 'MA Board'
          OR CorporateName IS NULL)
     AND (EmployeeID != 'Non_Board_DA'
          OR EmployeeID IS NULL)) mmr
LEFT JOIN
  (SELECT PI_R_Id,
          sum(PI_quantity*PI_unitPrice) AS TotalAmount,
          sum(PI_discountAmount) AS TotalDiscount,
          sum(PI_quantity*PI_unitPrice) - sum(PI_discountAmount) AS DiscountedAmount,
          sum(PI_cashLessComponent + PI_maComponent + PI_cashComponent) AS NetPaidAmount,
          sum(CASE WHEN WalletName LIKE 'MAWallet' THEN PI_cashLessComponent ELSE 0 END) AS MAWallet,
          sum(CASE WHEN WalletName LIKE 'CorpWallet' THEN PI_cashLessComponent ELSE 0 END) AS CorpWallet,
          sum(PI_maComponent) AS MBWallet,
          sum(CASE WHEN WalletName LIKE 'PaymentReceivedByPG' THEN PI_cashLessComponent ELSE 0 END) AS PaymentReceivedByPG,
          sum(PI_cashComponent) AS Cash
   FROM
     (SELECT PI_id,
             PI_R_Id,
             PI_quantity,
             PI_unitPrice,
             PI_discountAmount,
             PI_cashLessComponent,
             PI_maComponent,
             PI_cashComponent,
             JSON_VALUE(CONVERT(varchar(MAX), PI_walletBlob), '$[0].WalletName') AS WalletName
      FROM MediMarket.Provider.PharmacyItem (nolock)) x
   GROUP BY PI_R_Id) pi ON mmr.RequestId = pi.PI_R_Id) D group by convert(date,DeliveryTime0) order by 3""".format(startDate = startdate, endDate = enddate)

AA = pd.read_sql(a, conMB)
AA['Count_'] = AA['Count_'].apply(lambda i :  int(i))
AA


# In[5]:


b="""select 'Total_AOV' AS tag, sum(OOP_GMV+Corp_GMV)/count(distinct RequestID) as Count_, convert(date,DeliveryTime0) as d1 from (SELECT *,
       CASE
           WHEN (PMEntityId != '1018900'
                 OR PMEntityId IS NULL)
                AND (PaymentReceivedByPG > 0
                     OR Cash>0)
                AND (CorporateName IN ('Tata Consultancy Services Ltd',
                                       'Intel Technology India Pvt Ltd',
                                       'Tata Motors Limited',
                                       'Synopsys India Pvt Ltd-1',
                                       'Medi Buddy',
                                       'J P Morgan Services India Pvt Ltd',
                                       'WM Global Technology Services Pvt Ltd',
                                       'Ovhtech R&D (India) Private Ltd',
                                       'Synamedia Technologies(INDIA) Pvt Ltd',
                                       'J P Morgan Chase Bank',
                                       'Synopsys India Pvt Ltd-2',
                                       'Synopsys India Pvt Ltd',
                                       'Goproducts Engineering India LLP',
                                       'Synopsys (India) Pvt Ltd',
                                       'Tata Consulting Engineers Limited',
                                       'Indian Institute Of Management Bangalore',
                                       'Katerra India Private Limited',
                                       'J P Morgan India Pvt Limited',
                                       'Rakuten India Enterprise Pvt Ltd',
                                       'One.In Digitech Media Private Limited',
                                       'DocsApp',
                                       'Medi Assist',
                                       'Greynium Information Technologies Pvt Ltd',
                                       'Medibuddy',
                                       'NTT DATA',
                                       'Infoblox Technical Support & SWDVPT Pvt Ltd',
                                       'Bharti Airtel Limited & Its Subsidiaries/ Associate',
                                       'Quess Corp Ltd',
                                       'Verse Innovation Private Limited',
                                       'WM Global Technology Services Pvt Ltd-1',
                                       'Tata Chemicals Ltd-1',
                                       'Lowe%s Services India Pvt Ltd',
                                       'Hyperverge',
                                       'Intuit',
                                       'WM GLOBAL Sourcing India Private Limited')
                     OR MAWallet >0
                     OR CorpWallet >0) THEN 'OOP with Wallet'
           WHEN (PMEntityId != '1018900'
                 OR PMEntityId IS NULL)
                AND (PaymentReceivedByPG > 0
                     OR Cash>0) THEN 'OOP with NoWallet'
           WHEN PMEntityId = '1018900' THEN 'Pure Retail'
           ELSE 'Corporate'
       END AS Flag,
       PaymentReceivedByPG + Cash + MBWallet AS OOP_Ordervalue,
       MAWallet + CorpWallet AS Corp_Ordervalue,
       TotalAmount*(PaymentReceivedByPG + Cash + MBWallet)/NetPaidAmount AS OOP_GMV,
                                                           TotalAmount*(MAWallet + CorpWallet)/NetPaidAmount AS Corp_GMV
FROM
  (SELECT DISTINCT RequestId,
                   JSON_VALUE(CONVERT(varchar(MAX), Properties), '$.additionals.PMEntityId') AS PMEntityId,
                   CorporateName,
                   AppointmentStatusId,
                   AppointmentStatus,
                   CASE
                       WHEN rf.RF_Id IS NOT NULL THEN convert(date, rf.RF_CreatedOn)
                   END AS DeliveryTime0
   FROM MediMarket.rpt.MediMarketRequest (nolock)
   LEFT JOIN
     (SELECT RF_R_ID,
             RF_Id ,
             RF_IsActive,
             RF_StatusId,
             RF_CreatedOn
      FROM MediMarket..tblRequestFlow(nolock)
      WHERE RF_CreatedOn >='{startDate}'
        AND RF_CreatedOn < '{endDate}') rf ON RequestId = rf.RF_R_ID
   AND RF_IsActive = 1
   AND RF_StatusId IN (86,
                       87,
                       57)
   WHERE rf.RF_CreatedOn >='{startDate}'
     AND rf.RF_CreatedOn < '{endDate}'
     AND ContractTypeId IN (10307,
                            10390)
     AND (CorporateName != 'MA Board'
          OR CorporateName IS NULL)
     AND (EmployeeID != 'Non_Board_DA'
          OR EmployeeID IS NULL)) mmr
LEFT JOIN
  (SELECT PI_R_Id,
          sum(PI_quantity*PI_unitPrice) AS TotalAmount,
          sum(PI_discountAmount) AS TotalDiscount,
          sum(PI_quantity*PI_unitPrice) - sum(PI_discountAmount) AS DiscountedAmount,
          sum(PI_cashLessComponent + PI_maComponent + PI_cashComponent) AS NetPaidAmount,
          sum(CASE WHEN WalletName LIKE 'MAWallet' THEN PI_cashLessComponent ELSE 0 END) AS MAWallet,
          sum(CASE WHEN WalletName LIKE 'CorpWallet' THEN PI_cashLessComponent ELSE 0 END) AS CorpWallet,
          sum(PI_maComponent) AS MBWallet,
          sum(CASE WHEN WalletName LIKE 'PaymentReceivedByPG' THEN PI_cashLessComponent ELSE 0 END) AS PaymentReceivedByPG,
          sum(PI_cashComponent) AS Cash
   FROM
     (SELECT PI_id,
             PI_R_Id,
             PI_quantity,
             PI_unitPrice,
             PI_discountAmount,
             PI_cashLessComponent,
             PI_maComponent,
             PI_cashComponent,
             JSON_VALUE(CONVERT(varchar(MAX), PI_walletBlob), '$[0].WalletName') AS WalletName
      FROM MediMarket.Provider.PharmacyItem (nolock)) x
   GROUP BY PI_R_Id) pi ON mmr.RequestId = pi.PI_R_Id) D group by convert(date,DeliveryTime0) order by 3""".format(startDate = startdate, endDate = enddate)

BB = pd.read_sql(b, conMB)
BB['Count_'] = BB['Count_'].apply(lambda i :  int(i))
BB


# In[6]:


c="""select 'Total_Orders' AS tag, convert(date, ApptCreatedDate) AS d1,
       count(distinct RequestID) AS Count_
FROM rpt.MediMarketRequest (nolock)
INNER JOIN MediMarket.dbo.tblrequest tr WITH (nolock) ON tr.R_Id=RequestID
AND isactive=1
LEFT JOIN MediMarket.dbo.tblworkflowstatusmaster sm WITH (nolock) ON appointmentstatusid=sm.sm_id
LEFT JOIN Medimarket.dbo.tblorder(nolock) ord ON ord.o_id=OrderId
LEFT JOIN MediAuth2..tblapplicationuser tau WITH(nolock) ON tau.tau_id=R_createdby
LEFT JOIN ProviderMaster.pmr.ENTITY et WITH (nolock) ON et.E_id= TAU_ProviderMasterEntityId
LEFT JOIN
  (SELECT O_id,
          MBWallet,
          CorpWallet,
          CouponCode,
          PaymentGateway,
          MAWallet,
          HomeSampleCollectionCharges
   FROM
     (SELECT Amount,
             PaymentSource,
             O_id
      FROM Medimarket.marketplace.orderfinancial (nolock)) d pivot (max(Amount)
                                                                    FOR PaymentSource IN (MBWallet, CorpWallet, CouponCode, PaymentGateway, MAWallet,HomeSampleCollectionCharges)) piv)AS tt ON tt.O_ID=OrderId
LEFT JOIN medimarket..tblOrderItem oit ON oit.OI_Id = orderitemid
LEFT JOIN MediMarket.Provider.PharmaDetail(NOLOCK) ON MA_RequestID = R_ID
LEFT JOIN MediMarket..tblRequestFlow(nolock) rf ON R_Id = rf.RF_R_ID
AND RF_IsActive = 1

WHERE ContractTypeId IN (10307,
                         10390)
  AND ApptCreatedDate>='{startDate}'
  AND ApptCreatedDate < '{endDate}'   
AND R_Remarks not in ('Doctor consultation successful, order will be processed again', 'Reason : Placed a new order;Placed a new order', 'Due to technical issue order got cancelled and new order raised with correct address.', 'We are unable to proceed your order due to the technical and hence we have placed the new order.' , 'Duplicate Order' , 'Duplicate Entry')
GROUP BY convert(date, ApptCreatedDate)
ORDER BY 2 """.format(startDate = startdate, endDate = enddate)

CC = pd.read_sql(c, conMB)
CC['Count_'] = CC['Count_'].apply(lambda i :  int(i))
CC


# In[36]:


d= """select 'Total_Deliveries' AS tag, count(distinct RequestID) as Count_, convert(date,DeliveryTime0) as d1 from (SELECT *,
       CASE
           WHEN (PMEntityId != '1018900'
                 OR PMEntityId IS NULL)
                AND (PaymentReceivedByPG > 0
                     OR Cash>0)
                AND (CorporateName IN ('Tata Consultancy Services Ltd',
                                       'Intel Technology India Pvt Ltd',
                                       'Tata Motors Limited',
                                       'Synopsys India Pvt Ltd-1',
                                       'Medi Buddy',
                                       'J P Morgan Services India Pvt Ltd',
                                       'WM Global Technology Services Pvt Ltd',
                                       'Ovhtech R&D (India) Private Ltd',
                                       'Synamedia Technologies(INDIA) Pvt Ltd',
                                       'J P Morgan Chase Bank',
                                       'Synopsys India Pvt Ltd-2',
                                       'Synopsys India Pvt Ltd',
                                       'Goproducts Engineering India LLP',
                                       'Synopsys (India) Pvt Ltd',
                                       'Tata Consulting Engineers Limited',
                                       'Indian Institute Of Management Bangalore',
                                       'Katerra India Private Limited',
                                       'J P Morgan India Pvt Limited',
                                       'Rakuten India Enterprise Pvt Ltd',
                                       'One.In Digitech Media Private Limited',
                                       'DocsApp',
                                       'Medi Assist',
                                       'Greynium Information Technologies Pvt Ltd',
                                       'Medibuddy',
                                       'NTT DATA',
                                       'Infoblox Technical Support & SWDVPT Pvt Ltd',
                                       'Bharti Airtel Limited & Its Subsidiaries/ Associate',
                                       'Quess Corp Ltd',
                                       'Verse Innovation Private Limited',
                                       'WM Global Technology Services Pvt Ltd-1',
                                       'Tata Chemicals Ltd-1',
                                       'Lowe%s Services India Pvt Ltd',
                                       'Hyperverge',
                                       'Intuit',
                                       'WM GLOBAL Sourcing India Private Limited')
                     OR MAWallet >0
                     OR CorpWallet >0) THEN 'OOP with Wallet'
           WHEN (PMEntityId != '1018900'
                 OR PMEntityId IS NULL)
                AND (PaymentReceivedByPG > 0
                     OR Cash>0) THEN 'OOP with NoWallet'
           WHEN PMEntityId = '1018900' THEN 'Pure Retail'
           ELSE 'Corporate'
       END AS Flag,
       PaymentReceivedByPG + Cash + MBWallet AS OOP_Ordervalue,
       MAWallet + CorpWallet AS Corp_Ordervalue,
       TotalAmount*(PaymentReceivedByPG + Cash + MBWallet)/NetPaidAmount AS OOP_GMV,
                                                           TotalAmount*(MAWallet + CorpWallet)/NetPaidAmount AS Corp_GMV
FROM
  (SELECT DISTINCT RequestId,
                   JSON_VALUE(CONVERT(varchar(MAX), Properties), '$.additionals.PMEntityId') AS PMEntityId,
                   CorporateName,
                   AppointmentStatusId,
                   AppointmentStatus,
                   CASE
                       WHEN rf.RF_Id IS NOT NULL THEN convert(date, rf.RF_CreatedOn)
                   END AS DeliveryTime0
   FROM MediMarket.rpt.MediMarketRequest (nolock)
   LEFT JOIN
     (SELECT RF_R_ID,
             RF_Id ,
             RF_IsActive,
             RF_StatusId,
             RF_CreatedOn
      FROM MediMarket..tblRequestFlow(nolock)
      WHERE RF_CreatedOn >='{startDate}'
        AND RF_CreatedOn < '{endDate}') rf ON RequestId = rf.RF_R_ID
   AND RF_IsActive = 1
   AND RF_StatusId IN (86,
                       87,
                       57)
   WHERE rf.RF_CreatedOn >='{startDate}'
     AND rf.RF_CreatedOn < '{endDate}'
     AND ContractTypeId IN (10307,
                            10390)
     AND (CorporateName != 'MA Board'
          OR CorporateName IS NULL)
     AND (EmployeeID != 'Non_Board_DA'
          OR EmployeeID IS NULL)) mmr
LEFT JOIN
  (SELECT PI_R_Id,
          sum(PI_quantity*PI_unitPrice) AS TotalAmount,
          sum(PI_discountAmount) AS TotalDiscount,
          sum(PI_quantity*PI_unitPrice) - sum(PI_discountAmount) AS DiscountedAmount,
          sum(PI_cashLessComponent + PI_maComponent + PI_cashComponent) AS NetPaidAmount,
          sum(CASE WHEN WalletName LIKE 'MAWallet' THEN PI_cashLessComponent ELSE 0 END) AS MAWallet,
          sum(CASE WHEN WalletName LIKE 'CorpWallet' THEN PI_cashLessComponent ELSE 0 END) AS CorpWallet,
          sum(PI_maComponent) AS MBWallet,
          sum(CASE WHEN WalletName LIKE 'PaymentReceivedByPG' THEN PI_cashLessComponent ELSE 0 END) AS PaymentReceivedByPG,
          sum(PI_cashComponent) AS Cash
   FROM
     (SELECT PI_id,
             PI_R_Id,
             PI_quantity,
             PI_unitPrice,
             PI_discountAmount,
             PI_cashLessComponent,
             PI_maComponent,
             PI_cashComponent,
             JSON_VALUE(CONVERT(varchar(MAX), PI_walletBlob), '$[0].WalletName') AS WalletName
      FROM MediMarket.Provider.PharmacyItem (nolock)) x
   GROUP BY PI_R_Id) pi ON mmr.RequestId = pi.PI_R_Id) D group by convert(date,DeliveryTime0) order by 3
""".format(startDate = startdate, endDate = enddate)

DD = pd.read_sql(d, conMB)
DD


# In[37]:


yy= DD.append(CC,ignore_index=True)
yy= yy.append(BB,ignore_index=True)
yy= yy.append(AA,ignore_index=True)


# In[38]:


yy['Count_'] = yy['Count_'].apply(lambda i :  int(i))
def pivot(d):
      pivotData = d.pivot(index='tag', columns='d1', values='Count_')
      pivotData.reset_index(inplace=True)
      return pivotData


# In[39]:


pivot(yy)


# In[ ]:





# In[40]:


Q = """select   count(RequestID) as total_orders,sum(GMV) as Total_GMV  FROM  (
SELECT -- LastUpdateDate,
DISTINCT RequestID, -- ContractTypeName
-- , sm.SM_StatusDesc as "Appt Status"
PaymentSource,
         CASE
                                          WHEN lower(ContractTypeName)='mp_pharmacy'
                                               AND JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.isCashless')='true' THEN 'CashlessOrder'
                                          WHEN lower(ContractTypeName)!='mp_pharmacy'
                                               AND (MAWallet>0
                                                    OR JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.isCashless')='true') THEN 'CashlessOrder'
                                          ELSE 'CashOrder'
                                      END AS isCashless ,
                                    
                                      CASE
                                          WHEN AppointmentStatus = 'Attended' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Callback Required' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Cancelled by Medical Center' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Cancelled by Ops' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Cancelled by User' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Confirmed by Medical Center' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Confirmed by Ops' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Confirmed by Retailer' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Customer Awaited' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Delivered Partial' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Delivery attempt failed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivery Executive Assigned' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Info submitted' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Invoice Generated' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Invoice Pending' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'MACancel' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'New Appointment Request' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'New Cashless Request' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Not Attended' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'On Hold' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Out for Delivery' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Placed with Partner' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Postponed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rejected by Moderator' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Rejected by Retailer' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Rescheduled by Ops' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rescheduled by User' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Settled' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'UserCancel' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'System Confirmed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rescheduled by Medical Center' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Cancelled' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Confirmed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Processed' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Ready To Deliver' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Received' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Medicine Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Medicine Delivery in Progess' THEN 'InProcess'
                                      END AS "Final_Status" 
         

                          ,EmployeeID,
                         convert(date, ApptCreatedDate) AS ApptCreatedDate -- , AppointmentApprovalDate
-- , DATEDIFF(day, AppointmentApprovalDate, ApptCreatedDate) as ApptApprovalTAT
-- , ApptDate
-- , DATEDIFF(day, ApptDate, AppointmentApprovalDate) as ApptTAT_2

,

-- CASE
--     WHEN CorporateName IS NOT NULL
--          AND CorporateName != '0' THEN CorporateName
--     ELSE E_FullName
-- END AS CorporateName_Rpt ,
-- E_FullName AS CorporateName ,
-- MAID ,
ContactNo,
PrimaryID,
MAID,
MAWallet ,
MBWallet ,
CorpWallet ,

CASE
    WHEN ContractTypeName='MP_PHARMACY'
         OR ContractTypeName='Pharmacy_Mediassist' THEN JSON_VALUE(convert(varchar(MAX),Properties),'$.cashlessAmount')
    ELSE '0'
END AS PharmacyCashless ,
CASE
    WHEN ContractTypeName='MP_PHARMACY'
         OR ContractTypeName='Pharmacy_Mediassist' THEN JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount')
    ELSE '0'
END AS PharmacyCash ,
 
(CASE
     WHEN oft.Amount> 0 THEN oft.Amount
     ELSE 0
 END + CASE
          WHEN ContractTypeName='MP_PHARMACY'
                OR ContractTypeName='Pharmacy_Mediassist' THEN cast(JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount') AS decimal)
          ELSE 0
      END) AS OrderValue ,
CASE
    WHEN ProviderERID = 72326 THEN (CASE
                                                 WHEN oft.Amount> 0 THEN oft.Amount
                                                 ELSE 0
                                             END + CASE
                                                      WHEN ContractTypeName='MP_PHARMACY'
                                                            OR ContractTypeName='Pharmacy_Mediassist' THEN cast(JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount') AS decimal)
                                                      ELSE 0
                                                  END) / 0.85
    WHEN ProviderERID = 199600 THEN (CASE
                                              WHEN oft.Amount> 0 THEN oft.Amount
                                              ELSE 0
                                          END + CASE
                                                    WHEN ContractTypeName='MP_PHARMACY'
                                                         OR ContractTypeName='Pharmacy_Mediassist' THEN cast(JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount') AS decimal)
                                                    ELSE 0
                                                END) / 0.90
END AS GMV ,

 -- , JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.isCashless') AS IsCashless

                                      
                                      
                                      AppointmentStatus 
                                      
                                     
FROM rpt.MediMarketRequest (nolock) -- inner  join MediMarket.dbo.tblrequest_Archive tr  with (nolock)
--  on  tr.R_Id=RequestID and isactive=1

INNER JOIN MediMarket.dbo.tblrequest tr WITH (nolock) ON tr.R_Id=RequestID
AND isactive=1
LEFT JOIN providermaster.pmr.vwEntityRelation vwer (nolock) ON CorporateERID = vwer.ID
LEFT JOIN MediMarket.dbo.tblworkflowstatusmaster sm WITH (nolock) ON appointmentstatusid=sm.sm_id
LEFT JOIN Medimarket.dbo.tblorder(nolock) ord ON ord.o_id=OrderId
LEFT JOIN MediAuth2..tblapplicationuser tau WITH(nolock) ON tau.tau_id=R_createdby
LEFT JOIN ProviderMaster.pmr.ENTITY et WITH (nolock) ON et.E_id= TAU_ProviderMasterEntityId
LEFT JOIN
( SELECT O_id,
         MBWallet,
         CorpWallet,
         CouponCode,
         PaymentGateway,
         MAWallet,
         HomeSampleCollectionCharges
 FROM
  ( SELECT Amount,
            PaymentSource,
            O_id
    FROM Medimarket.marketplace.orderfinancial (nolock) ) d pivot ( MAX(Amount)
                                                                  FOR PaymentSource IN (MBWallet, CorpWallet, CouponCode, PaymentGateway, MAWallet,HomeSampleCollectionCharges) ) piv )AS tt ON tt.O_ID=OrderId
-- LEFT JOIN medimarket..tblOrderItem oit ON oit.OI_Id = orderitemid
-- LEFT JOIN MediMarket.Provider.PharmaDetail(NOLOCK) ON MA_RequestID = R_ID
-- LEFT JOIN MediMarket..tblRequestFlow(nolock) rf ON R_Id = rf.RF_R_ID
-- AND RF_IsActive = 1
-- AND RF_StatusId IN (86,
--                     87)
LEFT JOIN
(SELECT O_Id,
        TransactionId,PaymentSource,
        MAX(Amount) AS Amount
 FROM Medimarket.marketplace.orderfinancial (nolock)
 GROUP BY O_Id,PaymentSource,
          TransactionId) oft ON oft.O_ID=OrderId
-- LEFT JOIN
-- (SELECT RW_R_Id,
--         RW_TrackingNumber
--  FROM Medimarket.dbo.tblRequestWallet (nolock)) rw ON rw.RW_R_Id=RequestId
WHERE ContractTypeId IN (-- 10386,10389,10390,10401,10846,10847,10861,10949,12553,
10307,
10390 -- 10389 -- MP_INVESTIGATION
-- 10401 -- MP_CONSULTATION
-- ,13465
)
 AND ApptCreatedDate>='{startDate}'
     AND ApptCreatedDate < '{endDate}'
      AND R_Remarks not in ('Doctor consultation successful, order will be processed again', 'Reason : Placed a new order;Placed a new order', 'Due to technical issue order got cancelled and new order raised with correct address.', 'We are unable to proceed your order due to the technical and hence we have placed the new order.' , 'Duplicate Order' , 'Duplicate Entry')

) AS V 
where EmployeeID IS not NULL
  AND EmployeeID <> 'Non_Board_DA'""".format(startDate = startdate, endDate = enddate)

dftotal = pd.read_sql(Q, conMB)
dftotal['Total_GMV'] = dftotal['Total_GMV'].apply(lambda i :  int(i))
dftotal


# In[41]:


R = """select   count(RequestID) as total_orders,sum(GMV) as Total_GMV  FROM  (
SELECT -- LastUpdateDate,
DISTINCT RequestID, -- ContractTypeName
-- , sm.SM_StatusDesc as "Appt Status"
PaymentSource,
         CASE
                                          WHEN lower(ContractTypeName)='mp_pharmacy'
                                               AND JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.isCashless')='true' THEN 'CashlessOrder'
                                          WHEN lower(ContractTypeName)!='mp_pharmacy'
                                               AND (MAWallet>0
                                                    OR JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.isCashless')='true') THEN 'CashlessOrder'
                                          ELSE 'CashOrder'
                                      END AS isCashless ,
                                    
                                      CASE
                                          WHEN AppointmentStatus = 'Attended' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Callback Required' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Cancelled by Medical Center' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Cancelled by Ops' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Cancelled by User' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Confirmed by Medical Center' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Confirmed by Ops' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Confirmed by Retailer' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Customer Awaited' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Delivered Partial' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Delivery attempt failed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivery Executive Assigned' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Info submitted' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Invoice Generated' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Invoice Pending' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'MACancel' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'New Appointment Request' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'New Cashless Request' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Not Attended' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'On Hold' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Out for Delivery' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Placed with Partner' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Postponed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rejected by Moderator' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Rejected by Retailer' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Rescheduled by Ops' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rescheduled by User' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Settled' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'UserCancel' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'System Confirmed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rescheduled by Medical Center' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Cancelled' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Confirmed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Processed' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Ready To Deliver' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Received' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Medicine Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Medicine Delivery in Progess' THEN 'InProcess'
                                      END AS "Final_Status" 
         

                          ,EmployeeID,
                         convert(date, ApptCreatedDate) AS ApptCreatedDate -- , AppointmentApprovalDate
-- , DATEDIFF(day, AppointmentApprovalDate, ApptCreatedDate) as ApptApprovalTAT
-- , ApptDate
-- , DATEDIFF(day, ApptDate, AppointmentApprovalDate) as ApptTAT_2

,

-- CASE
--     WHEN CorporateName IS NOT NULL
--          AND CorporateName != '0' THEN CorporateName
--     ELSE E_FullName
-- END AS CorporateName_Rpt ,
-- E_FullName AS CorporateName ,
-- MAID ,
ContactNo,
PrimaryID,
MAID,
MAWallet ,
MBWallet ,
CorpWallet ,

CASE
    WHEN ContractTypeName='MP_PHARMACY'
         OR ContractTypeName='Pharmacy_Mediassist' THEN JSON_VALUE(convert(varchar(MAX),Properties),'$.cashlessAmount')
    ELSE '0'
END AS PharmacyCashless ,
CASE
    WHEN ContractTypeName='MP_PHARMACY'
         OR ContractTypeName='Pharmacy_Mediassist' THEN JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount')
    ELSE '0'
END AS PharmacyCash ,
 
(CASE
     WHEN oft.Amount> 0 THEN oft.Amount
     ELSE 0
 END + CASE
          WHEN ContractTypeName='MP_PHARMACY'
                OR ContractTypeName='Pharmacy_Mediassist' THEN cast(JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount') AS decimal)
          ELSE 0
      END) AS OrderValue ,
CASE
    WHEN ProviderERID = 72326 THEN (CASE
                                                 WHEN oft.Amount> 0 THEN oft.Amount
                                                 ELSE 0
                                             END + CASE
                                                      WHEN ContractTypeName='MP_PHARMACY'
                                                            OR ContractTypeName='Pharmacy_Mediassist' THEN cast(JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount') AS decimal)
                                                      ELSE 0
                                                  END) / 0.85
    WHEN ProviderERID = 199600 THEN (CASE
                                              WHEN oft.Amount> 0 THEN oft.Amount
                                              ELSE 0
                                          END + CASE
                                                    WHEN ContractTypeName='MP_PHARMACY'
                                                         OR ContractTypeName='Pharmacy_Mediassist' THEN cast(JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount') AS decimal)
                                                    ELSE 0
                                                END) / 0.90
END AS GMV ,

 -- , JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.isCashless') AS IsCashless

                                      
                                      
                                      AppointmentStatus 
                                      
                                     
FROM rpt.MediMarketRequest (nolock) -- inner  join MediMarket.dbo.tblrequest_Archive tr  with (nolock)
--  on  tr.R_Id=RequestID and isactive=1

INNER JOIN MediMarket.dbo.tblrequest tr WITH (nolock) ON tr.R_Id=RequestID
AND isactive=1
LEFT JOIN providermaster.pmr.vwEntityRelation vwer (nolock) ON CorporateERID = vwer.ID
LEFT JOIN MediMarket.dbo.tblworkflowstatusmaster sm WITH (nolock) ON appointmentstatusid=sm.sm_id
LEFT JOIN Medimarket.dbo.tblorder(nolock) ord ON ord.o_id=OrderId
LEFT JOIN MediAuth2..tblapplicationuser tau WITH(nolock) ON tau.tau_id=R_createdby
LEFT JOIN ProviderMaster.pmr.ENTITY et WITH (nolock) ON et.E_id= TAU_ProviderMasterEntityId
LEFT JOIN
( SELECT O_id,
         MBWallet,
         CorpWallet,
         CouponCode,
         PaymentGateway,
         MAWallet,
         HomeSampleCollectionCharges
 FROM
  ( SELECT Amount,
            PaymentSource,
            O_id
    FROM Medimarket.marketplace.orderfinancial (nolock) ) d pivot ( MAX(Amount)
                                                                  FOR PaymentSource IN (MBWallet, CorpWallet, CouponCode, PaymentGateway, MAWallet,HomeSampleCollectionCharges) ) piv )AS tt ON tt.O_ID=OrderId
-- LEFT JOIN medimarket..tblOrderItem oit ON oit.OI_Id = orderitemid
-- LEFT JOIN MediMarket.Provider.PharmaDetail(NOLOCK) ON MA_RequestID = R_ID
-- LEFT JOIN MediMarket..tblRequestFlow(nolock) rf ON R_Id = rf.RF_R_ID
-- AND RF_IsActive = 1
-- AND RF_StatusId IN (86,
--                     87)
LEFT JOIN
(SELECT O_Id,
        TransactionId,PaymentSource,
        MAX(Amount) AS Amount
 FROM Medimarket.marketplace.orderfinancial (nolock)
 GROUP BY O_Id,PaymentSource,
          TransactionId) oft ON oft.O_ID=OrderId
-- LEFT JOIN
-- (SELECT RW_R_Id,
--         RW_TrackingNumber
--  FROM Medimarket.dbo.tblRequestWallet (nolock)) rw ON rw.RW_R_Id=RequestId
WHERE ContractTypeId IN (-- 10386,10389,10390,10401,10846,10847,10861,10949,12553,
10307,
10390 -- 10389 -- MP_INVESTIGATION
-- 10401 -- MP_CONSULTATION
-- ,13465
)
 AND ApptCreatedDate>='{startDate}'
     AND ApptCreatedDate < '{endDate}'
      AND R_Remarks not in ('Doctor consultation successful, order will be processed again', 'Reason : Placed a new order;Placed a new order', 'Due to technical issue order got cancelled and new order raised with correct address.', 'We are unable to proceed your order due to the technical and hence we have placed the new order.' , 'Duplicate Order' , 'Duplicate Entry')

) AS V 
where EmployeeID IS not NULL
  AND EmployeeID <> 'Non_Board_DA'
  and PaymentSource= 'PaymentGateway'
""".format(startDate = startdate, endDate = enddate)

dftotal_PG = pd.read_sql(R, conMB)
dftotal_PG['Total_GMV'] = dftotal_PG['Total_GMV'].apply(lambda i :  int(i))
dftotal_PG


# In[42]:


S= """select   count(RequestID) as total_orders,sum(GMV) as Total_GMV  FROM  (
SELECT -- LastUpdateDate,
DISTINCT RequestID, -- ContractTypeName
-- , sm.SM_StatusDesc as "Appt Status"
PaymentSource,
         CASE
                                          WHEN lower(ContractTypeName)='mp_pharmacy'
                                               AND JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.isCashless')='true' THEN 'CashlessOrder'
                                          WHEN lower(ContractTypeName)!='mp_pharmacy'
                                               AND (MAWallet>0
                                                    OR JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.isCashless')='true') THEN 'CashlessOrder'
                                          ELSE 'CashOrder'
                                      END AS isCashless ,
                                    
                                      CASE
                                          WHEN AppointmentStatus = 'Attended' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Callback Required' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Cancelled by Medical Center' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Cancelled by Ops' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Cancelled by User' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Confirmed by Medical Center' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Confirmed by Ops' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Confirmed by Retailer' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Customer Awaited' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Delivered Partial' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Delivery attempt failed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivery Executive Assigned' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Info submitted' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Invoice Generated' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Invoice Pending' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'MACancel' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'New Appointment Request' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'New Cashless Request' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Not Attended' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'On Hold' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Out for Delivery' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Placed with Partner' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Postponed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rejected by Moderator' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Rejected by Retailer' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Rescheduled by Ops' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rescheduled by User' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Settled' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'UserCancel' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'System Confirmed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rescheduled by Medical Center' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Cancelled' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Confirmed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Processed' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Ready To Deliver' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Received' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Medicine Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Medicine Delivery in Progess' THEN 'InProcess'
                                      END AS "Final_Status" 
         

                          ,EmployeeID,
                         convert(date, ApptCreatedDate) AS ApptCreatedDate -- , AppointmentApprovalDate
-- , DATEDIFF(day, AppointmentApprovalDate, ApptCreatedDate) as ApptApprovalTAT
-- , ApptDate
-- , DATEDIFF(day, ApptDate, AppointmentApprovalDate) as ApptTAT_2

,

-- CASE
--     WHEN CorporateName IS NOT NULL
--          AND CorporateName != '0' THEN CorporateName
--     ELSE E_FullName
-- END AS CorporateName_Rpt ,
-- E_FullName AS CorporateName ,
-- MAID ,
ContactNo,
PrimaryID,
MAID,
MAWallet ,
MBWallet ,
CorpWallet ,

CASE
    WHEN ContractTypeName='MP_PHARMACY'
         OR ContractTypeName='Pharmacy_Mediassist' THEN JSON_VALUE(convert(varchar(MAX),Properties),'$.cashlessAmount')
    ELSE '0'
END AS PharmacyCashless ,
CASE
    WHEN ContractTypeName='MP_PHARMACY'
         OR ContractTypeName='Pharmacy_Mediassist' THEN JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount')
    ELSE '0'
END AS PharmacyCash ,
 
(CASE
     WHEN oft.Amount> 0 THEN oft.Amount
     ELSE 0
 END + CASE
          WHEN ContractTypeName='MP_PHARMACY'
                OR ContractTypeName='Pharmacy_Mediassist' THEN cast(JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount') AS decimal)
          ELSE 0
      END) AS OrderValue ,
CASE
    WHEN ProviderERID = 72326 THEN (CASE
                                                 WHEN oft.Amount> 0 THEN oft.Amount
                                                 ELSE 0
                                             END + CASE
                                                      WHEN ContractTypeName='MP_PHARMACY'
                                                            OR ContractTypeName='Pharmacy_Mediassist' THEN cast(JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount') AS decimal)
                                                      ELSE 0
                                                  END) / 0.85
    WHEN ProviderERID = 199600 THEN (CASE
                                              WHEN oft.Amount> 0 THEN oft.Amount
                                              ELSE 0
                                          END + CASE
                                                    WHEN ContractTypeName='MP_PHARMACY'
                                                         OR ContractTypeName='Pharmacy_Mediassist' THEN cast(JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount') AS decimal)
                                                    ELSE 0
                                                END) / 0.90
END AS GMV ,

 -- , JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.isCashless') AS IsCashless

                                      
                                      
                                      AppointmentStatus 
                                      
                                     
FROM rpt.MediMarketRequest (nolock) -- inner  join MediMarket.dbo.tblrequest_Archive tr  with (nolock)
--  on  tr.R_Id=RequestID and isactive=1

INNER JOIN MediMarket.dbo.tblrequest tr WITH (nolock) ON tr.R_Id=RequestID
AND isactive=1
LEFT JOIN providermaster.pmr.vwEntityRelation vwer (nolock) ON CorporateERID = vwer.ID
LEFT JOIN MediMarket.dbo.tblworkflowstatusmaster sm WITH (nolock) ON appointmentstatusid=sm.sm_id
LEFT JOIN Medimarket.dbo.tblorder(nolock) ord ON ord.o_id=OrderId
LEFT JOIN MediAuth2..tblapplicationuser tau WITH(nolock) ON tau.tau_id=R_createdby
LEFT JOIN ProviderMaster.pmr.ENTITY et WITH (nolock) ON et.E_id= TAU_ProviderMasterEntityId
LEFT JOIN
( SELECT O_id,
         MBWallet,
         CorpWallet,
         CouponCode,
         PaymentGateway,
         MAWallet,
         HomeSampleCollectionCharges
 FROM
  ( SELECT Amount,
            PaymentSource,
            O_id
    FROM Medimarket.marketplace.orderfinancial (nolock) ) d pivot ( MAX(Amount)
                                                                  FOR PaymentSource IN (MBWallet, CorpWallet, CouponCode, PaymentGateway, MAWallet,HomeSampleCollectionCharges) ) piv )AS tt ON tt.O_ID=OrderId
-- LEFT JOIN medimarket..tblOrderItem oit ON oit.OI_Id = orderitemid
-- LEFT JOIN MediMarket.Provider.PharmaDetail(NOLOCK) ON MA_RequestID = R_ID
-- LEFT JOIN MediMarket..tblRequestFlow(nolock) rf ON R_Id = rf.RF_R_ID
-- AND RF_IsActive = 1
-- AND RF_StatusId IN (86,
--                     87)
LEFT JOIN
(SELECT O_Id,
        TransactionId,PaymentSource,
        MAX(Amount) AS Amount
 FROM Medimarket.marketplace.orderfinancial (nolock)
 GROUP BY O_Id,PaymentSource,
          TransactionId) oft ON oft.O_ID=OrderId
-- LEFT JOIN
-- (SELECT RW_R_Id,
--         RW_TrackingNumber
--  FROM Medimarket.dbo.tblRequestWallet (nolock)) rw ON rw.RW_R_Id=RequestId
WHERE ContractTypeId IN (-- 10386,10389,10390,10401,10846,10847,10861,10949,12553,
10307,
10390 -- 10389 -- MP_INVESTIGATION
-- 10401 -- MP_CONSULTATION
-- ,13465
)
 AND ApptCreatedDate>='{startDate}'
     AND ApptCreatedDate < '{endDate}'
      AND R_Remarks not in ('Doctor consultation successful, order will be processed again', 'Reason : Placed a new order;Placed a new order', 'Due to technical issue order got cancelled and new order raised with correct address.', 'We are unable to proceed your order due to the technical and hence we have placed the new order.' , 'Duplicate Order' , 'Duplicate Entry')

) AS V 
where EmployeeID IS not NULL
  AND EmployeeID <> 'Non_Board_DA'
and IsCashless='CashOrder'""".format(startDate = startdate, endDate = enddate)

dftotal_Cash = pd.read_sql(S, conMB)
dftotal_Cash['Total_GMV'] = dftotal_Cash['Total_GMV'].apply(lambda i :  int(i))
dftotal_Cash


# In[43]:


T= """select   Final_Status,count(RequestID) as total_orders,sum(GMV) as Total_GMV  FROM  (
SELECT -- LastUpdateDate,
DISTINCT RequestID, -- ContractTypeName
-- , sm.SM_StatusDesc as "Appt Status"
PaymentSource,
         CASE
                                          WHEN lower(ContractTypeName)='mp_pharmacy'
                                               AND JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.isCashless')='true' THEN 'CashlessOrder'
                                          WHEN lower(ContractTypeName)!='mp_pharmacy'
                                               AND (MAWallet>0
                                                    OR JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.isCashless')='true') THEN 'CashlessOrder'
                                          ELSE 'CashOrder'
                                      END AS isCashless ,
                                    
                                      CASE
                                          WHEN AppointmentStatus = 'Attended' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Callback Required' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Cancelled by Medical Center' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Cancelled by Ops' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Cancelled by User' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Confirmed by Medical Center' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Confirmed by Ops' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Confirmed by Retailer' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Customer Awaited' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Delivered Partial' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Delivery attempt failed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivery Executive Assigned' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Info submitted' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Invoice Generated' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Invoice Pending' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'MACancel' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'New Appointment Request' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'New Cashless Request' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Not Attended' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'On Hold' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Out for Delivery' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Placed with Partner' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Postponed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rejected by Moderator' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Rejected by Retailer' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Rescheduled by Ops' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rescheduled by User' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Settled' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'UserCancel' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'System Confirmed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rescheduled by Medical Center' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Cancelled' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Confirmed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Processed' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Ready To Deliver' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Received' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Medicine Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Medicine Delivery in Progess' THEN 'InProcess'
                                      END AS "Final_Status" 
         

                          ,EmployeeID,
                         convert(date, ApptCreatedDate) AS ApptCreatedDate -- , AppointmentApprovalDate
-- , DATEDIFF(day, AppointmentApprovalDate, ApptCreatedDate) as ApptApprovalTAT
-- , ApptDate
-- , DATEDIFF(day, ApptDate, AppointmentApprovalDate) as ApptTAT_2

,

-- CASE
--     WHEN CorporateName IS NOT NULL
--          AND CorporateName != '0' THEN CorporateName
--     ELSE E_FullName
-- END AS CorporateName_Rpt ,
-- E_FullName AS CorporateName ,
-- MAID ,
ContactNo,
PrimaryID,
MAID,
MAWallet ,
MBWallet ,
CorpWallet ,

CASE
    WHEN ContractTypeName='MP_PHARMACY'
         OR ContractTypeName='Pharmacy_Mediassist' THEN JSON_VALUE(convert(varchar(MAX),Properties),'$.cashlessAmount')
    ELSE '0'
END AS PharmacyCashless ,
CASE
    WHEN ContractTypeName='MP_PHARMACY'
         OR ContractTypeName='Pharmacy_Mediassist' THEN JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount')
    ELSE '0'
END AS PharmacyCash ,
 
(CASE
     WHEN oft.Amount> 0 THEN oft.Amount
     ELSE 0
 END + CASE
          WHEN ContractTypeName='MP_PHARMACY'
                OR ContractTypeName='Pharmacy_Mediassist' THEN cast(JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount') AS decimal)
          ELSE 0
      END) AS OrderValue ,
CASE
    WHEN ProviderERID = 72326 THEN (CASE
                                                 WHEN oft.Amount> 0 THEN oft.Amount
                                                 ELSE 0
                                             END + CASE
                                                      WHEN ContractTypeName='MP_PHARMACY'
                                                            OR ContractTypeName='Pharmacy_Mediassist' THEN cast(JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount') AS decimal)
                                                      ELSE 0
                                                  END) / 0.85
    WHEN ProviderERID = 199600 THEN (CASE
                                              WHEN oft.Amount> 0 THEN oft.Amount
                                              ELSE 0
                                          END + CASE
                                                    WHEN ContractTypeName='MP_PHARMACY'
                                                         OR ContractTypeName='Pharmacy_Mediassist' THEN cast(JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount') AS decimal)
                                                    ELSE 0
                                                END) / 0.90
END AS GMV ,

 -- , JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.isCashless') AS IsCashless

                                      
                                      
                                      AppointmentStatus 
                                      
                                     
FROM rpt.MediMarketRequest (nolock) -- inner  join MediMarket.dbo.tblrequest_Archive tr  with (nolock)
--  on  tr.R_Id=RequestID and isactive=1

INNER JOIN MediMarket.dbo.tblrequest tr WITH (nolock) ON tr.R_Id=RequestID
AND isactive=1
LEFT JOIN providermaster.pmr.vwEntityRelation vwer (nolock) ON CorporateERID = vwer.ID
LEFT JOIN MediMarket.dbo.tblworkflowstatusmaster sm WITH (nolock) ON appointmentstatusid=sm.sm_id
LEFT JOIN Medimarket.dbo.tblorder(nolock) ord ON ord.o_id=OrderId
LEFT JOIN MediAuth2..tblapplicationuser tau WITH(nolock) ON tau.tau_id=R_createdby
LEFT JOIN ProviderMaster.pmr.ENTITY et WITH (nolock) ON et.E_id= TAU_ProviderMasterEntityId
LEFT JOIN
( SELECT O_id,
         MBWallet,
         CorpWallet,
         CouponCode,
         PaymentGateway,
         MAWallet,
         HomeSampleCollectionCharges
 FROM
  ( SELECT Amount,
            PaymentSource,
            O_id
    FROM Medimarket.marketplace.orderfinancial (nolock) ) d pivot ( MAX(Amount)
                                                                  FOR PaymentSource IN (MBWallet, CorpWallet, CouponCode, PaymentGateway, MAWallet,HomeSampleCollectionCharges) ) piv )AS tt ON tt.O_ID=OrderId
-- LEFT JOIN medimarket..tblOrderItem oit ON oit.OI_Id = orderitemid
-- LEFT JOIN MediMarket.Provider.PharmaDetail(NOLOCK) ON MA_RequestID = R_ID
-- LEFT JOIN MediMarket..tblRequestFlow(nolock) rf ON R_Id = rf.RF_R_ID
-- AND RF_IsActive = 1
-- AND RF_StatusId IN (86,
--                     87)
LEFT JOIN
(SELECT O_Id,
        TransactionId,PaymentSource,
        MAX(Amount) AS Amount
 FROM Medimarket.marketplace.orderfinancial (nolock)
 GROUP BY O_Id,PaymentSource,
          TransactionId) oft ON oft.O_ID=OrderId
-- LEFT JOIN
-- (SELECT RW_R_Id,
--         RW_TrackingNumber
--  FROM Medimarket.dbo.tblRequestWallet (nolock)) rw ON rw.RW_R_Id=RequestId
WHERE ContractTypeId IN (-- 10386,10389,10390,10401,10846,10847,10861,10949,12553,
10307,
10390 -- 10389 -- MP_INVESTIGATION
-- 10401 -- MP_CONSULTATION
-- ,13465
)
 AND ApptCreatedDate>='{startDate}'
     AND ApptCreatedDate < '{endDate}'
      AND R_Remarks not in ('Doctor consultation successful, order will be processed again', 'Reason : Placed a new order;Placed a new order', 'Due to technical issue order got cancelled and new order raised with correct address.', 'We are unable to proceed your order due to the technical and hence we have placed the new order.' , 'Duplicate Order' , 'Duplicate Entry')

) AS V 
where EmployeeID IS not NULL
  AND EmployeeID <> 'Non_Board_DA'
group by Final_Status""".format(startDate = startdate, endDate = enddate)

dftotal_split = pd.read_sql(T, conMB)
dftotal_split['Total_GMV'] = dftotal_split['Total_GMV'].apply(lambda i :  int(i))
dftotal_split


# In[44]:


U= """select   count(RequestID) as total_orders,sum(GMV) as Total_GMV  FROM  (
SELECT -- LastUpdateDate,
DISTINCT RequestID, -- ContractTypeName
-- , sm.SM_StatusDesc as "Appt Status"
PaymentSource,
         CASE
                                          WHEN lower(ContractTypeName)='mp_pharmacy'
                                               AND JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.isCashless')='true' THEN 'CashlessOrder'
                                          WHEN lower(ContractTypeName)!='mp_pharmacy'
                                               AND (MAWallet>0
                                                    OR JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.isCashless')='true') THEN 'CashlessOrder'
                                          ELSE 'CashOrder'
                                      END AS isCashless ,
                                    
                                      CASE
                                          WHEN AppointmentStatus = 'Attended' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Callback Required' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Cancelled by Medical Center' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Cancelled by Ops' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Cancelled by User' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Confirmed by Medical Center' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Confirmed by Ops' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Confirmed by Retailer' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Customer Awaited' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Delivered Partial' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Delivery attempt failed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivery Executive Assigned' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Info submitted' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Invoice Generated' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Invoice Pending' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'MACancel' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'New Appointment Request' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'New Cashless Request' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Not Attended' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'On Hold' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Out for Delivery' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Placed with Partner' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Postponed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rejected by Moderator' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Rejected by Retailer' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Rescheduled by Ops' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rescheduled by User' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Settled' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'UserCancel' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'System Confirmed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rescheduled by Medical Center' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Cancelled' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Confirmed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Processed' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Ready To Deliver' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Received' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Medicine Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Medicine Delivery in Progess' THEN 'InProcess'
                                      END AS "Final_Status" 
         

                          ,EmployeeID,
                         convert(date, ApptCreatedDate) AS ApptCreatedDate -- , AppointmentApprovalDate
-- , DATEDIFF(day, AppointmentApprovalDate, ApptCreatedDate) as ApptApprovalTAT
-- , ApptDate
-- , DATEDIFF(day, ApptDate, AppointmentApprovalDate) as ApptTAT_2

,

-- CASE
--     WHEN CorporateName IS NOT NULL
--          AND CorporateName != '0' THEN CorporateName
--     ELSE E_FullName
-- END AS CorporateName_Rpt ,
-- E_FullName AS CorporateName ,
-- MAID ,
ContactNo,
PrimaryID,
MAID,
MAWallet ,
MBWallet ,
CorpWallet ,

CASE
    WHEN ContractTypeName='MP_PHARMACY'
         OR ContractTypeName='Pharmacy_Mediassist' THEN JSON_VALUE(convert(varchar(MAX),Properties),'$.cashlessAmount')
    ELSE '0'
END AS PharmacyCashless ,
CASE
    WHEN ContractTypeName='MP_PHARMACY'
         OR ContractTypeName='Pharmacy_Mediassist' THEN JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount')
    ELSE '0'
END AS PharmacyCash ,
 
(CASE
     WHEN oft.Amount> 0 THEN oft.Amount
     ELSE 0
 END + CASE
          WHEN ContractTypeName='MP_PHARMACY'
                OR ContractTypeName='Pharmacy_Mediassist' THEN cast(JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount') AS decimal)
          ELSE 0
      END) AS OrderValue ,
CASE
    WHEN ProviderERID = 72326 THEN (CASE
                                                 WHEN oft.Amount> 0 THEN oft.Amount
                                                 ELSE 0
                                             END + CASE
                                                      WHEN ContractTypeName='MP_PHARMACY'
                                                            OR ContractTypeName='Pharmacy_Mediassist' THEN cast(JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount') AS decimal)
                                                      ELSE 0
                                                  END) / 0.85
    WHEN ProviderERID = 199600 THEN (CASE
                                              WHEN oft.Amount> 0 THEN oft.Amount
                                              ELSE 0
                                          END + CASE
                                                    WHEN ContractTypeName='MP_PHARMACY'
                                                         OR ContractTypeName='Pharmacy_Mediassist' THEN cast(JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount') AS decimal)
                                                    ELSE 0
                                                END) / 0.90
END AS GMV ,

 -- , JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.isCashless') AS IsCashless

                                      
                                      
                                      AppointmentStatus 
                                      
                                     
FROM rpt.MediMarketRequest (nolock) -- inner  join MediMarket.dbo.tblrequest_Archive tr  with (nolock)
--  on  tr.R_Id=RequestID and isactive=1

INNER JOIN MediMarket.dbo.tblrequest tr WITH (nolock) ON tr.R_Id=RequestID
AND isactive=1
LEFT JOIN providermaster.pmr.vwEntityRelation vwer (nolock) ON CorporateERID = vwer.ID
LEFT JOIN MediMarket.dbo.tblworkflowstatusmaster sm WITH (nolock) ON appointmentstatusid=sm.sm_id
LEFT JOIN Medimarket.dbo.tblorder(nolock) ord ON ord.o_id=OrderId
LEFT JOIN MediAuth2..tblapplicationuser tau WITH(nolock) ON tau.tau_id=R_createdby
LEFT JOIN ProviderMaster.pmr.ENTITY et WITH (nolock) ON et.E_id= TAU_ProviderMasterEntityId
LEFT JOIN
( SELECT O_id,
         MBWallet,
         CorpWallet,
         CouponCode,
         PaymentGateway,
         MAWallet,
         HomeSampleCollectionCharges
 FROM
  ( SELECT Amount,
            PaymentSource,
            O_id
    FROM Medimarket.marketplace.orderfinancial (nolock) ) d pivot ( MAX(Amount)
                                                                  FOR PaymentSource IN (MBWallet, CorpWallet, CouponCode, PaymentGateway, MAWallet,HomeSampleCollectionCharges) ) piv )AS tt ON tt.O_ID=OrderId
-- LEFT JOIN medimarket..tblOrderItem oit ON oit.OI_Id = orderitemid
-- LEFT JOIN MediMarket.Provider.PharmaDetail(NOLOCK) ON MA_RequestID = R_ID
-- LEFT JOIN MediMarket..tblRequestFlow(nolock) rf ON R_Id = rf.RF_R_ID
-- AND RF_IsActive = 1
-- AND RF_StatusId IN (86,
--                     87)
LEFT JOIN
(SELECT O_Id,
        TransactionId,PaymentSource,
        MAX(Amount) AS Amount
 FROM Medimarket.marketplace.orderfinancial (nolock)
 GROUP BY O_Id,PaymentSource,
          TransactionId) oft ON oft.O_ID=OrderId
-- LEFT JOIN
-- (SELECT RW_R_Id,
--         RW_TrackingNumber
--  FROM Medimarket.dbo.tblRequestWallet (nolock)) rw ON rw.RW_R_Id=RequestId
WHERE ContractTypeId IN (-- 10386,10389,10390,10401,10846,10847,10861,10949,12553,
10307,
10390 -- 10389 -- MP_INVESTIGATION
-- 10401 -- MP_CONSULTATION
-- ,13465
)
 AND ApptCreatedDate>='{startDate}'
     AND ApptCreatedDate < '{endDate}'
      AND R_Remarks not in ('Doctor consultation successful, order will be processed again', 'Reason : Placed a new order;Placed a new order', 'Due to technical issue order got cancelled and new order raised with correct address.', 'We are unable to proceed your order due to the technical and hence we have placed the new order.' , 'Duplicate Order' , 'Duplicate Entry')

) AS V 
where EmployeeID IS not NULL
  AND EmployeeID <> 'Non_Board_DA'
and IsCashless='CashlessOrder'
""".format(startDate = startdate, endDate = enddate)

dftotal_Cashless = pd.read_sql(U, conMB)
dftotal_Cashless['Total_GMV'] = dftotal_Cashless['Total_GMV'].apply(lambda i :  int(i))
dftotal_Cashless


# In[45]:


A = """select   count(RequestID) as total_orders,sum(GMV) as Total_GMV  FROM  (
SELECT -- LastUpdateDate,
DISTINCT RequestID, -- ContractTypeName
-- , sm.SM_StatusDesc as "Appt Status"
PaymentSource,
         CASE
                                          WHEN lower(ContractTypeName)='mp_pharmacy'
                                               AND JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.isCashless')='true' THEN 'CashlessOrder'
                                          WHEN lower(ContractTypeName)!='mp_pharmacy'
                                               AND (MAWallet>0
                                                    OR JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.isCashless')='true') THEN 'CashlessOrder'
                                          ELSE 'CashOrder'
                                      END AS isCashless ,
                                    
                                      CASE
                                          WHEN AppointmentStatus = 'Attended' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Callback Required' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Cancelled by Medical Center' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Cancelled by Ops' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Cancelled by User' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Confirmed by Medical Center' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Confirmed by Ops' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Confirmed by Retailer' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Customer Awaited' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Delivered Partial' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Delivery attempt failed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivery Executive Assigned' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Info submitted' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Invoice Generated' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Invoice Pending' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'MACancel' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'New Appointment Request' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'New Cashless Request' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Not Attended' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'On Hold' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Out for Delivery' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Placed with Partner' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Postponed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rejected by Moderator' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Rejected by Retailer' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Rescheduled by Ops' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rescheduled by User' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Settled' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'UserCancel' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'System Confirmed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rescheduled by Medical Center' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Cancelled' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Confirmed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Processed' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Ready To Deliver' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Received' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Medicine Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Medicine Delivery in Progess' THEN 'InProcess'
                                      END AS "Final_Status" 
         

                          ,EmployeeID,
                         convert(date, ApptCreatedDate) AS ApptCreatedDate -- , AppointmentApprovalDate
-- , DATEDIFF(day, AppointmentApprovalDate, ApptCreatedDate) as ApptApprovalTAT
-- , ApptDate
-- , DATEDIFF(day, ApptDate, AppointmentApprovalDate) as ApptTAT_2

,

-- CASE
--     WHEN CorporateName IS NOT NULL
--          AND CorporateName != '0' THEN CorporateName
--     ELSE E_FullName
-- END AS CorporateName_Rpt ,
-- E_FullName AS CorporateName ,
-- MAID ,
ContactNo,
PrimaryID,
MAID,
MAWallet ,
MBWallet ,
CorpWallet ,

CASE
    WHEN ContractTypeName='MP_PHARMACY'
         OR ContractTypeName='Pharmacy_Mediassist' THEN JSON_VALUE(convert(varchar(MAX),Properties),'$.cashlessAmount')
    ELSE '0'
END AS PharmacyCashless ,
CASE
    WHEN ContractTypeName='MP_PHARMACY'
         OR ContractTypeName='Pharmacy_Mediassist' THEN JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount')
    ELSE '0'
END AS PharmacyCash ,
 
(CASE
     WHEN oft.Amount> 0 THEN oft.Amount
     ELSE 0
 END + CASE
          WHEN ContractTypeName='MP_PHARMACY'
                OR ContractTypeName='Pharmacy_Mediassist' THEN cast(JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount') AS decimal)
          ELSE 0
      END) AS OrderValue ,
CASE
    WHEN ProviderERID = 72326 THEN (CASE
                                                 WHEN oft.Amount> 0 THEN oft.Amount
                                                 ELSE 0
                                             END + CASE
                                                      WHEN ContractTypeName='MP_PHARMACY'
                                                            OR ContractTypeName='Pharmacy_Mediassist' THEN cast(JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount') AS decimal)
                                                      ELSE 0
                                                  END) / 0.85
    WHEN ProviderERID = 199600 THEN (CASE
                                              WHEN oft.Amount> 0 THEN oft.Amount
                                              ELSE 0
                                          END + CASE
                                                    WHEN ContractTypeName='MP_PHARMACY'
                                                         OR ContractTypeName='Pharmacy_Mediassist' THEN cast(JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount') AS decimal)
                                                    ELSE 0
                                                END) / 0.90
END AS GMV ,

 -- , JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.isCashless') AS IsCashless

                                      
                                      
                                      AppointmentStatus 
                                      
                                     
FROM rpt.MediMarketRequest (nolock) -- inner  join MediMarket.dbo.tblrequest_Archive tr  with (nolock)
--  on  tr.R_Id=RequestID and isactive=1

INNER JOIN MediMarket.dbo.tblrequest tr WITH (nolock) ON tr.R_Id=RequestID
AND isactive=1
LEFT JOIN providermaster.pmr.vwEntityRelation vwer (nolock) ON CorporateERID = vwer.ID
LEFT JOIN MediMarket.dbo.tblworkflowstatusmaster sm WITH (nolock) ON appointmentstatusid=sm.sm_id
LEFT JOIN Medimarket.dbo.tblorder(nolock) ord ON ord.o_id=OrderId
LEFT JOIN MediAuth2..tblapplicationuser tau WITH(nolock) ON tau.tau_id=R_createdby
LEFT JOIN ProviderMaster.pmr.ENTITY et WITH (nolock) ON et.E_id= TAU_ProviderMasterEntityId
LEFT JOIN
( SELECT O_id,
         MBWallet,
         CorpWallet,
         CouponCode,
         PaymentGateway,
         MAWallet,
         HomeSampleCollectionCharges
 FROM
  ( SELECT Amount,
            PaymentSource,
            O_id
    FROM Medimarket.marketplace.orderfinancial (nolock) ) d pivot ( MAX(Amount)
                                                                  FOR PaymentSource IN (MBWallet, CorpWallet, CouponCode, PaymentGateway, MAWallet,HomeSampleCollectionCharges) ) piv )AS tt ON tt.O_ID=OrderId
-- LEFT JOIN medimarket..tblOrderItem oit ON oit.OI_Id = orderitemid
-- LEFT JOIN MediMarket.Provider.PharmaDetail(NOLOCK) ON MA_RequestID = R_ID
-- LEFT JOIN MediMarket..tblRequestFlow(nolock) rf ON R_Id = rf.RF_R_ID
-- AND RF_IsActive = 1
-- AND RF_StatusId IN (86,
--                     87)
LEFT JOIN
(SELECT O_Id,
        TransactionId,PaymentSource,
        MAX(Amount) AS Amount
 FROM Medimarket.marketplace.orderfinancial (nolock)
 GROUP BY O_Id,PaymentSource,
          TransactionId) oft ON oft.O_ID=OrderId
-- LEFT JOIN
-- (SELECT RW_R_Id,
--         RW_TrackingNumber
--  FROM Medimarket.dbo.tblRequestWallet (nolock)) rw ON rw.RW_R_Id=RequestId
WHERE ContractTypeId IN (-- 10386,10389,10390,10401,10846,10847,10861,10949,12553,
10307,
10390 -- 10389 -- MP_INVESTIGATION
-- 10401 -- MP_CONSULTATION
-- ,13465
)
 AND ApptCreatedDate>='{startDate}'
     AND ApptCreatedDate < '{endDate}'
      AND R_Remarks not in ('Doctor consultation successful, order will be processed again', 'Reason : Placed a new order;Placed a new order', 'Due to technical issue order got cancelled and new order raised with correct address.', 'We are unable to proceed your order due to the technical and hence we have placed the new order.' , 'Duplicate Order' , 'Duplicate Entry')
AND MAID IS NOT NULL
) AS V 
where EmployeeID IS not NULL
  AND EmployeeID <> 'Non_Board_DA'""".format(startDate = startdate, endDate = enddate)

dfCorp = pd.read_sql(A, conMB)
dfCorp['Total_GMV'] = dfCorp['Total_GMV'].apply(lambda i :  int(i))
dfCorp


# In[46]:


B = """select   count(RequestID) as total_orders,sum(GMV) as Total_GMV  FROM  (
SELECT -- LastUpdateDate,
DISTINCT RequestID, -- ContractTypeName
-- , sm.SM_StatusDesc as "Appt Status"
PaymentSource,
         CASE
                                          WHEN lower(ContractTypeName)='mp_pharmacy'
                                               AND JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.isCashless')='true' THEN 'CashlessOrder'
                                          WHEN lower(ContractTypeName)!='mp_pharmacy'
                                               AND (MAWallet>0
                                                    OR JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.isCashless')='true') THEN 'CashlessOrder'
                                          ELSE 'CashOrder'
                                      END AS isCashless ,
                                    
                                      CASE
                                          WHEN AppointmentStatus = 'Attended' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Callback Required' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Cancelled by Medical Center' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Cancelled by Ops' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Cancelled by User' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Confirmed by Medical Center' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Confirmed by Ops' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Confirmed by Retailer' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Customer Awaited' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Delivered Partial' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Delivery attempt failed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivery Executive Assigned' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Info submitted' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Invoice Generated' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Invoice Pending' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'MACancel' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'New Appointment Request' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'New Cashless Request' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Not Attended' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'On Hold' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Out for Delivery' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Placed with Partner' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Postponed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rejected by Moderator' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Rejected by Retailer' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Rescheduled by Ops' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rescheduled by User' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Settled' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'UserCancel' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'System Confirmed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rescheduled by Medical Center' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Cancelled' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Confirmed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Processed' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Ready To Deliver' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Received' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Medicine Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Medicine Delivery in Progess' THEN 'InProcess'
                                      END AS "Final_Status" 
         

                          ,EmployeeID,
                         convert(date, ApptCreatedDate) AS ApptCreatedDate -- , AppointmentApprovalDate
-- , DATEDIFF(day, AppointmentApprovalDate, ApptCreatedDate) as ApptApprovalTAT
-- , ApptDate
-- , DATEDIFF(day, ApptDate, AppointmentApprovalDate) as ApptTAT_2

,

-- CASE
--     WHEN CorporateName IS NOT NULL
--          AND CorporateName != '0' THEN CorporateName
--     ELSE E_FullName
-- END AS CorporateName_Rpt ,
-- E_FullName AS CorporateName ,
-- MAID ,
ContactNo,
PrimaryID,
MAID,
MAWallet ,
MBWallet ,
CorpWallet ,

CASE
    WHEN ContractTypeName='MP_PHARMACY'
         OR ContractTypeName='Pharmacy_Mediassist' THEN JSON_VALUE(convert(varchar(MAX),Properties),'$.cashlessAmount')
    ELSE '0'
END AS PharmacyCashless ,
CASE
    WHEN ContractTypeName='MP_PHARMACY'
         OR ContractTypeName='Pharmacy_Mediassist' THEN JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount')
    ELSE '0'
END AS PharmacyCash ,
 
(CASE
     WHEN oft.Amount> 0 THEN oft.Amount
     ELSE 0
 END + CASE
          WHEN ContractTypeName='MP_PHARMACY'
                OR ContractTypeName='Pharmacy_Mediassist' THEN cast(JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount') AS decimal)
          ELSE 0
      END) AS OrderValue ,
CASE
    WHEN ProviderERID = 72326 THEN (CASE
                                                 WHEN oft.Amount> 0 THEN oft.Amount
                                                 ELSE 0
                                             END + CASE
                                                      WHEN ContractTypeName='MP_PHARMACY'
                                                            OR ContractTypeName='Pharmacy_Mediassist' THEN cast(JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount') AS decimal)
                                                      ELSE 0
                                                  END) / 0.85
    WHEN ProviderERID = 199600 THEN (CASE
                                              WHEN oft.Amount> 0 THEN oft.Amount
                                              ELSE 0
                                          END + CASE
                                                    WHEN ContractTypeName='MP_PHARMACY'
                                                         OR ContractTypeName='Pharmacy_Mediassist' THEN cast(JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount') AS decimal)
                                                    ELSE 0
                                                END) / 0.90
END AS GMV ,

 -- , JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.isCashless') AS IsCashless

                                      
                                      
                                      AppointmentStatus 
                                      
                                     
FROM rpt.MediMarketRequest (nolock) -- inner  join MediMarket.dbo.tblrequest_Archive tr  with (nolock)
--  on  tr.R_Id=RequestID and isactive=1

INNER JOIN MediMarket.dbo.tblrequest tr WITH (nolock) ON tr.R_Id=RequestID
AND isactive=1
LEFT JOIN providermaster.pmr.vwEntityRelation vwer (nolock) ON CorporateERID = vwer.ID
LEFT JOIN MediMarket.dbo.tblworkflowstatusmaster sm WITH (nolock) ON appointmentstatusid=sm.sm_id
LEFT JOIN Medimarket.dbo.tblorder(nolock) ord ON ord.o_id=OrderId
LEFT JOIN MediAuth2..tblapplicationuser tau WITH(nolock) ON tau.tau_id=R_createdby
LEFT JOIN ProviderMaster.pmr.ENTITY et WITH (nolock) ON et.E_id= TAU_ProviderMasterEntityId
LEFT JOIN
( SELECT O_id,
         MBWallet,
         CorpWallet,
         CouponCode,
         PaymentGateway,
         MAWallet,
         HomeSampleCollectionCharges
 FROM
  ( SELECT Amount,
            PaymentSource,
            O_id
    FROM Medimarket.marketplace.orderfinancial (nolock) ) d pivot ( MAX(Amount)
                                                                  FOR PaymentSource IN (MBWallet, CorpWallet, CouponCode, PaymentGateway, MAWallet,HomeSampleCollectionCharges) ) piv )AS tt ON tt.O_ID=OrderId
-- LEFT JOIN medimarket..tblOrderItem oit ON oit.OI_Id = orderitemid
-- LEFT JOIN MediMarket.Provider.PharmaDetail(NOLOCK) ON MA_RequestID = R_ID
-- LEFT JOIN MediMarket..tblRequestFlow(nolock) rf ON R_Id = rf.RF_R_ID
-- AND RF_IsActive = 1
-- AND RF_StatusId IN (86,
--                     87)
LEFT JOIN
(SELECT O_Id,
        TransactionId,PaymentSource,
        MAX(Amount) AS Amount
 FROM Medimarket.marketplace.orderfinancial (nolock)
 GROUP BY O_Id,PaymentSource,
          TransactionId) oft ON oft.O_ID=OrderId
-- LEFT JOIN
-- (SELECT RW_R_Id,
--         RW_TrackingNumber
--  FROM Medimarket.dbo.tblRequestWallet (nolock)) rw ON rw.RW_R_Id=RequestId
WHERE ContractTypeId IN (-- 10386,10389,10390,10401,10846,10847,10861,10949,12553,
10307,
10390 -- 10389 -- MP_INVESTIGATION
-- 10401 -- MP_CONSULTATION
-- ,13465
)
 AND ApptCreatedDate>='{startDate}'
     AND ApptCreatedDate < '{endDate}'
      AND R_Remarks not in ('Doctor consultation successful, order will be processed again', 'Reason : Placed a new order;Placed a new order', 'Due to technical issue order got cancelled and new order raised with correct address.', 'We are unable to proceed your order due to the technical and hence we have placed the new order.' , 'Duplicate Order' , 'Duplicate Entry')
and maid is not null
) AS V 
where EmployeeID IS not NULL
  AND EmployeeID <> 'Non_Board_DA'
  and PaymentSource= 'PaymentGateway'
""".format(startDate = startdate, endDate = enddate)

dfCorp_PG = pd.read_sql(B, conMB)
dfCorp_PG['Total_GMV'] = dfCorp_PG['Total_GMV'].apply(lambda i :  int(i))
dfCorp_PG


# In[47]:


C= """select   Final_Status,count(RequestID) as total_orders,sum(GMV) as Total_GMV  FROM  (
SELECT -- LastUpdateDate,
DISTINCT RequestID, -- ContractTypeName
-- , sm.SM_StatusDesc as "Appt Status"
PaymentSource,
         CASE
                                          WHEN lower(ContractTypeName)='mp_pharmacy'
                                               AND JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.isCashless')='true' THEN 'CashlessOrder'
                                          WHEN lower(ContractTypeName)!='mp_pharmacy'
                                               AND (MAWallet>0
                                                    OR JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.isCashless')='true') THEN 'CashlessOrder'
                                          ELSE 'CashOrder'
                                      END AS isCashless ,
                                    
                                      CASE
                                          WHEN AppointmentStatus = 'Attended' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Callback Required' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Cancelled by Medical Center' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Cancelled by Ops' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Cancelled by User' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Confirmed by Medical Center' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Confirmed by Ops' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Confirmed by Retailer' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Customer Awaited' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Delivered Partial' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Delivery attempt failed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivery Executive Assigned' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Info submitted' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Invoice Generated' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Invoice Pending' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'MACancel' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'New Appointment Request' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'New Cashless Request' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Not Attended' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'On Hold' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Out for Delivery' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Placed with Partner' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Postponed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rejected by Moderator' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Rejected by Retailer' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Rescheduled by Ops' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rescheduled by User' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Settled' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'UserCancel' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'System Confirmed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rescheduled by Medical Center' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Cancelled' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Confirmed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Processed' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Ready To Deliver' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Received' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Medicine Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Medicine Delivery in Progess' THEN 'InProcess'
                                      END AS "Final_Status" 
         

                          ,EmployeeID,
                         convert(date, ApptCreatedDate) AS ApptCreatedDate -- , AppointmentApprovalDate
-- , DATEDIFF(day, AppointmentApprovalDate, ApptCreatedDate) as ApptApprovalTAT
-- , ApptDate
-- , DATEDIFF(day, ApptDate, AppointmentApprovalDate) as ApptTAT_2

,

-- CASE
--     WHEN CorporateName IS NOT NULL
--          AND CorporateName != '0' THEN CorporateName
--     ELSE E_FullName
-- END AS CorporateName_Rpt ,
-- E_FullName AS CorporateName ,
-- MAID ,
ContactNo,
PrimaryID,
MAID,
MAWallet ,
MBWallet ,
CorpWallet ,

CASE
    WHEN ContractTypeName='MP_PHARMACY'
         OR ContractTypeName='Pharmacy_Mediassist' THEN JSON_VALUE(convert(varchar(MAX),Properties),'$.cashlessAmount')
    ELSE '0'
END AS PharmacyCashless ,
CASE
    WHEN ContractTypeName='MP_PHARMACY'
         OR ContractTypeName='Pharmacy_Mediassist' THEN JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount')
    ELSE '0'
END AS PharmacyCash ,
 
(CASE
     WHEN oft.Amount> 0 THEN oft.Amount
     ELSE 0
 END + CASE
          WHEN ContractTypeName='MP_PHARMACY'
                OR ContractTypeName='Pharmacy_Mediassist' THEN cast(JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount') AS decimal)
          ELSE 0
      END) AS OrderValue ,
CASE
    WHEN ProviderERID = 72326 THEN (CASE
                                                 WHEN oft.Amount> 0 THEN oft.Amount
                                                 ELSE 0
                                             END + CASE
                                                      WHEN ContractTypeName='MP_PHARMACY'
                                                            OR ContractTypeName='Pharmacy_Mediassist' THEN cast(JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount') AS decimal)
                                                      ELSE 0
                                                  END) / 0.85
    WHEN ProviderERID = 199600 THEN (CASE
                                              WHEN oft.Amount> 0 THEN oft.Amount
                                              ELSE 0
                                          END + CASE
                                                    WHEN ContractTypeName='MP_PHARMACY'
                                                         OR ContractTypeName='Pharmacy_Mediassist' THEN cast(JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount') AS decimal)
                                                    ELSE 0
                                                END) / 0.90
END AS GMV ,

 -- , JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.isCashless') AS IsCashless

                                      
                                      
                                      AppointmentStatus 
                                      
                                     
FROM rpt.MediMarketRequest (nolock) -- inner  join MediMarket.dbo.tblrequest_Archive tr  with (nolock)
--  on  tr.R_Id=RequestID and isactive=1

INNER JOIN MediMarket.dbo.tblrequest tr WITH (nolock) ON tr.R_Id=RequestID
AND isactive=1
LEFT JOIN providermaster.pmr.vwEntityRelation vwer (nolock) ON CorporateERID = vwer.ID
LEFT JOIN MediMarket.dbo.tblworkflowstatusmaster sm WITH (nolock) ON appointmentstatusid=sm.sm_id
LEFT JOIN Medimarket.dbo.tblorder(nolock) ord ON ord.o_id=OrderId
LEFT JOIN MediAuth2..tblapplicationuser tau WITH(nolock) ON tau.tau_id=R_createdby
LEFT JOIN ProviderMaster.pmr.ENTITY et WITH (nolock) ON et.E_id= TAU_ProviderMasterEntityId
LEFT JOIN
( SELECT O_id,
         MBWallet,
         CorpWallet,
         CouponCode,
         PaymentGateway,
         MAWallet,
         HomeSampleCollectionCharges
 FROM
  ( SELECT Amount,
            PaymentSource,
            O_id
    FROM Medimarket.marketplace.orderfinancial (nolock) ) d pivot ( MAX(Amount)
                                                                  FOR PaymentSource IN (MBWallet, CorpWallet, CouponCode, PaymentGateway, MAWallet,HomeSampleCollectionCharges) ) piv )AS tt ON tt.O_ID=OrderId
-- LEFT JOIN medimarket..tblOrderItem oit ON oit.OI_Id = orderitemid
-- LEFT JOIN MediMarket.Provider.PharmaDetail(NOLOCK) ON MA_RequestID = R_ID
-- LEFT JOIN MediMarket..tblRequestFlow(nolock) rf ON R_Id = rf.RF_R_ID
-- AND RF_IsActive = 1
-- AND RF_StatusId IN (86,
--                     87)
LEFT JOIN
(SELECT O_Id,
        TransactionId,PaymentSource,
        MAX(Amount) AS Amount
 FROM Medimarket.marketplace.orderfinancial (nolock)
 GROUP BY O_Id,PaymentSource,
          TransactionId) oft ON oft.O_ID=OrderId
-- LEFT JOIN
-- (SELECT RW_R_Id,
--         RW_TrackingNumber
--  FROM Medimarket.dbo.tblRequestWallet (nolock)) rw ON rw.RW_R_Id=RequestId
WHERE ContractTypeId IN (-- 10386,10389,10390,10401,10846,10847,10861,10949,12553,
10307,
10390 -- 10389 -- MP_INVESTIGATION
-- 10401 -- MP_CONSULTATION
-- ,13465
)
 AND ApptCreatedDate>='{startDate}'
     AND ApptCreatedDate < '{endDate}'
      AND R_Remarks not in ('Doctor consultation successful, order will be processed again', 'Reason : Placed a new order;Placed a new order', 'Due to technical issue order got cancelled and new order raised with correct address.', 'We are unable to proceed your order due to the technical and hence we have placed the new order.' , 'Duplicate Order' , 'Duplicate Entry')
and MAID is not null
) AS V 
where EmployeeID IS not NULL
  AND EmployeeID <> 'Non_Board_DA'
group by Final_Status""".format(startDate = startdate, endDate = enddate)

dfCorp_split = pd.read_sql(C, conMB)
dfCorp_split['Total_GMV'] = dfCorp_split['Total_GMV'].apply(lambda i :  int(i))
dfCorp_split


# In[48]:


D= """select   Final_Status,count(RequestID) as total_orders,sum(GMV) as Total_GMV  FROM  (
SELECT -- LastUpdateDate,
DISTINCT RequestID, -- ContractTypeName
-- , sm.SM_StatusDesc as "Appt Status"
PaymentSource,
         CASE
                                          WHEN lower(ContractTypeName)='mp_pharmacy'
                                               AND JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.isCashless')='true' THEN 'CashlessOrder'
                                          WHEN lower(ContractTypeName)!='mp_pharmacy'
                                               AND (MAWallet>0
                                                    OR JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.isCashless')='true') THEN 'CashlessOrder'
                                          ELSE 'CashOrder'
                                      END AS isCashless ,
                                    
                                      CASE
                                          WHEN AppointmentStatus = 'Attended' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Callback Required' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Cancelled by Medical Center' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Cancelled by Ops' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Cancelled by User' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Confirmed by Medical Center' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Confirmed by Ops' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Confirmed by Retailer' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Customer Awaited' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Delivered Partial' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Delivery attempt failed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivery Executive Assigned' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Info submitted' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Invoice Generated' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Invoice Pending' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'MACancel' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'New Appointment Request' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'New Cashless Request' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Not Attended' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'On Hold' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Out for Delivery' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Placed with Partner' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Postponed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rejected by Moderator' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Rejected by Retailer' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Rescheduled by Ops' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rescheduled by User' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Settled' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'UserCancel' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'System Confirmed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rescheduled by Medical Center' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Cancelled' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Confirmed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Processed' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Ready To Deliver' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Received' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Medicine Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Medicine Delivery in Progess' THEN 'InProcess'
                                      END AS "Final_Status" 
         

                          ,EmployeeID,
                         convert(date, ApptCreatedDate) AS ApptCreatedDate -- , AppointmentApprovalDate
-- , DATEDIFF(day, AppointmentApprovalDate, ApptCreatedDate) as ApptApprovalTAT
-- , ApptDate
-- , DATEDIFF(day, ApptDate, AppointmentApprovalDate) as ApptTAT_2

,

-- CASE
--     WHEN CorporateName IS NOT NULL
--          AND CorporateName != '0' THEN CorporateName
--     ELSE E_FullName
-- END AS CorporateName_Rpt ,
-- E_FullName AS CorporateName ,
-- MAID ,
ContactNo,
PrimaryID,
MAID,
MAWallet ,
MBWallet ,
CorpWallet ,

CASE
    WHEN ContractTypeName='MP_PHARMACY'
         OR ContractTypeName='Pharmacy_Mediassist' THEN JSON_VALUE(convert(varchar(MAX),Properties),'$.cashlessAmount')
    ELSE '0'
END AS PharmacyCashless ,
CASE
    WHEN ContractTypeName='MP_PHARMACY'
         OR ContractTypeName='Pharmacy_Mediassist' THEN JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount')
    ELSE '0'
END AS PharmacyCash ,
 
(CASE
     WHEN oft.Amount> 0 THEN oft.Amount
     ELSE 0
 END + CASE
          WHEN ContractTypeName='MP_PHARMACY'
                OR ContractTypeName='Pharmacy_Mediassist' THEN cast(JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount') AS decimal)
          ELSE 0
      END) AS OrderValue ,
CASE
    WHEN ProviderERID = 72326 THEN (CASE
                                                 WHEN oft.Amount> 0 THEN oft.Amount
                                                 ELSE 0
                                             END + CASE
                                                      WHEN ContractTypeName='MP_PHARMACY'
                                                            OR ContractTypeName='Pharmacy_Mediassist' THEN cast(JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount') AS decimal)
                                                      ELSE 0
                                                  END) / 0.85
    WHEN ProviderERID = 199600 THEN (CASE
                                              WHEN oft.Amount> 0 THEN oft.Amount
                                              ELSE 0
                                          END + CASE
                                                    WHEN ContractTypeName='MP_PHARMACY'
                                                         OR ContractTypeName='Pharmacy_Mediassist' THEN cast(JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount') AS decimal)
                                                    ELSE 0
                                                END) / 0.90
END AS GMV ,

 -- , JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.isCashless') AS IsCashless

                                      
                                      
                                      AppointmentStatus 
                                      
                                     
FROM rpt.MediMarketRequest (nolock) -- inner  join MediMarket.dbo.tblrequest_Archive tr  with (nolock)
--  on  tr.R_Id=RequestID and isactive=1

INNER JOIN MediMarket.dbo.tblrequest tr WITH (nolock) ON tr.R_Id=RequestID
AND isactive=1
LEFT JOIN providermaster.pmr.vwEntityRelation vwer (nolock) ON CorporateERID = vwer.ID
LEFT JOIN MediMarket.dbo.tblworkflowstatusmaster sm WITH (nolock) ON appointmentstatusid=sm.sm_id
LEFT JOIN Medimarket.dbo.tblorder(nolock) ord ON ord.o_id=OrderId
LEFT JOIN MediAuth2..tblapplicationuser tau WITH(nolock) ON tau.tau_id=R_createdby
LEFT JOIN ProviderMaster.pmr.ENTITY et WITH (nolock) ON et.E_id= TAU_ProviderMasterEntityId
LEFT JOIN
( SELECT O_id,
         MBWallet,
         CorpWallet,
         CouponCode,
         PaymentGateway,
         MAWallet,
         HomeSampleCollectionCharges
 FROM
  ( SELECT Amount,
            PaymentSource,
            O_id
    FROM Medimarket.marketplace.orderfinancial (nolock) ) d pivot ( MAX(Amount)
                                                                  FOR PaymentSource IN (MBWallet, CorpWallet, CouponCode, PaymentGateway, MAWallet,HomeSampleCollectionCharges) ) piv )AS tt ON tt.O_ID=OrderId
-- LEFT JOIN medimarket..tblOrderItem oit ON oit.OI_Id = orderitemid
-- LEFT JOIN MediMarket.Provider.PharmaDetail(NOLOCK) ON MA_RequestID = R_ID
-- LEFT JOIN MediMarket..tblRequestFlow(nolock) rf ON R_Id = rf.RF_R_ID
-- AND RF_IsActive = 1
-- AND RF_StatusId IN (86,
--                     87)
LEFT JOIN
(SELECT O_Id,
        TransactionId,PaymentSource,
        MAX(Amount) AS Amount
 FROM Medimarket.marketplace.orderfinancial (nolock)
 GROUP BY O_Id,PaymentSource,
          TransactionId) oft ON oft.O_ID=OrderId
-- LEFT JOIN
-- (SELECT RW_R_Id,
--         RW_TrackingNumber
--  FROM Medimarket.dbo.tblRequestWallet (nolock)) rw ON rw.RW_R_Id=RequestId
WHERE ContractTypeId IN (-- 10386,10389,10390,10401,10846,10847,10861,10949,12553,
10307,
10390 -- 10389 -- MP_INVESTIGATION
-- 10401 -- MP_CONSULTATION
-- ,13465
)
 AND ApptCreatedDate>='{startDate}'
     AND ApptCreatedDate < '{endDate}'
      AND R_Remarks not in ('Doctor consultation successful, order will be processed again', 'Reason : Placed a new order;Placed a new order', 'Due to technical issue order got cancelled and new order raised with correct address.', 'We are unable to proceed your order due to the technical and hence we have placed the new order.' , 'Duplicate Order' , 'Duplicate Entry')
and MAID is null
) AS V 
where EmployeeID IS not NULL
  AND EmployeeID <> 'Non_Board_DA'
group by Final_Status""".format(startDate = startdate, endDate = enddate)

dfRetail_split = pd.read_sql(D, conMB)
dfRetail_split['Total_GMV'] = dfRetail_split['Total_GMV'].apply(lambda i :  int(i))
dfRetail_split


# In[49]:


E = """select   count(RequestID) as total_orders,sum(GMV) as Total_GMV  FROM  (
SELECT -- LastUpdateDate,
DISTINCT RequestID, -- ContractTypeName
-- , sm.SM_StatusDesc as "Appt Status"
PaymentSource,
         CASE
                                          WHEN lower(ContractTypeName)='mp_pharmacy'
                                               AND JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.isCashless')='true' THEN 'CashlessOrder'
                                          WHEN lower(ContractTypeName)!='mp_pharmacy'
                                               AND (MAWallet>0
                                                    OR JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.isCashless')='true') THEN 'CashlessOrder'
                                          ELSE 'CashOrder'
                                      END AS isCashless ,
                                    
                                      CASE
                                          WHEN AppointmentStatus = 'Attended' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Callback Required' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Cancelled by Medical Center' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Cancelled by Ops' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Cancelled by User' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Confirmed by Medical Center' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Confirmed by Ops' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Confirmed by Retailer' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Customer Awaited' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Delivered Partial' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Delivery attempt failed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivery Executive Assigned' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Info submitted' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Invoice Generated' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Invoice Pending' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'MACancel' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'New Appointment Request' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'New Cashless Request' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Not Attended' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'On Hold' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Out for Delivery' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Placed with Partner' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Postponed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rejected by Moderator' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Rejected by Retailer' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Rescheduled by Ops' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rescheduled by User' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Settled' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'UserCancel' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'System Confirmed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rescheduled by Medical Center' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Cancelled' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Confirmed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Processed' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Ready To Deliver' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Received' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Medicine Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Medicine Delivery in Progess' THEN 'InProcess'
                                      END AS "Final_Status" 
         

                          ,EmployeeID,
                         convert(date, ApptCreatedDate) AS ApptCreatedDate -- , AppointmentApprovalDate
-- , DATEDIFF(day, AppointmentApprovalDate, ApptCreatedDate) as ApptApprovalTAT
-- , ApptDate
-- , DATEDIFF(day, ApptDate, AppointmentApprovalDate) as ApptTAT_2

,

-- CASE
--     WHEN CorporateName IS NOT NULL
--          AND CorporateName != '0' THEN CorporateName
--     ELSE E_FullName
-- END AS CorporateName_Rpt ,
-- E_FullName AS CorporateName ,
-- MAID ,
ContactNo,
PrimaryID,
MAID,
MAWallet ,
MBWallet ,
CorpWallet ,

CASE
    WHEN ContractTypeName='MP_PHARMACY'
         OR ContractTypeName='Pharmacy_Mediassist' THEN JSON_VALUE(convert(varchar(MAX),Properties),'$.cashlessAmount')
    ELSE '0'
END AS PharmacyCashless ,
CASE
    WHEN ContractTypeName='MP_PHARMACY'
         OR ContractTypeName='Pharmacy_Mediassist' THEN JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount')
    ELSE '0'
END AS PharmacyCash ,
 
(CASE
     WHEN oft.Amount> 0 THEN oft.Amount
     ELSE 0
 END + CASE
          WHEN ContractTypeName='MP_PHARMACY'
                OR ContractTypeName='Pharmacy_Mediassist' THEN cast(JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount') AS decimal)
          ELSE 0
      END) AS OrderValue ,
CASE
    WHEN ProviderERID = 72326 THEN (CASE
                                                 WHEN oft.Amount> 0 THEN oft.Amount
                                                 ELSE 0
                                             END + CASE
                                                      WHEN ContractTypeName='MP_PHARMACY'
                                                            OR ContractTypeName='Pharmacy_Mediassist' THEN cast(JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount') AS decimal)
                                                      ELSE 0
                                                  END) / 0.85
    WHEN ProviderERID = 199600 THEN (CASE
                                              WHEN oft.Amount> 0 THEN oft.Amount
                                              ELSE 0
                                          END + CASE
                                                    WHEN ContractTypeName='MP_PHARMACY'
                                                         OR ContractTypeName='Pharmacy_Mediassist' THEN cast(JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount') AS decimal)
                                                    ELSE 0
                                                END) / 0.90
END AS GMV ,

 -- , JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.isCashless') AS IsCashless

                                      
                                      
                                      AppointmentStatus 
                                      
                                     
FROM rpt.MediMarketRequest (nolock) -- inner  join MediMarket.dbo.tblrequest_Archive tr  with (nolock)
--  on  tr.R_Id=RequestID and isactive=1

INNER JOIN MediMarket.dbo.tblrequest tr WITH (nolock) ON tr.R_Id=RequestID
AND isactive=1
LEFT JOIN providermaster.pmr.vwEntityRelation vwer (nolock) ON CorporateERID = vwer.ID
LEFT JOIN MediMarket.dbo.tblworkflowstatusmaster sm WITH (nolock) ON appointmentstatusid=sm.sm_id
LEFT JOIN Medimarket.dbo.tblorder(nolock) ord ON ord.o_id=OrderId
LEFT JOIN MediAuth2..tblapplicationuser tau WITH(nolock) ON tau.tau_id=R_createdby
LEFT JOIN ProviderMaster.pmr.ENTITY et WITH (nolock) ON et.E_id= TAU_ProviderMasterEntityId
LEFT JOIN
( SELECT O_id,
         MBWallet,
         CorpWallet,
         CouponCode,
         PaymentGateway,
         MAWallet,
         HomeSampleCollectionCharges
 FROM
  ( SELECT Amount,
            PaymentSource,
            O_id
    FROM Medimarket.marketplace.orderfinancial (nolock) ) d pivot ( MAX(Amount)
                                                                  FOR PaymentSource IN (MBWallet, CorpWallet, CouponCode, PaymentGateway, MAWallet,HomeSampleCollectionCharges) ) piv )AS tt ON tt.O_ID=OrderId
-- LEFT JOIN medimarket..tblOrderItem oit ON oit.OI_Id = orderitemid
-- LEFT JOIN MediMarket.Provider.PharmaDetail(NOLOCK) ON MA_RequestID = R_ID
-- LEFT JOIN MediMarket..tblRequestFlow(nolock) rf ON R_Id = rf.RF_R_ID
-- AND RF_IsActive = 1
-- AND RF_StatusId IN (86,
--                     87)
LEFT JOIN
(SELECT O_Id,
        TransactionId,PaymentSource,
        MAX(Amount) AS Amount
 FROM Medimarket.marketplace.orderfinancial (nolock)
 GROUP BY O_Id,PaymentSource,
          TransactionId) oft ON oft.O_ID=OrderId
-- LEFT JOIN
-- (SELECT RW_R_Id,
--         RW_TrackingNumber
--  FROM Medimarket.dbo.tblRequestWallet (nolock)) rw ON rw.RW_R_Id=RequestId
WHERE ContractTypeId IN (-- 10386,10389,10390,10401,10846,10847,10861,10949,12553,
10307,
10390 -- 10389 -- MP_INVESTIGATION
-- 10401 -- MP_CONSULTATION
-- ,13465
)
 AND ApptCreatedDate>='{startDate}'
     AND ApptCreatedDate < '{endDate}'
      AND R_Remarks not in ('Doctor consultation successful, order will be processed again', 'Reason : Placed a new order;Placed a new order', 'Due to technical issue order got cancelled and new order raised with correct address.', 'We are unable to proceed your order due to the technical and hence we have placed the new order.' , 'Duplicate Order' , 'Duplicate Entry')
AND MAID IS NULL
) AS V 
where EmployeeID IS not NULL
  AND EmployeeID <> 'Non_Board_DA'""".format(startDate = startdate, endDate = enddate)

dfRetail_total = pd.read_sql(E, conMB)
dfRetail_total['Total_GMV'] = dfRetail_total['Total_GMV'].apply(lambda i :  int(i))
dfRetail_total


# In[50]:


F= """select   count(RequestID) as total_orders,sum(GMV) as Total_GMV  FROM  (
SELECT -- LastUpdateDate,
DISTINCT RequestID, -- ContractTypeName
-- , sm.SM_StatusDesc as "Appt Status"
PaymentSource,
         CASE
                                          WHEN lower(ContractTypeName)='mp_pharmacy'
                                               AND JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.isCashless')='true' THEN 'CashlessOrder'
                                          WHEN lower(ContractTypeName)!='mp_pharmacy'
                                               AND (MAWallet>0
                                                    OR JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.isCashless')='true') THEN 'CashlessOrder'
                                          ELSE 'CashOrder'
                                      END AS isCashless ,
                                    
                                      CASE
                                          WHEN AppointmentStatus = 'Attended' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Callback Required' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Cancelled by Medical Center' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Cancelled by Ops' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Cancelled by User' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Confirmed by Medical Center' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Confirmed by Ops' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Confirmed by Retailer' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Customer Awaited' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Delivered Partial' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Delivery attempt failed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivery Executive Assigned' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Info submitted' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Invoice Generated' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Invoice Pending' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'MACancel' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'New Appointment Request' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'New Cashless Request' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Not Attended' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'On Hold' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Out for Delivery' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Placed with Partner' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Postponed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rejected by Moderator' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Rejected by Retailer' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Rescheduled by Ops' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rescheduled by User' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Settled' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'UserCancel' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'System Confirmed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rescheduled by Medical Center' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Cancelled' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Confirmed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Processed' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Ready To Deliver' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Received' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Medicine Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Medicine Delivery in Progess' THEN 'InProcess'
                                      END AS "Final_Status" 
         

                          ,EmployeeID,
                         convert(date, ApptCreatedDate) AS ApptCreatedDate -- , AppointmentApprovalDate
-- , DATEDIFF(day, AppointmentApprovalDate, ApptCreatedDate) as ApptApprovalTAT
-- , ApptDate
-- , DATEDIFF(day, ApptDate, AppointmentApprovalDate) as ApptTAT_2

,

-- CASE
--     WHEN CorporateName IS NOT NULL
--          AND CorporateName != '0' THEN CorporateName
--     ELSE E_FullName
-- END AS CorporateName_Rpt ,
-- E_FullName AS CorporateName ,
-- MAID ,
ContactNo,
PrimaryID,
MAID,
MAWallet ,
MBWallet ,
CorpWallet ,

CASE
    WHEN ContractTypeName='MP_PHARMACY'
         OR ContractTypeName='Pharmacy_Mediassist' THEN JSON_VALUE(convert(varchar(MAX),Properties),'$.cashlessAmount')
    ELSE '0'
END AS PharmacyCashless ,
CASE
    WHEN ContractTypeName='MP_PHARMACY'
         OR ContractTypeName='Pharmacy_Mediassist' THEN JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount')
    ELSE '0'
END AS PharmacyCash ,
 
(CASE
     WHEN oft.Amount> 0 THEN oft.Amount
     ELSE 0
 END + CASE
          WHEN ContractTypeName='MP_PHARMACY'
                OR ContractTypeName='Pharmacy_Mediassist' THEN cast(JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount') AS decimal)
          ELSE 0
      END) AS OrderValue ,
CASE
    WHEN ProviderERID = 72326 THEN (CASE
                                                 WHEN oft.Amount> 0 THEN oft.Amount
                                                 ELSE 0
                                             END + CASE
                                                      WHEN ContractTypeName='MP_PHARMACY'
                                                            OR ContractTypeName='Pharmacy_Mediassist' THEN cast(JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount') AS decimal)
                                                      ELSE 0
                                                  END) / 0.85
    WHEN ProviderERID = 199600 THEN (CASE
                                              WHEN oft.Amount> 0 THEN oft.Amount
                                              ELSE 0
                                          END + CASE
                                                    WHEN ContractTypeName='MP_PHARMACY'
                                                         OR ContractTypeName='Pharmacy_Mediassist' THEN cast(JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount') AS decimal)
                                                    ELSE 0
                                                END) / 0.90
END AS GMV ,

 -- , JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.isCashless') AS IsCashless

                                      
                                      
                                      AppointmentStatus 
                                      
                                     
FROM rpt.MediMarketRequest (nolock) -- inner  join MediMarket.dbo.tblrequest_Archive tr  with (nolock)
--  on  tr.R_Id=RequestID and isactive=1

INNER JOIN MediMarket.dbo.tblrequest tr WITH (nolock) ON tr.R_Id=RequestID
AND isactive=1
LEFT JOIN providermaster.pmr.vwEntityRelation vwer (nolock) ON CorporateERID = vwer.ID
LEFT JOIN MediMarket.dbo.tblworkflowstatusmaster sm WITH (nolock) ON appointmentstatusid=sm.sm_id
LEFT JOIN Medimarket.dbo.tblorder(nolock) ord ON ord.o_id=OrderId
LEFT JOIN MediAuth2..tblapplicationuser tau WITH(nolock) ON tau.tau_id=R_createdby
LEFT JOIN ProviderMaster.pmr.ENTITY et WITH (nolock) ON et.E_id= TAU_ProviderMasterEntityId
LEFT JOIN
( SELECT O_id,
         MBWallet,
         CorpWallet,
         CouponCode,
         PaymentGateway,
         MAWallet,
         HomeSampleCollectionCharges
 FROM
  ( SELECT Amount,
            PaymentSource,
            O_id
    FROM Medimarket.marketplace.orderfinancial (nolock) ) d pivot ( MAX(Amount)
                                                                  FOR PaymentSource IN (MBWallet, CorpWallet, CouponCode, PaymentGateway, MAWallet,HomeSampleCollectionCharges) ) piv )AS tt ON tt.O_ID=OrderId
-- LEFT JOIN medimarket..tblOrderItem oit ON oit.OI_Id = orderitemid
-- LEFT JOIN MediMarket.Provider.PharmaDetail(NOLOCK) ON MA_RequestID = R_ID
-- LEFT JOIN MediMarket..tblRequestFlow(nolock) rf ON R_Id = rf.RF_R_ID
-- AND RF_IsActive = 1
-- AND RF_StatusId IN (86,
--                     87)
LEFT JOIN
(SELECT O_Id,
        TransactionId,PaymentSource,
        MAX(Amount) AS Amount
 FROM Medimarket.marketplace.orderfinancial (nolock)
 GROUP BY O_Id,PaymentSource,
          TransactionId) oft ON oft.O_ID=OrderId
-- LEFT JOIN
-- (SELECT RW_R_Id,
--         RW_TrackingNumber
--  FROM Medimarket.dbo.tblRequestWallet (nolock)) rw ON rw.RW_R_Id=RequestId
WHERE ContractTypeId IN (-- 10386,10389,10390,10401,10846,10847,10861,10949,12553,
10307,
10390 -- 10389 -- MP_INVESTIGATION
-- 10401 -- MP_CONSULTATION
-- ,13465
)
 AND ApptCreatedDate>='{startDate}'
     AND ApptCreatedDate < '{endDate}'
      AND R_Remarks not in ('Doctor consultation successful, order will be processed again', 'Reason : Placed a new order;Placed a new order', 'Due to technical issue order got cancelled and new order raised with correct address.', 'We are unable to proceed your order due to the technical and hence we have placed the new order.' , 'Duplicate Order' , 'Duplicate Entry')
AND MAID IS NULL
) AS V 
where EmployeeID IS not NULL
  AND EmployeeID <> 'Non_Board_DA'
and IsCashless='CashOrder'""".format(startDate = startdate, endDate = enddate)

dfRetail_Cash = pd.read_sql(F, conMB)
dfRetail_Cash['Total_GMV'] = dfRetail_Cash['Total_GMV'].apply(lambda i :  int(i))
dfRetail_Cash


# In[51]:


G= """select   count(RequestID) as total_orders,sum(GMV) as Total_GMV  FROM  (
SELECT -- LastUpdateDate,
DISTINCT RequestID, -- ContractTypeName
-- , sm.SM_StatusDesc as "Appt Status"
PaymentSource,
         CASE
                                          WHEN lower(ContractTypeName)='mp_pharmacy'
                                               AND JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.isCashless')='true' THEN 'CashlessOrder'
                                          WHEN lower(ContractTypeName)!='mp_pharmacy'
                                               AND (MAWallet>0
                                                    OR JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.isCashless')='true') THEN 'CashlessOrder'
                                          ELSE 'CashOrder'
                                      END AS isCashless ,
                                    
                                      CASE
                                          WHEN AppointmentStatus = 'Attended' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Callback Required' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Cancelled by Medical Center' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Cancelled by Ops' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Cancelled by User' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Confirmed by Medical Center' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Confirmed by Ops' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Confirmed by Retailer' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Customer Awaited' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Delivered Partial' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Delivery attempt failed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivery Executive Assigned' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Info submitted' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Invoice Generated' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Invoice Pending' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'MACancel' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'New Appointment Request' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'New Cashless Request' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Not Attended' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'On Hold' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Out for Delivery' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Placed with Partner' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Postponed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rejected by Moderator' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Rejected by Retailer' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Rescheduled by Ops' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rescheduled by User' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Settled' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'UserCancel' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'System Confirmed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rescheduled by Medical Center' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Cancelled' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Confirmed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Processed' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Ready To Deliver' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Received' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Medicine Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Medicine Delivery in Progess' THEN 'InProcess'
                                      END AS "Final_Status" 
         

                          ,EmployeeID,
                         convert(date, ApptCreatedDate) AS ApptCreatedDate -- , AppointmentApprovalDate
-- , DATEDIFF(day, AppointmentApprovalDate, ApptCreatedDate) as ApptApprovalTAT
-- , ApptDate
-- , DATEDIFF(day, ApptDate, AppointmentApprovalDate) as ApptTAT_2

,

-- CASE
--     WHEN CorporateName IS NOT NULL
--          AND CorporateName != '0' THEN CorporateName
--     ELSE E_FullName
-- END AS CorporateName_Rpt ,
-- E_FullName AS CorporateName ,
-- MAID ,
ContactNo,
PrimaryID,
MAID,
MAWallet ,
MBWallet ,
CorpWallet ,

CASE
    WHEN ContractTypeName='MP_PHARMACY'
         OR ContractTypeName='Pharmacy_Mediassist' THEN JSON_VALUE(convert(varchar(MAX),Properties),'$.cashlessAmount')
    ELSE '0'
END AS PharmacyCashless ,
CASE
    WHEN ContractTypeName='MP_PHARMACY'
         OR ContractTypeName='Pharmacy_Mediassist' THEN JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount')
    ELSE '0'
END AS PharmacyCash ,
 
(CASE
     WHEN oft.Amount> 0 THEN oft.Amount
     ELSE 0
 END + CASE
          WHEN ContractTypeName='MP_PHARMACY'
                OR ContractTypeName='Pharmacy_Mediassist' THEN cast(JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount') AS decimal)
          ELSE 0
      END) AS OrderValue ,
CASE
    WHEN ProviderERID = 72326 THEN (CASE
                                                 WHEN oft.Amount> 0 THEN oft.Amount
                                                 ELSE 0
                                             END + CASE
                                                      WHEN ContractTypeName='MP_PHARMACY'
                                                            OR ContractTypeName='Pharmacy_Mediassist' THEN cast(JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount') AS decimal)
                                                      ELSE 0
                                                  END) / 0.85
    WHEN ProviderERID = 199600 THEN (CASE
                                              WHEN oft.Amount> 0 THEN oft.Amount
                                              ELSE 0
                                          END + CASE
                                                    WHEN ContractTypeName='MP_PHARMACY'
                                                         OR ContractTypeName='Pharmacy_Mediassist' THEN cast(JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount') AS decimal)
                                                    ELSE 0
                                                END) / 0.90
END AS GMV ,

 -- , JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.isCashless') AS IsCashless

                                      
                                      
                                      AppointmentStatus 
                                      
                                     
FROM rpt.MediMarketRequest (nolock) -- inner  join MediMarket.dbo.tblrequest_Archive tr  with (nolock)
--  on  tr.R_Id=RequestID and isactive=1

INNER JOIN MediMarket.dbo.tblrequest tr WITH (nolock) ON tr.R_Id=RequestID
AND isactive=1
LEFT JOIN providermaster.pmr.vwEntityRelation vwer (nolock) ON CorporateERID = vwer.ID
LEFT JOIN MediMarket.dbo.tblworkflowstatusmaster sm WITH (nolock) ON appointmentstatusid=sm.sm_id
LEFT JOIN Medimarket.dbo.tblorder(nolock) ord ON ord.o_id=OrderId
LEFT JOIN MediAuth2..tblapplicationuser tau WITH(nolock) ON tau.tau_id=R_createdby
LEFT JOIN ProviderMaster.pmr.ENTITY et WITH (nolock) ON et.E_id= TAU_ProviderMasterEntityId
LEFT JOIN
( SELECT O_id,
         MBWallet,
         CorpWallet,
         CouponCode,
         PaymentGateway,
         MAWallet,
         HomeSampleCollectionCharges
 FROM
  ( SELECT Amount,
            PaymentSource,
            O_id
    FROM Medimarket.marketplace.orderfinancial (nolock) ) d pivot ( MAX(Amount)
                                                                  FOR PaymentSource IN (MBWallet, CorpWallet, CouponCode, PaymentGateway, MAWallet,HomeSampleCollectionCharges) ) piv )AS tt ON tt.O_ID=OrderId
-- LEFT JOIN medimarket..tblOrderItem oit ON oit.OI_Id = orderitemid
-- LEFT JOIN MediMarket.Provider.PharmaDetail(NOLOCK) ON MA_RequestID = R_ID
-- LEFT JOIN MediMarket..tblRequestFlow(nolock) rf ON R_Id = rf.RF_R_ID
-- AND RF_IsActive = 1
-- AND RF_StatusId IN (86,
--                     87)
LEFT JOIN
(SELECT O_Id,
        TransactionId,PaymentSource,
        MAX(Amount) AS Amount
 FROM Medimarket.marketplace.orderfinancial (nolock)
 GROUP BY O_Id,PaymentSource,
          TransactionId) oft ON oft.O_ID=OrderId
-- LEFT JOIN
-- (SELECT RW_R_Id,
--         RW_TrackingNumber
--  FROM Medimarket.dbo.tblRequestWallet (nolock)) rw ON rw.RW_R_Id=RequestId
WHERE ContractTypeId IN (-- 10386,10389,10390,10401,10846,10847,10861,10949,12553,
10307,
10390 -- 10389 -- MP_INVESTIGATION
-- 10401 -- MP_CONSULTATION
-- ,13465
)
 AND ApptCreatedDate>='{startDate}'
     AND ApptCreatedDate < '{endDate}'
      AND R_Remarks not in ('Doctor consultation successful, order will be processed again', 'Reason : Placed a new order;Placed a new order', 'Due to technical issue order got cancelled and new order raised with correct address.', 'We are unable to proceed your order due to the technical and hence we have placed the new order.' , 'Duplicate Order' , 'Duplicate Entry')
AND MAID IS NULL
) AS V 
where EmployeeID IS not NULL
  AND EmployeeID <> 'Non_Board_DA'
and IsCashless='CashlessOrder'""".format(startDate = startdate, endDate = enddate)

dfRetail_Cashless = pd.read_sql(G, conMB)
dfRetail_Cashless['Total_GMV'] = dfRetail_Cashless['Total_GMV'].apply(lambda i :  int(i))
dfRetail_Cashless


# In[52]:


H = """select   count(RequestID) as total_orders,sum(GMV) as Total_GMV  FROM  (
SELECT -- LastUpdateDate,
DISTINCT RequestID, -- ContractTypeName
-- , sm.SM_StatusDesc as "Appt Status"
PaymentSource,
         CASE
                                          WHEN lower(ContractTypeName)='mp_pharmacy'
                                               AND JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.isCashless')='true' THEN 'CashlessOrder'
                                          WHEN lower(ContractTypeName)!='mp_pharmacy'
                                               AND (MAWallet>0
                                                    OR JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.isCashless')='true') THEN 'CashlessOrder'
                                          ELSE 'CashOrder'
                                      END AS isCashless ,
                                    
                                      CASE
                                          WHEN AppointmentStatus = 'Attended' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Callback Required' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Cancelled by Medical Center' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Cancelled by Ops' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Cancelled by User' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Confirmed by Medical Center' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Confirmed by Ops' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Confirmed by Retailer' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Customer Awaited' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Delivered Partial' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Delivery attempt failed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivery Executive Assigned' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Info submitted' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Invoice Generated' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Invoice Pending' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'MACancel' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'New Appointment Request' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'New Cashless Request' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Not Attended' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'On Hold' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Out for Delivery' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Placed with Partner' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Postponed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rejected by Moderator' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Rejected by Retailer' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Rescheduled by Ops' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rescheduled by User' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Settled' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'UserCancel' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'System Confirmed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rescheduled by Medical Center' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Cancelled' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Confirmed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Processed' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Ready To Deliver' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Received' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Medicine Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Medicine Delivery in Progess' THEN 'InProcess'
                                      END AS "Final_Status" 
         

                          ,EmployeeID,
                         convert(date, ApptCreatedDate) AS ApptCreatedDate -- , AppointmentApprovalDate
-- , DATEDIFF(day, AppointmentApprovalDate, ApptCreatedDate) as ApptApprovalTAT
-- , ApptDate
-- , DATEDIFF(day, ApptDate, AppointmentApprovalDate) as ApptTAT_2

,

-- CASE
--     WHEN CorporateName IS NOT NULL
--          AND CorporateName != '0' THEN CorporateName
--     ELSE E_FullName
-- END AS CorporateName_Rpt ,
-- E_FullName AS CorporateName ,
-- MAID ,
ContactNo,
PrimaryID,
MAID,
MAWallet ,
MBWallet ,
CorpWallet ,

CASE
    WHEN ContractTypeName='MP_PHARMACY'
         OR ContractTypeName='Pharmacy_Mediassist' THEN JSON_VALUE(convert(varchar(MAX),Properties),'$.cashlessAmount')
    ELSE '0'
END AS PharmacyCashless ,
CASE
    WHEN ContractTypeName='MP_PHARMACY'
         OR ContractTypeName='Pharmacy_Mediassist' THEN JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount')
    ELSE '0'
END AS PharmacyCash ,
 
(CASE
     WHEN oft.Amount> 0 THEN oft.Amount
     ELSE 0
 END + CASE
          WHEN ContractTypeName='MP_PHARMACY'
                OR ContractTypeName='Pharmacy_Mediassist' THEN cast(JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount') AS decimal)
          ELSE 0
      END) AS OrderValue ,
CASE
    WHEN ProviderERID = 72326 THEN (CASE
                                                 WHEN oft.Amount> 0 THEN oft.Amount
                                                 ELSE 0
                                             END + CASE
                                                      WHEN ContractTypeName='MP_PHARMACY'
                                                            OR ContractTypeName='Pharmacy_Mediassist' THEN cast(JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount') AS decimal)
                                                      ELSE 0
                                                  END) / 0.85
    WHEN ProviderERID = 199600 THEN (CASE
                                              WHEN oft.Amount> 0 THEN oft.Amount
                                              ELSE 0
                                          END + CASE
                                                    WHEN ContractTypeName='MP_PHARMACY'
                                                         OR ContractTypeName='Pharmacy_Mediassist' THEN cast(JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount') AS decimal)
                                                    ELSE 0
                                                END) / 0.90
END AS GMV ,

 -- , JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.isCashless') AS IsCashless

                                      
                                      
                                      AppointmentStatus 
                                      
                                     
FROM rpt.MediMarketRequest (nolock) -- inner  join MediMarket.dbo.tblrequest_Archive tr  with (nolock)
--  on  tr.R_Id=RequestID and isactive=1

INNER JOIN MediMarket.dbo.tblrequest tr WITH (nolock) ON tr.R_Id=RequestID
AND isactive=1
LEFT JOIN providermaster.pmr.vwEntityRelation vwer (nolock) ON CorporateERID = vwer.ID
LEFT JOIN MediMarket.dbo.tblworkflowstatusmaster sm WITH (nolock) ON appointmentstatusid=sm.sm_id
LEFT JOIN Medimarket.dbo.tblorder(nolock) ord ON ord.o_id=OrderId
LEFT JOIN MediAuth2..tblapplicationuser tau WITH(nolock) ON tau.tau_id=R_createdby
LEFT JOIN ProviderMaster.pmr.ENTITY et WITH (nolock) ON et.E_id= TAU_ProviderMasterEntityId
LEFT JOIN
( SELECT O_id,
         MBWallet,
         CorpWallet,
         CouponCode,
         PaymentGateway,
         MAWallet,
         HomeSampleCollectionCharges
 FROM
  ( SELECT Amount,
            PaymentSource,
            O_id
    FROM Medimarket.marketplace.orderfinancial (nolock) ) d pivot ( MAX(Amount)
                                                                  FOR PaymentSource IN (MBWallet, CorpWallet, CouponCode, PaymentGateway, MAWallet,HomeSampleCollectionCharges) ) piv )AS tt ON tt.O_ID=OrderId
-- LEFT JOIN medimarket..tblOrderItem oit ON oit.OI_Id = orderitemid
-- LEFT JOIN MediMarket.Provider.PharmaDetail(NOLOCK) ON MA_RequestID = R_ID
-- LEFT JOIN MediMarket..tblRequestFlow(nolock) rf ON R_Id = rf.RF_R_ID
-- AND RF_IsActive = 1
-- AND RF_StatusId IN (86,
--                     87)
LEFT JOIN
(SELECT O_Id,
        TransactionId,PaymentSource,
        MAX(Amount) AS Amount
 FROM Medimarket.marketplace.orderfinancial (nolock)
 GROUP BY O_Id,PaymentSource,
          TransactionId) oft ON oft.O_ID=OrderId
-- LEFT JOIN
-- (SELECT RW_R_Id,
--         RW_TrackingNumber
--  FROM Medimarket.dbo.tblRequestWallet (nolock)) rw ON rw.RW_R_Id=RequestId
WHERE ContractTypeId IN (-- 10386,10389,10390,10401,10846,10847,10861,10949,12553,
10307,
10390 -- 10389 -- MP_INVESTIGATION
-- 10401 -- MP_CONSULTATION
-- ,13465
)
 AND ApptCreatedDate>='{startDate}'
     AND ApptCreatedDate < '{endDate}'
      AND R_Remarks not in ('Doctor consultation successful, order will be processed again', 'Reason : Placed a new order;Placed a new order', 'Due to technical issue order got cancelled and new order raised with correct address.', 'We are unable to proceed your order due to the technical and hence we have placed the new order.' , 'Duplicate Order' , 'Duplicate Entry')
AND MAID IS NOT NULL
) AS V 
where EmployeeID IS not NULL
  AND EmployeeID <> 'Non_Board_DA'
and IsCashless='CashOrder'""".format(startDate = startdate, endDate = enddate)

dfCorp_Cash = pd.read_sql(H, conMB)
dfCorp_Cash['Total_GMV'] = dfCorp_Cash['Total_GMV'].apply(lambda i :  int(i))
dfCorp_Cash


# In[53]:


I = """select   count(RequestID) as total_orders,sum(GMV) as Total_GMV  FROM  (
SELECT -- LastUpdateDate,
DISTINCT RequestID, -- ContractTypeName
-- , sm.SM_StatusDesc as "Appt Status"
PaymentSource,
         CASE
                                          WHEN lower(ContractTypeName)='mp_pharmacy'
                                               AND JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.isCashless')='true' THEN 'CashlessOrder'
                                          WHEN lower(ContractTypeName)!='mp_pharmacy'
                                               AND (MAWallet>0
                                                    OR JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.isCashless')='true') THEN 'CashlessOrder'
                                          ELSE 'CashOrder'
                                      END AS isCashless ,
                                    
                                      CASE
                                          WHEN AppointmentStatus = 'Attended' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Callback Required' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Cancelled by Medical Center' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Cancelled by Ops' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Cancelled by User' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Confirmed by Medical Center' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Confirmed by Ops' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Confirmed by Retailer' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Customer Awaited' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Delivered Partial' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Delivery attempt failed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivery Executive Assigned' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Info submitted' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Invoice Generated' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Invoice Pending' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'MACancel' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'New Appointment Request' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'New Cashless Request' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Not Attended' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'On Hold' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Out for Delivery' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Placed with Partner' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Postponed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rejected by Moderator' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Rejected by Retailer' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Rescheduled by Ops' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rescheduled by User' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Settled' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'UserCancel' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'System Confirmed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rescheduled by Medical Center' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Cancelled' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Confirmed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Processed' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Ready To Deliver' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Received' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Medicine Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Medicine Delivery in Progess' THEN 'InProcess'
                                      END AS "Final_Status" 
         

                          ,EmployeeID,
                         convert(date, ApptCreatedDate) AS ApptCreatedDate -- , AppointmentApprovalDate
-- , DATEDIFF(day, AppointmentApprovalDate, ApptCreatedDate) as ApptApprovalTAT
-- , ApptDate
-- , DATEDIFF(day, ApptDate, AppointmentApprovalDate) as ApptTAT_2

,

-- CASE
--     WHEN CorporateName IS NOT NULL
--          AND CorporateName != '0' THEN CorporateName
--     ELSE E_FullName
-- END AS CorporateName_Rpt ,
-- E_FullName AS CorporateName ,
-- MAID ,
ContactNo,
PrimaryID,
MAID,
MAWallet ,
MBWallet ,
CorpWallet ,

CASE
    WHEN ContractTypeName='MP_PHARMACY'
         OR ContractTypeName='Pharmacy_Mediassist' THEN JSON_VALUE(convert(varchar(MAX),Properties),'$.cashlessAmount')
    ELSE '0'
END AS PharmacyCashless ,
CASE
    WHEN ContractTypeName='MP_PHARMACY'
         OR ContractTypeName='Pharmacy_Mediassist' THEN JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount')
    ELSE '0'
END AS PharmacyCash ,
 
(CASE
     WHEN oft.Amount> 0 THEN oft.Amount
     ELSE 0
 END + CASE
          WHEN ContractTypeName='MP_PHARMACY'
                OR ContractTypeName='Pharmacy_Mediassist' THEN cast(JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount') AS decimal)
          ELSE 0
      END) AS OrderValue ,
CASE
    WHEN ProviderERID = 72326 THEN (CASE
                                                 WHEN oft.Amount> 0 THEN oft.Amount
                                                 ELSE 0
                                             END + CASE
                                                      WHEN ContractTypeName='MP_PHARMACY'
                                                            OR ContractTypeName='Pharmacy_Mediassist' THEN cast(JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount') AS decimal)
                                                      ELSE 0
                                                  END) / 0.85
    WHEN ProviderERID = 199600 THEN (CASE
                                              WHEN oft.Amount> 0 THEN oft.Amount
                                              ELSE 0
                                          END + CASE
                                                    WHEN ContractTypeName='MP_PHARMACY'
                                                         OR ContractTypeName='Pharmacy_Mediassist' THEN cast(JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount') AS decimal)
                                                    ELSE 0
                                                END) / 0.90
END AS GMV ,

 -- , JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.isCashless') AS IsCashless

                                      
                                      
                                      AppointmentStatus 
                                      
                                     
FROM rpt.MediMarketRequest (nolock) -- inner  join MediMarket.dbo.tblrequest_Archive tr  with (nolock)
--  on  tr.R_Id=RequestID and isactive=1

INNER JOIN MediMarket.dbo.tblrequest tr WITH (nolock) ON tr.R_Id=RequestID
AND isactive=1
LEFT JOIN providermaster.pmr.vwEntityRelation vwer (nolock) ON CorporateERID = vwer.ID
LEFT JOIN MediMarket.dbo.tblworkflowstatusmaster sm WITH (nolock) ON appointmentstatusid=sm.sm_id
LEFT JOIN Medimarket.dbo.tblorder(nolock) ord ON ord.o_id=OrderId
LEFT JOIN MediAuth2..tblapplicationuser tau WITH(nolock) ON tau.tau_id=R_createdby
LEFT JOIN ProviderMaster.pmr.ENTITY et WITH (nolock) ON et.E_id= TAU_ProviderMasterEntityId
LEFT JOIN
( SELECT O_id,
         MBWallet,
         CorpWallet,
         CouponCode,
         PaymentGateway,
         MAWallet,
         HomeSampleCollectionCharges
 FROM
  ( SELECT Amount,
            PaymentSource,
            O_id
    FROM Medimarket.marketplace.orderfinancial (nolock) ) d pivot ( MAX(Amount)
                                                                  FOR PaymentSource IN (MBWallet, CorpWallet, CouponCode, PaymentGateway, MAWallet,HomeSampleCollectionCharges) ) piv )AS tt ON tt.O_ID=OrderId
-- LEFT JOIN medimarket..tblOrderItem oit ON oit.OI_Id = orderitemid
-- LEFT JOIN MediMarket.Provider.PharmaDetail(NOLOCK) ON MA_RequestID = R_ID
-- LEFT JOIN MediMarket..tblRequestFlow(nolock) rf ON R_Id = rf.RF_R_ID
-- AND RF_IsActive = 1
-- AND RF_StatusId IN (86,
--                     87)
LEFT JOIN
(SELECT O_Id,
        TransactionId,PaymentSource,
        MAX(Amount) AS Amount
 FROM Medimarket.marketplace.orderfinancial (nolock)
 GROUP BY O_Id,PaymentSource,
          TransactionId) oft ON oft.O_ID=OrderId
-- LEFT JOIN
-- (SELECT RW_R_Id,
--         RW_TrackingNumber
--  FROM Medimarket.dbo.tblRequestWallet (nolock)) rw ON rw.RW_R_Id=RequestId
WHERE ContractTypeId IN (-- 10386,10389,10390,10401,10846,10847,10861,10949,12553,
10307,
10390 -- 10389 -- MP_INVESTIGATION
-- 10401 -- MP_CONSULTATION
-- ,13465
)
 AND ApptCreatedDate>='{startDate}'
     AND ApptCreatedDate < '{endDate}'
      AND R_Remarks not in ('Doctor consultation successful, order will be processed again', 'Reason : Placed a new order;Placed a new order', 'Due to technical issue order got cancelled and new order raised with correct address.', 'We are unable to proceed your order due to the technical and hence we have placed the new order.' , 'Duplicate Order' , 'Duplicate Entry')
AND MAID IS NOT NULL
) AS V 
where EmployeeID IS not NULL
  AND EmployeeID <> 'Non_Board_DA'
and IsCashless='CashlessOrder'""".format(startDate = startdate, endDate = enddate)

dfCorp_Cashless = pd.read_sql(I, conMB)
dfCorp_Cashless['Total_GMV'] = dfCorp_Cashless['Total_GMV'].apply(lambda i :  int(i))
dfCorp_Cashless


# In[54]:


SS="""SELECT 
      
       OrderType,
       count(RequestID) AS CountOfOrdersCreated
      
FROM
  (SELECT CASE
              WHEN Properties LIKE '%source\":\"web%' THEN 'Web'
              WHEN Properties LIKE '%source\":\"AndroidMB%' THEN 'Android'
              WHEN Properties LIKE '%source\":\"iOSMB%' THEN 'iOS'
              WHEN lower(O_Blob) LIKE '%"createdByApp"%' THEN JSON_VALUE(CONVERT(varchar(MAX), O_Blob), '$.createdByApp')
              ELSE 'No Info Available'
          END AS Platform,MAID,
          CASE
              WHEN lower(Properties) like '%type\":\"search%' OR lower(Properties) like '%type\":\"onlyotc%' THEN 'SEARCH'
              WHEN lower(Properties) LIKE '%type\":\"uploadrx%' THEN 'UPLOADRX'
              WHEN lower(Properties) LIKE '%type\":\"reorder%' THEN 'REORDER'
              WHEN lower(Properties) LIKE '%type\":\"re-order%' THEN 'REORDER'
              WHEN lower(Properties) LIKE '%type\":\"essentials%' THEN 'ESSENTIALS'
              WHEN lower(Properties) LIKE '%type\":\"prescription%' THEN 'PRESCRIPTION'
              WHEN lower(Properties) LIKE '%"BookingSource"%' THEN JSON_VALUE(CONVERT(varchar(MAX), Properties), '$.additionals.BookingSource')
              ELSE 'No Info Available'
          END AS OrderType,
          RequestId,
          CASE
              WHEN AppointmentStatusId IN (57,
                                           87) THEN 1
          END AS Delivered,
          convert(date, ApptCreatedDate) AS ApptCreatedDate
   FROM rpt.MediMarketRequest (nolock) mmr
   LEFT JOIN dbo.tblRequest (nolock) tbr ON mmr.RequestId = tbr.R_Id
   LEFT JOIN dbo.tblOrder (nolock) tbo ON mmr.OrderId = tbo.O_Id
   WHERE ApptCreatedDate >= '{startDate}' AND ApptCreatedDate < '{endDate}'
     AND ContractTypeId IN (10307,
                            10390)) A
                           where Platform like 'Android' AND OrderType in ('SEARCH','UPLOADRX','REORDER')
                       
GROUP BY   OrderType """.format(startDate = startdate, endDate = enddate)

dfAndroid_ordertypeOrders = pd.read_sql(SS, conMB)
dfAndroid_ordertypeOrders


# In[58]:


# In[406]:


HEADER = """<html>
<head>
<style type="text/css">


.dataframe {
  font-family: "Open Sans", "HelveticaNeue", "Helvetica Neue", Helvetica, Arial, sans-serif;
  border-collapse: collapse;
  width: 90%;
}
.dataframe td, .dataframe th {
  border: 1px solid #4C4C4C;
  padding: 4px;
}

.dataframe tr:nth-child(even){background-color: #f2f2f2;}

.dataframe tr:hover {background-color: #add8e6;}

.dataframe th {
  padding-top: 2px;
  padding-bottom: 2px;
  text-align: center;
  background-color: #add8e6;
  color: #000000;
}

.dataframe td {
  text-align: center;
}

</style>

"""

FOOTER = """</body></html>"""

print('Imported Header and Footer...')

# In[407]:

gmail_user  = "deepak.nagar@medibuddy.in"
gmail_pwd="nmoeawovxqreawmj"


# In[437]:


def mail(to, subject, text, attach=None):

   msg = MIMEMultipart()
   msg['From'] = gmail_user
   msg['To'] = ", ".join(to)
   msg['Subject'] = subject

   msg.attach(MIMEText(text, 'html'))
   if attach:
       for items in attach:
              part = MIMEBase('application', 'octet-stream')
              part.set_payload(open(items, 'rb').read())
              Encoders.encode_base64(part)
              part.add_header('Content-Disposition', 'attachment; filename="%s"' % os.path.basename(items))
              msg.attach(part)
   mailServer = smtplib.SMTP("smtp.gmail.com", 587)
   mailServer.ehlo()
   mailServer.starttls()
   mailServer.ehlo()
   mailServer.login(gmail_user, gmail_pwd)
   mailServer.sendmail(gmail_user, to, msg.as_string())
   mailServer.close()


# In[ ]:





# In[59]:



html_yy= pivot(yy).to_html(justify ='center', index=False).replace('\n','')
html_dftotal= dftotal.to_html(justify ='center', index=False).replace('\n','')
html_dftotal_PG= dftotal_PG.to_html(justify ='center', index=False).replace('\n','')
html_dftotal_Cash= dftotal_Cash.to_html(justify ='center', index=False).replace('\n','')
html_dftotal_Cashless= dftotal_Cashless.to_html(justify ='center', index=False).replace('\n','')
html_dftotal_split= dftotal_split.to_html(justify ='center', index=False).replace('\n','')
html_dfCorp= dfCorp.to_html(justify ='center', index=False).replace('\n','')
html_dfCorp_PG= dfCorp_PG.to_html(justify ='center', index=False).replace('\n','')
html_dfCorp_split= dfCorp_split.to_html(justify ='center', index=False).replace('\n','')
html_dfRetail_split= dfRetail_split.to_html(justify ='center', index=False).replace('\n','')
html_dfRetail_total= dfRetail_total.to_html(justify ='center', index=False).replace('\n','')
html_dfRetail_Cash= dfRetail_Cash.to_html(justify ='center', index=False).replace('\n','')
html_dfRetail_Cashless= dfRetail_Cashless.to_html(justify ='center', index=False).replace('\n','')
html_dfCorp_Cash= dfCorp_Cash.to_html(justify ='center', index=False).replace('\n','')
html_dfCorp_Cashless= dfCorp_Cashless.to_html(justify ='center', index=False).replace('\n','')


# In[60]:


html = "<h2>Total Meds </h2>" + html_yy + '<br/><br/>' + "<h2>Total </h2>" + html_dftotal + '<br/><br/>' + "<h2>TotalPG </h2>" + html_dftotal_PG + '<br/><br/>' + "<h2>TotalCash</h2>" + html_dftotal_Cash+'<br/><br/>' + "<h2>TotalCashless</h2>" + html_dftotal_Cashless +'<br/><br/>' + "<h2>TotallSplit</h2>" + html_dftotal_split + "<h2>Total Corporate Meds </h2>" + html_dfCorp + '<br/><br/>' + "<h2> Corporate PG </h2>" + html_dfCorp_PG + '<br/><br/>' + "<h2>Total Corporate Cash </h2>" + html_dfCorp_Cash + '<br/><br/>' + "<h2>Total Corporate Cashless</h2>" + html_dfCorp_Cashless +'<br/><br/>' + "<h2>Total Corporate split</h2>" + html_dfCorp_split +'<br/><br/>' + "<h2>Total Retail Split</h2>" + html_dfRetail_total+ "<h2>Total Retail Cash </h2>" + html_dfRetail_Cash + '<br/><br/>' + "<h2>Total Retail Cashless </h2>" + html_dfRetail_Cashless + '<br/><br/>' + "<h2>Total Retail Split </h2>" + html_dfRetail_split

today = datetime.date.today()
yesterday = (today - datetime.timedelta(days=1)).strftime('%d-%b-%Y')
print(yesterday)
    
Heading = '3P' + '(' + yesterday + ')'
recipients = ["deepak.nagar@medibuddy.in","sivathanu.k@docsapp.in","adhithya.parthasarathi@medibuddy.in","vishwanath.ms@medibuddy.in","ronak.shah@medibuddy.in"]

#recipients = ["mukul.bansal@docsapp.in","ashima.setia@medibuddy.in","yashaswee.p@medibuddy.in","srishti.jain@docsapp.in","avinash.kumar@docsapp.in"]


#recipients = ["mukul.bansal@docsapp.in","avinash.kumar@docsapp.in","sonal@docsapp.in","keerthi.kiran@medibuddy.in",
#"shreyas@docsapp.in","balakrishna@docsapp.in","rahul.bora@medibuddy.in","harsha@medibuddy.in","yashaswee.p@medibuddy.in",
#"prachi.singh@medibuddy.in","sandeep.singh@medibuddy.in","srishti.jain@docsapp.in"]

email_body = HEADER + html + FOOTER
#print(email_body)
mail(recipients,Heading,email_body,None)
print('mail sent')
    



conMB.close()


# In[29]:


html = "<h2>Total Meds </h2>" + html_yy + '<br/><br/>' + "<h2>Total </h2>" + html_dftotal + '<br/><br/>' + "<h2>TotalPG </h2>" + html_dftotal_PG + '<br/><br/>' + "<h2>TotalCash</h2>" + html_dftotal_Cash+'<br/><br/>' + "<h2>TotalCashless</h2>" + html_dftotal_Cashless +'<br/><br/>' + "<h2>TotallSplit</h2>" + html_dftotal_split + "<h2>Total Corporate Meds </h2>" + html_dfCorp + '<br/><br/>' + "<h2> Corporate PG </h2>" + html_dfCorp_PG + '<br/><br/>' + "<h2>Total Corporate Cash </h2>" + html_dfCorp_Cash + '<br/><br/>' + "<h2>Total Corporate Cashless</h2>" + html_dfCorp_Cashless +'<br/><br/>' + "<h2>Total Corporate split</h2>" + html_dfCorp_split +'<br/><br/>' + "<h2>Total Retail Split</h2>" + html_dfRetail_total+ "<h2>Total Retail Cash </h2>" + html_dfRetail_Cash + '<br/><br/>' + "<h2>Total Retail Cashless </h2>" + html_dfRetail_Cashless + '<br/><br/>' + "<h2>Total Retail Split </h2>" + html_dfRetail_split

today = datetime.date.today()
yesterday = (today - datetime.timedelta(days=1)).strftime('%d-%b-%Y')
print(yesterday)
    
Heading = '3P' + '(' + yesterday + ')'
recipients = ["deepak.nagar@medibuddy.in","sivathanu.k@docsapp.in","adhithya.parthasarathi@medibuddy.in","vishwanath.ms@medibuddy.in","ronak.shah@medibuddy.in"]

#recipients = ["mukul.bansal@docsapp.in","ashima.setia@medibuddy.in","yashaswee.p@medibuddy.in","srishti.jain@docsapp.in","avinash.kumar@docsapp.in"]


#recipients = ["mukul.bansal@docsapp.in","avinash.kumar@docsapp.in","sonal@docsapp.in","keerthi.kiran@medibuddy.in",
#"shreyas@docsapp.in","balakrishna@docsapp.in","rahul.bora@medibuddy.in","harsha@medibuddy.in","yashaswee.p@medibuddy.in",
#"prachi.singh@medibuddy.in","sandeep.singh@medibuddy.in","srishti.jain@docsapp.in"]

email_body = HEADER + html + FOOTER
#print(email_body)
mail(recipients,Heading,email_body,None)
print('mail sent')
    



conMB.close()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




