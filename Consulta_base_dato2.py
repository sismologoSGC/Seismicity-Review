#from importlib.resources import files
import os
import argparse
#from matplotlib.pyplot import magnitude_spectrum
import pymysql
import utils as ut
import datetime as dt
import pandas as pd
from colorama import init, Fore
#import warnings

# database connection
connection = pymysql.connect(host="172.25.3.135", user="consulta", passwd="consulta", database="seiscomp3")
cursor = connection.cursor()
# some other statements  with the help of cursor

print(connection)

q1 = """
SELECT Origin.time_value, POEv.publicID, Origin.depth_value, Magnitude.magnitude_value, Origin.quality_standardError, Origin.depth_uncertainty, Origin.latitude_uncertainty, Origin.longitude_uncertainty, Origin.quality_associatedPhaseCount, Origin.creationInfo_author, Event.type, Origin.creationInfo_agencyID, EventDescription.text, Origin.latitude_value, Origin.longitude_value, Magnitude.type, Origin.methodID, Origin.earthModelID
FROM Event AS EvMF left join PublicObject AS POEv ON EvMF._oid = POEv._oid
left join PublicObject as POOri ON EvMF.preferredOriginID=POOri.publicID  
left join Origin ON POOri._oid=Origin._oid left join PublicObject as POMag on EvMF.preferredMagnitudeID=POMag.publicID  
left join Magnitude ON Magnitude._oid = POMag._oid  
left join Event ON Event._oid= POEv._oid 
left join EventDescription ON EvMF._oid = EventDescription._parent_oid
WHERE
Origin.time_value between "2022/07/01 00:00:00" and "2022/07/01 00:59:59";
"""

sql_db = pd.read_sql_query(q1,connection)
df = pd.DataFrame(sql_db)
df = df.where(pd.notnull(df), None)
df = df.sort_values("time_value",ascending=False)
print(df)

eventos = df.values.tolist()

for ev in eventos:
    latitude = ev[13]                                                 #latitud

print(ev)