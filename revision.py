#!/usr/bin/python3
#from importlib.resources import files
import os
import argparse
#from matplotlib.pyplot import magnitude_spectrum
import pymysql
import utils as ut
import datetime as dt
import pandas as pd
from colorama import init, Fore
import numpy as np
#import warnings

#warnings.filterwarnings('ignore')
init(autoreset=True)

def connect2mysql(name,starttime,endtime):
    """
    Parameters:
    -----------
    name : str
        'sentido','destacado','normal' 
    starttime: datetime object
        Start time with the next format: "YYYYmmdd HHMMss"
    endtime: datetime object
        End time with the next format: "YYYYmmdd HHMMss"

    returns:
    --------
    codex : str
        string to PHPmyAdmin    
    """
    year1 = f"{starttime.year:02d}"
    mes1 = f"{starttime.month:02d}"
    dia1 = f"{starttime.day:02d}"
    hora1 = f"{starttime.hour:02d}"
    min1 = f"{starttime.minute:02d}"
    sec1 = f"{starttime.second:02d}"

    year2 = f"{endtime.year:02d}"
    mes2 = f"{endtime.month:02d}"
    dia2 = f"{endtime.day:02d}"
    hora2 = f"{endtime.hour:02d}"
    min2 = f"{endtime.minute:02d}"
    sec2 = f"{endtime.second:02d}"

    if name == 'normal':
        codex = f'Select   Origin.time_value, POEv.publicID, Origin.depth_value, Magnitude.magnitude_value, Origin.quality_standardError, Origin.depth_uncertainty, Origin.latitude_uncertainty, Origin.longitude_uncertainty, Origin.quality_associatedPhaseCount, Origin.creationInfo_author, Event.type, Origin.creationInfo_agencyID, EventDescription.text, Origin.latitude_value, Origin.longitude_value, Magnitude.type, Origin.methodID, Origin.earthModelID\
                    from Event AS EvMF left join PublicObject AS POEv ON EvMF._oid = POEv._oid\
                    left join PublicObject as POOri ON EvMF.preferredOriginID=POOri.publicID  \
                    left join Origin ON POOri._oid=Origin._oid left join PublicObject as POMag on EvMF.preferredMagnitudeID=POMag.publicID  \
                    left join Magnitude ON Magnitude._oid = POMag._oid  \
                    left join Event ON Event._oid= POEv._oid \
                    left join EventDescription ON EvMF._oid = EventDescription._parent_oid\
                    where \
                    Origin.time_value between "{year1}/{mes1}/{dia1} {hora1}:{min1}:{sec1}" and "{year2}/{mes2}/{dia2} {hora2}:{min2}:{sec2}"'

    elif name == 'sentido':
                codex = f'Select   Origin.time_value, POEv.publicID, Origin.depth_value, Magnitude.magnitude_value, Origin.quality_standardError, Origin.depth_uncertainty, Origin.latitude_uncertainty, Origin.longitude_uncertainty, Origin.quality_associatedPhaseCount, Origin.creationInfo_author, Event.type, Origin.creationInfo_agencyID, FeltReport.report, Magnitude.type, Origin.methodID, Origin.earthModelID\
                    from Event AS EvMF left join PublicObject AS POEv ON EvMF._oid = POEv._oid\
                    left join PublicObject as POOri ON EvMF.preferredOriginID=POOri.publicID  \
                    left join Origin ON POOri._oid=Origin._oid left join PublicObject as POMag on EvMF.preferredMagnitudeID=POMag.publicID  \
                    left join Magnitude ON Magnitude._oid = POMag._oid  \
                    left join Event ON Event._oid= POEv._oid, FeltReport\
                    where \
                    FeltReport._oid = Event._oid\
                    AND Origin.time_value between "{year1}/{mes1}/{dia1} {hora1}:{min1}:{sec1}" and "{year2}/{mes2}/{dia2} {hora2}:{min2}:{sec2}"'
    elif name == 'destacado':
        codex = f'Select   Origin.time_value, POEv.publicID, Origin.depth_value, Magnitude.magnitude_value,  Origin.quality_standardError, Origin.depth_uncertainty, Origin.latitude_uncertainty, Origin.longitude_uncertainty, Origin.quality_associatedPhaseCount, Origin.creationInfo_author, Event.type, Origin.creationInfo_agencyID, Comment.text, Magnitude.type, Origin.methodID, Origin.earthModelID\
                from Event AS EvMF left join PublicObject AS POEv ON EvMF._oid = POEv._oid\
                left join PublicObject as POOri ON EvMF.preferredOriginID=POOri.publicID  \
                left join Origin ON POOri._oid=Origin._oid left join PublicObject as POMag on EvMF.preferredMagnitudeID=POMag.publicID  \
                left join Magnitude ON Magnitude._oid = POMag._oid  \
                left join Event ON Event._oid= POEv._oid, Comment\
                where \
                Comment._parent_oid = Event._oid\
                AND Comment.text LIKE "%DESTACADO%"\
                AND Origin.time_value between "{year1}/{mes1}/{dia1} {hora1}:{min1}:{sec1}" and "{year2}/{mes2}/{dia2} {hora2}::{min2}:{sec2}"'

    db = pymysql.connect(host="172.25.3.135", user="consulta", passwd="consulta", db="seiscomp3")
    sql_db = pd.read_sql_query(codex,db)
    df = pd.DataFrame(sql_db)
    df = df.where(pd.notnull(df), None)
    df = df.sort_values("time_value",ascending=False)
    return df

def sentido_process(df):
    """
    Parameters:
-----------
    df : DataFrame
        dataframe obtained by connect2mysql method with name='sentido'.

    """
    Senti = []
    IDsen = []
    feel = {}
    feelv = []
    eventos = df.values.tolist()
    for ev in eventos:
        Senti.append(ev[12])
        sentidos = str(ev[12]).split(",")                         #Sentidos
        Nsentidos = len(sentidos)                         #Numero de sentidos
        if Nsentidos == 3 and "Bogot  " not in sentidos or Nsentidos >= 4:
            IDse = ev[1]
            IDsen.append(IDse) 
            feel[IDse] = Nsentidos

    return IDsen

def destacado_process(df):
    """
    Parameters:
    -----------
    df : DataFrame
        dataframe obtained by connect2mysql method with name='destacado'.

    """
    IDs = []
    eventos = df.values.tolist()
    for ev in eventos:
        ID = ev[1]                                 #ID
        IDs.append(ID)
    return IDs

def normal_process(df,IDs,IDsen,user=None):
    """
    Parameters:
    -----------
    df : DataFrame
        dataframe obtained by connect2mysql method with mode='normal'.

    """
    volcanic_bna_folder = os.path.join(os.path.dirname(__file__),"bna_volcanic_files")
    #files= glob.glob(os.path.join("/home/lmercado/lmercado/Revision_Sismicidad/model_files/","*"))
    files_folder = os.path.join(os.path.dirname(__file__),"model_files")

    tit = "Fecha                ID        Depth    Mag  T_Mag    Rms  Erprof   Erlat   Erlon    fases     Author          Type                Agency      Observaci  n "
    errms=""
    errloc = ""
    errfases = ""
    errevpic = ""
    errevint= ""
    magn= ""
    print(Fore.GREEN +"\n-------------------------------------------------------------------------------------------------------------------------------------------------------------------------")
    print(tit)
    print(Fore.GREEN +"\n-------------------------------------------------------------------------------------------------------------------------------------------------------------------------")
    conteo=0
    conteore=0
    
    eventos = df.values.tolist()
    for ev in eventos:
        fecha = str(ev[0].year)+"/"+str(ev[0].month)+"/"+str(ev[0].day)+" "+str(ev[0].hour)+":"+str(ev[0].minute)+":"+str(ev[0].second)
        fe = fecha.ljust(16," ")                   #fecha
        ID = str(ev[1]).ljust(14," ")                                 #ID
        if ev[3] != None:                          #Magnitud
            mag = str(round(ev[3],2)).ljust(6," ")
        else:
            mag = "None".ljust(6," ")
        if ev[2] != None:
            dep = str(round(ev[2],1)).ljust(7," ")    #profundidad
        else:
            dep = "None".ljust(7," ")
        if ev[4] != None:
            rms = str(round(ev[4],2)).ljust(6," ")    #rms
        else:
            rms ="None".ljust(6," ")
        if ev[5] != None:                         #errprofundidad
            erdep = str(round(ev[5],2)).ljust(7," ")
        else:
            erdep = "None".ljust(7," ")

        if ev[6] != None:                         #errorlatitud
            erlat = str(round(ev[6],2)).ljust(8," ")
        else:
            erlat = "None".ljust(8," ")

        if ev[7] != None:                         #errorlongitud
            erlon = str(round(ev[7],2)).ljust(8," ")
        else:
            erlon = "None".ljust(8," ")
        if ev[8] != None:                         #fases
            fases = str(ev[8]).ljust(7," ")
        else:
            fases = "None".ljust(5," ")
        aut = str(ev[9]).ljust(17," ")

        if ev[10] != None:                        #Type
            tip = str(ev[10][0:19]).ljust(22," ")#32
        else:
            tip = "None".ljust(22," ")
        agen = str(ev[11]).ljust(5," ")           #Agencia
        ubic = str(ev[12])                                                #Ubicacion
        latitude = ev[13]                                                 #latitud
        longitude = ev[14]                                                #longitud
        magnitude_type=str(ev[15]).ljust(9," ")
        method_type=str(ev[16])
        earthModel_type=str(ev[17])
        mag1='MLr_1'.ljust(9," ")
        magvmm='MLr_vmm'.ljust(9," ")
        mag2='MLr_2'.ljust(9," ")
        mag3='MLr_3'.ljust(9," ")
        mag4='MLr_4'.ljust(9," ")
        mag5='MLr_5'.ljust(9," ")
        
    

        errfa= ["not locatable","None"]
        errsi = ["not locatable","None", "not existing"]
        errloc=["not locatable","outside of network interest","volcanic eruption","explosion","not existing"]
        errvol = ["not locatable","volcanic eruption","not existing"]
        autors = ["scanloc", "scautoloc_reg", "scanlocbay"]
        suerr = []


        if user != None:
            if str(ev[9]) == user+"@proc3" or str(ev[9]) == user+"@proc1" or str(ev[9]) == user+"@proc2" or str(ev[9]) == user+"@proc4":
                    if ev[4] != None:
                            if float(ev[4]) >= 1.51 and ev[10] not in errloc and ev[1] not in suerr:
                                    errms += f"{fe} {ID} {dep}{mag}{magnitude_type}"+Fore.RED+f"{rms}"+f" {erdep}{erlat}{erlon} {fases}{aut} {tip} {agen}\n"
                                    suerr.append(ID)
                                    conteo += 1
                                    comment= "RMS alto".ljust(32," ")
                                    print(Fore.GREEN+f"{fe}"+Fore.WHITE+f" {ID} {dep}{mag}{magnitude_type}"+Fore.RED+f"{rms}"+Fore.WHITE+f" {erdep}{erlat}{erlon} {fases}{aut} {tip} {agen}    {comment}   {ubic}")

                            if ev[6] != None and ev[7] != None and ev[5] != None and ev[1] not in suerr and str(ev[10]) not in errloc:
                                    if float(ev[6]) >= 12.0 or float(ev[7]) >= 12.0 or float(ev[5]) >=12.0:
                                        suerr.append(ID)
                                        conteo += 1
                                        errloc += f"{fe} {ID} {dep}{mag}{magnitude_type}"+Fore.RED+f"{rms}"+f" {erdep}{erlat}{erlon} {fases}{aut} {tip} {agen}\n"
                                        comment= "Errores de localizacion alto".ljust(32," ")
                                        print(Fore.GREEN+f"{fe}"+Fore.WHITE+f" {ID} {dep}{mag}{magnitude_type}{rms}"+Fore.RED+f" {erdep}{erlat}{erlon}"+Fore.WHITE+f" {fases}{aut} {tip} {agen}    {comment}   {ubic}")
                           
                            if ev[8] != None and np.isnan(ev[8]) == False and ev[1] not in suerr:
                                if int(ev[8]) >= 12  and ev[10] in errfa:
                                    if ut.inside_bna_polygon((longitude,latitude),volcanic_bna_folder) != True:
                                        conteo += 1
                                        errfases += f"{fe} {ID} {dep}{mag}{magnitude_type}"+Fore.RED+f"{rms}"+f" {erdep}{erlat}{erlon} {fases}{aut} {tip} {agen}\n"
                                        comment= "Evento localizable".ljust(32," ")
                                        print(Fore.GREEN+f"{fe}"+Fore.WHITE+f" {ID} {dep}{mag}{magnitude_type}{rms} {erdep}{erlat}{erlon}"+Fore.RED+f" {fases}"+Fore.WHITE+f"{aut} {tip} {agen}    {comment}   {ubic}")
                            
                            
                            if ev[8] != None and np.isnan(ev[8]) == False and ev[1] not in suerr:
                                if int(ev[8]) >= 7  and ev[10] not in errsi:
                                    if ut.test2((longitude,latitude),files_folder) == (True, 'zona1') and magnitude_type != mag1:
                                        conteo += 1
                                        comment= "Corregir magnitud con MLr_1".ljust(32," ")
                                        print(Fore.GREEN+f"{fe}"+Fore.WHITE+f" {ID} {dep}{mag}"+Fore.RED+f"{magnitude_type}"+Fore.WHITE+f"{rms} {erdep}{erlat}{erlon}"+Fore.RED+f" {fases}"+Fore.WHITE+f"{aut} {tip} {agen}    {comment}   {ubic}")
                            
                            if ev[8] != None and np.isnan(ev[8]) == False and ev[1] not in suerr:
                                if int(ev[8]) >= 7  and ev[10] not in errsi:
                                    if ut.test2((longitude,latitude),files_folder) == (True, 'zona2') and magnitude_type != mag2:
                                        conteo += 1
                                        comment= "Corregir magnitud con MLr_2".ljust(32," ")
                                        print(Fore.GREEN+f"{fe}"+Fore.WHITE+f" {ID} {dep}{mag}"+Fore.RED+f"{magnitude_type}"+Fore.WHITE+f"{rms} {erdep}{erlat}{erlon}"+Fore.RED+f" {fases}"+Fore.WHITE+f"{aut} {tip} {agen}    {comment}   {ubic}")

                            
                            if ev[8] != None and np.isnan(ev[8]) == False and ev[1] not in suerr:
                                if int(ev[8]) >= 7  and ev[10] not in errsi:
                                    if ut.test2((longitude,latitude),files_folder) == (True, 'zona3') and magnitude_type != mag3:
                                        conteo += 1
                                        comment= "Corregir magnitud con MLr_3".ljust(32," ")
                                        print(Fore.GREEN+f"{fe}"+Fore.WHITE+f" {ID} {dep}{mag}"+Fore.RED+f"{magnitude_type}"+Fore.WHITE+f"{rms} {erdep}{erlat}{erlon}"+Fore.RED+f" {fases}"+Fore.WHITE+f"{aut} {tip} {agen}    {comment}   {ubic}")


                            if ev[8] != None and np.isnan(ev[8]) == False and ev[1] not in suerr:
                                if int(ev[8]) >= 7  and ev[10] not in errsi:
                                    if ut.test2((longitude,latitude),files_folder) == (True, 'zona4') and magnitude_type != mag4:
                                        conteo += 1
                                        comment= "Corregir magnitud con MLr_4".ljust(32," ")
                                        print(Fore.GREEN+f"{fe}"+Fore.WHITE+f" {ID} {dep}{mag}"+Fore.RED+f"{magnitude_type}"+Fore.WHITE+f"{rms} {erdep}{erlat}{erlon}"+Fore.RED+f" {fases}"+Fore.WHITE+f"{aut} {tip} {agen}    {comment}   {ubic}")
                            
                            if ev[8] != None and np.isnan(ev[8]) == False and ev[1] not in suerr:
                                if int(ev[8]) >= 7  and ev[10] not in errsi:
                                    if ut.test2((longitude,latitude),files_folder) == (True, 'zona5') and magnitude_type != mag5:
                                        conteo += 1
                                        comment= "Corregir magnitud con MLr_5".ljust(32," ")
                                        print(Fore.GREEN+f"{fe}"+Fore.WHITE+f" {ID} {dep}{mag}"+Fore.RED+f"{magnitude_type}"+Fore.WHITE+f"{rms} {erdep}{erlat}{erlon}"+Fore.RED+f" {fases}"+Fore.WHITE+f"{aut} {tip} {agen}    {comment}   {ubic}")
                            
                            if ev[8] != None and np.isnan(ev[8]) == False and ev[1] not in suerr:
                                if int(ev[8]) >= 7  and ev[10] not in errsi:
                                    if ut.test2((longitude,latitude),files_folder) == (True, 'zona_vmm') and magnitude_type != magvmm:
                                        conteo += 1
                                        comment= "Corregir magnitud con MLr_vmm".ljust(32," ")
                                        print(Fore.GREEN+f"{fe}"+Fore.WHITE+f" {ID} {dep}{mag}"+Fore.RED+f"{magnitude_type}"+Fore.WHITE+f"{rms} {erdep}{erlat}{erlon}"+Fore.RED+f" {fases}"+Fore.WHITE+f"{aut} {tip} {agen}    {comment}   {ubic}")

                            if ev[8] != None and np.isnan(ev[8]) == False and ev[1] not in suerr:
                                if int(ev[8]) >= 7  and ev[10] not in errsi:
                                    if ut.test2((longitude,latitude),files_folder) == (True, 'zona_vmm') and earthModel_type != "VMM":
                                        conteo += 1
                                        comment= "Corregir modelo con VMM".ljust(32," ")
                                        print(Fore.GREEN+f"{fe}"+Fore.WHITE+f" {ID} {dep}{mag}{magnitude_type}"+Fore.WHITE+f"{rms} {erdep}{erlat}{erlon}"+Fore.RED+f" {fases}"+Fore.WHITE+f"{aut} {tip} {agen}    {comment}   {ubic}")
                            if ev[8] != None and np.isnan(ev[8]) == False and ev[1] not in suerr:
                                if int(ev[8]) >= 7  and ev[10] not in errsi and ev[10] != "explosion":
                                    if ut.test2((longitude,latitude),files_folder) == (True, 'Modelo_CARMA') and earthModel_type != "CARMA":
                                        conteo += 1
                                        comment= "Corregir modelo con CARMA".ljust(32," ")
                                        print(Fore.GREEN+f"{fe}"+Fore.WHITE+f" {ID} {dep}{mag}"+Fore.RED+f"{magnitude_type}"+Fore.WHITE+f"{rms} {erdep}{erlat}{erlon}"+Fore.RED+f" {fases}"+Fore.WHITE+f"{aut} {tip} {agen}    {comment}   {ubic}")
                            if ev[8] != None and np.isnan(ev[8]) == False and ev[1] not in suerr:
                                if int(ev[8]) >= 7  and ev[10] not in errsi and ev[10] != "explosion":
                                    if ut.test2((longitude,latitude),files_folder) == (True, 'Modelo_Cesar') and earthModel_type != "modelCesar2":
                                        conteo += 1
                                        comment= "Corregir modelo con Cesar".ljust(32," ")
                                        print(Fore.GREEN+f"{fe}"+Fore.WHITE+f" {ID} {dep}{mag}"+Fore.RED+f"{magnitude_type}"+Fore.WHITE+f"{rms} {erdep}{erlat}{erlon}"+Fore.RED+f" {fases}"+Fore.WHITE+f"{aut} {tip} {agen}    {comment}   {ubic}")

                            #if ev[8] != None and ev[1] not in suerr:
                            #    if int(ev[8]) <= 7  and ev[10] in errfa:
                            #            #conteo += 1
                            #            conteore += 1
                            #            errfases += f"{fe} {ID} {dep}{mag}{magnitude_type}"+Fore.RED+f"{rms}"+f" {erdep}{erlat}{erlon} {fases}{aut} {tip} {agen}\n"
                            #            comment= "Eventos con 7 o menos fases".ljust(32," ")
                            #            print(Fore.GREEN+f"{fe}"+Fore.WHITE+f" {ID} {dep}{mag}{magnitude_type}{method_type}{earthModel_type}{rms} {erdep}{erlat}{erlon}"+Fore.RED+f" {fases}"+Fore.WHITE+f"{aut} {tip} {agen}    {comment}   {ubic}")     

                            if ev[1] not in suerr and ev[10] != "not existing" and ev[9] in autors:
                                    if ev[11] == "SGC":
                                        suerr.append(ID)
                                        conteo += 1
                                        errevpic+= f"{fe} {ID} {dep}{mag}{magnitude_type}"+Fore.RED+f"{rms}"+f" {erdep}{erlat}{erlon} {fases}{aut} {tip} {agen}\n"
                                        comment= "Evento por picar o asociar".ljust(32," ")
                                        print(Fore.GREEN+f"{fe}"+Fore.WHITE+f" {ID} {dep}{mag}{magnitude_type}{rms} {erdep}{erlat}{erlon} {fases}"+Fore.RED+f"{aut}"+Fore.WHITE+f" {tip} {agen}    {comment}   {ubic}")

                            if ev[10] != None and ev[3] != None:
                                    if ev[10] == "outside of network interest" or ev[9] == "scautoloc_reg" or ev[9] == "scanloc":
                                        if ev[11] == "SGC" and float(ev[3]) >= 5 and ev[1] not in suerr and ev[10] != "not existing":
                                           suerr.append(ID)
                                           conteo += 1
                                           errevint  +=f"{fe} {ID} {dep}{mag}{magnitude_type}"+Fore.RED+f"{rms}"+f" {erdep}{erlat}{erlon} {fases}{aut} {tip} {agen}\n"
                                           comment= "Evento internacional sin asociar".ljust(32," ")
                                           print(Fore.GREEN+f"{fe}"+Fore.WHITE+f" {ID} {dep}"+Fore.RED+f"{mag}"+Fore.WHITE+f"{rms} {erdep}{erlat}{erlon} {fases}{aut}"+Fore.RED+f" {tip} {agen}"+Fore.WHITE+f"    {comment}") 

                                    if ev[10] not in errloc and ev[11] == "SGC" and ev[3] != None:
                                            if float(ev[3]) >= 4 and ev[1] not in IDs:
                                                conteo += 1
                                                comment= "Evento DESTACADO por M>4, sin etiqueta".ljust(32," ")
                                                print(Fore.GREEN+f"{fe}"+Fore.WHITE+f" {ID} {dep}"+Fore.RED+f"{mag}{magnitude_type}"+Fore.WHITE+f"{rms} {erdep}{erlat}{erlon} {fases}{aut} {tip} {agen}    "+Fore.RED+f"{comment}   {ubic}")
                                    
                                    if ev[1] in IDsen and ev[1] not in IDs:
                                            conteo +=1
                                            comment = f"Evento DESTACADO con {feel[ev[1]]} sentidos, sin etiqueta".ljust(32," ")
                                            print(Fore.GREEN+f"{fe}"+Fore.WHITE+f" {ID} {dep}"+Fore.RED+f"{mag}{magnitude_type}"+Fore.WHITE+f"{rms} {erdep}{erlat}{erlon} {fases}{aut} {tip} {agen}    "+Fore.RED+f"{comment}   {ubic}")

                            if float(latitude)>0.6 and float(latitude) < 1.8 and float(longitude) > -78.2 and float(longitude) < -76.6 and ev[11] == "SGC" and ev[10] not in errvol:
                                    if ut.inside_bna_polygon((longitude,latitude),volcanic_bna_folder) == True:
                                        conteo +=1
                                        comment = "Volcanico sin etiqueta -not locat."
                                        print(Fore.GREEN+f"{fe}"+Fore.WHITE+f" {ID} {dep}{mag}{magnitude_type}{rms} {erdep}{erlat}{erlon} {fases}{aut}"+Fore.RED+f" {tip} "+Fore.WHITE+f"{agen}    "+Fore.RED+f"{comment}"+Fore.WHITE+f"   {ubic}")
                            if ev[10] == "volcanic eruption" and (ev[1] not in IDs):
                                    if ut.inside_bna_polygon((longitude,latitude),volcanic_bna_folder) == True:
                                        conteo +=1
                                        comment = "Volcanico sin comm. -DESTACADO o sin etiqueta -not locat."
                                        print(Fore.GREEN+f"{fe}"+Fore.WHITE+f" {ID} {dep}{mag}{rms} {erdep}{erlat}{erlon} {fases}{aut}"+Fore.RED+f" {tip} "+Fore.WHITE+f"{agen}    "+Fore.RED+f"{comment}"+Fore.WHITE+f"   {ubic}")
                            if float(latitude)>3 and float(latitude) < 7.5  and float(longitude) < -77.2 and ev[10] not in errloc:
                                if float(ev[2]) > 30:
                                    conteo +=1
                                    comment = "Evento del Pacifico muy profundo"
                                    print(Fore.GREEN+f"{fe}"+Fore.WHITE+f" {ID} "+Fore.RED+f"{dep}"+Fore.WHITE+f"{mag}{magnitude_type}{rms} {erdep}{erlat}{erlon} {fases}{aut} {tip} {agen}    "+Fore.RED+f"{comment}"+Fore.WHITE+f"  {ubic}")             

        else:
            if ev[4] != None:
                    if float(ev[4]) >= 1.51 and ev[10] not in errloc and ev[1] not in suerr:        
                            errms += f"{fe} {ID} {dep}{mag}{magnitude_type}"+Fore.RED+f"{rms}"+f" {erdep}{erlat}{erlon} {fases}{aut} {tip} {agen}\n"  
                            suerr.append(ID)
                            conteo += 1
                            comment= "RMS alto".ljust(32," ")
                            print(Fore.GREEN+f"{fe}"+Fore.WHITE+f" {ID} {dep}{mag}{magnitude_type}"+Fore.RED+f"{rms}"+Fore.WHITE+f" {erdep}{erlat}{erlon} {fases}{aut} {tip} {agen}    {comment}   {ubic}")
                    if ev[6] != None and ev[7] != None and ev[5] != None and ev[1] not in suerr and str(ev[10]) not in errloc:
                            if float(ev[6]) >= 12.0 or float(ev[7]) >= 12.0 or float(ev[5]) >=12.0:
                                suerr.append(ID)
                                conteo += 1
                                errloc += f"{fe} {ID} {dep}{mag}{magnitude_type}"+Fore.RED+f"{rms}"+f" {erdep}{erlat}{erlon} {fases}{aut} {tip} {agen}\n"
                                comment= "Errores de localizacion alto".ljust(32," ")
                                print(Fore.GREEN+f"{fe}"+Fore.WHITE+f" {ID} {dep}{mag}{magnitude_type}{rms}"+Fore.RED+f" {erdep}{erlat}{erlon}"+Fore.WHITE+f" {fases}{aut} {tip} {agen}    {comment}   {ubic}")
                    
                    if ev[8] != None and np.isnan(ev[8]) == False and ev[1] not in suerr:
                            if int(ev[8]) >= 12  and ev[10] in errfa:
                                if ut.inside_bna_polygon((longitude,latitude),volcanic_bna_folder) != True:
                                    print("ACAAA",ut.inside_bna_polygon((longitude,latitude),volcanic_bna_folder))
                                    conteo += 1
                                    errfases += f"{fe} {ID} {dep}{mag}{magnitude_type}"+Fore.RED+f"{rms}"+f" {erdep}{erlat}{erlon} {fases}{aut} {tip} {agen}\n"
                                    comment= "Evento localizable".ljust(32," ")
                                    print(Fore.GREEN+f"{fe}"+Fore.WHITE+f" {ID} {dep}{mag}{magnitude_type}{rms} {erdep}{erlat}{erlon}"+Fore.RED+f" {fases}"+Fore.WHITE+f"{aut} {tip} {agen}    {comment}   {ubic}")
                    if ev[8] != None and np.isnan(ev[8]) == False and ev[1] not in suerr:
                            if int(ev[8]) >= 7  and ev[10] not in errsi:
                                if ut.test2((longitude,latitude),files_folder) == (True, 'zona1') and magnitude_type != mag1:
                                    conteo += 1
                                    comment= "Corregir magnitud con MLr_1".ljust(32," ")
                                    print(Fore.GREEN+f"{fe}"+Fore.WHITE+f" {ID} {dep}{mag}"+Fore.RED+f"{magnitude_type}"+Fore.WHITE+f"{rms} {erdep}{erlat}{erlon}"+Fore.RED+f" {fases}"+Fore.WHITE+f"{aut} {tip} {agen}    {comment}   {ubic}")
                    if ev[8] != None and np.isnan(ev[8]) == False and ev[1] not in suerr:
                            if int(ev[8]) >= 7  and ev[10] not in errsi:
                                if ut.test2((longitude,latitude),files_folder) == (True, 'zona2') and magnitude_type != mag2:
                                    conteo += 1
                                    comment= "Corregir magnitud con MLr_2".ljust(32," ")
                                    print(Fore.GREEN+f"{fe}"+Fore.WHITE+f" {ID} {dep}{mag}"+Fore.RED+f"{magnitude_type}"+Fore.WHITE+f"{rms} {erdep}{erlat}{erlon}"+Fore.RED+f" {fases}"+Fore.WHITE+f"{aut} {tip} {agen}    {comment}   {ubic}")
                    if ev[8] != None and np.isnan(ev[8]) == False and ev[1] not in suerr:
                            if int(ev[8]) >= 7  and ev[10] not in errsi:
                                if ut.test2((longitude,latitude),files_folder) == (True, 'zona3') and magnitude_type != mag3:
                                    conteo += 1
                                    comment= "Corregir magnitud con MLr_3".ljust(32," ")
                                    print(Fore.GREEN+f"{fe}"+Fore.WHITE+f" {ID} {dep}{mag}"+Fore.RED+f"{magnitude_type}"+Fore.WHITE+f"{rms} {erdep}{erlat}{erlon}"+Fore.RED+f" {fases}"+Fore.WHITE+f"{aut} {tip} {agen}    {comment}   {ubic}")
                    if ev[8] != None and np.isnan(ev[8]) == False and ev[1] not in suerr:
                            if int(ev[8]) >= 7  and ev[10] not in errsi:
                                if ut.test2((longitude,latitude),files_folder) == (True, 'zona4') and magnitude_type != mag4:
                                    conteo += 1
                                    comment= "Corregir magnitud con MLr_4".ljust(32," ")
                                    print(Fore.GREEN+f"{fe}"+Fore.WHITE+f" {ID} {dep}{mag}"+Fore.RED+f"{magnitude_type}"+Fore.WHITE+f"{rms} {erdep}{erlat}{erlon}"+Fore.RED+f" {fases}"+Fore.WHITE+f"{aut} {tip} {agen}    {comment}   {ubic}")
                    if ev[8] != None and np.isnan(ev[8]) == False and ev[1] not in suerr:
                            if int(ev[8]) >= 7  and ev[10] not in errsi:
                                if ut.test2((longitude,latitude),files_folder) == (True, 'zona5') and magnitude_type != mag5:
                                    conteo += 1
                                    comment= "Corregir magnitud con MLr_5".ljust(32," ")
                                    print(Fore.GREEN+f"{fe}"+Fore.WHITE+f" {ID} {dep}{mag}"+Fore.RED+f"{magnitude_type}"+Fore.WHITE+f"{rms} {erdep}{erlat}{erlon}"+Fore.RED+f" {fases}"+Fore.WHITE+f"{aut} {tip} {agen}    {comment}   {ubic}")
                    if ev[8] != None and np.isnan(ev[8]) == False and ev[1] not in suerr:
                            if int(ev[8]) >= 7  and ev[10] not in errsi:
                                if ut.test2((longitude,latitude),files_folder) == (True, 'zona_vmm') and magnitude_type != magvmm:
                                    conteo += 1
                                    comment= "Corregir magnitud con MLr_vmm".ljust(32," ")
                                    print(Fore.GREEN+f"{fe}"+Fore.WHITE+f" {ID} {dep}{mag}"+Fore.RED+f"{magnitude_type}"+Fore.WHITE+f"{rms} {erdep}{erlat}{erlon}"+Fore.RED+f" {fases}"+Fore.WHITE+f"{aut} {tip} {agen}    {comment}   {ubic}")
                    if ev[8] != None and np.isnan(ev[8]) == False and ev[1] not in suerr:
                            if int(ev[8]) >= 7  and ev[10] not in errsi:
                                if ut.test2((longitude,latitude),files_folder) == (True, 'zona_vmm') and earthModel_type != "VMM":
                                    conteo += 1
                                    comment= "Corregir modelo con VMM".ljust(32," ")
                                    print(Fore.GREEN+f"{fe}"+Fore.WHITE+f" {ID} {dep}{mag}"+Fore.RED+f"{magnitude_type}"+Fore.WHITE+f"{rms} {erdep}{erlat}{erlon}"+Fore.RED+f" {fases}"+Fore.WHITE+f"{aut} {tip} {agen}    {comment}   {ubic}")
                    if ev[8] != None and np.isnan(ev[8]) == False and ev[1] not in suerr:
                            if int(ev[8]) >= 7  and ev[10] not in errsi and ev[10] != "explosion":
                                if ut.test2((longitude,latitude),files_folder) == (True, 'Modelo_CARMA') and earthModel_type != "CARMA":
                                    conteo += 1
                                    comment= "Corregir modelo con CARMA".ljust(32," ")
                                    print(Fore.GREEN+f"{fe}"+Fore.WHITE+f" {ID} {dep}{mag}"+Fore.RED+f"{magnitude_type}"+Fore.WHITE+f"{rms} {erdep}{erlat}{erlon}"+Fore.RED+f" {fases}"+Fore.WHITE+f"{aut} {tip} {agen}    {comment}   {ubic}")
                    if ev[8] != None and np.isnan(ev[8]) == False and ev[1] not in suerr:
                            if int(ev[8]) >= 7  and ev[10] not in errsi and ev[10] != "explosion":
                                if ut.test2((longitude,latitude),files_folder) == (True, 'Modelo_Cesar') and earthModel_type != "modelCesar2":
                                    conteo += 1
                                    comment= "Corregir modelo con Cesar".ljust(32," ")
                                    print(Fore.GREEN+f"{fe}"+Fore.WHITE+f" {ID} {dep}{mag}"+Fore.RED+f"{magnitude_type}"+Fore.WHITE+f"{rms} {erdep}{erlat}{erlon}"+Fore.RED+f" {fases}"+Fore.WHITE+f"{aut} {tip} {agen}    {comment}   {ubic}")

                    #if ev[8] != None and ev[1] not in suerr:
                    #        if int(ev[8]) <= 7  and ev[10] in errfa:
                    #            #conteo += 1
                    #            conteore += 1
                    #            errfases += f"{fe} {ID} {dep}{mag}{magnitude_type}"+Fore.RED+f"{rms}"+f" {erdep}{erlat}{erlon} {fases}{aut} {tip} {agen}\n"
                    #            comment= "Eventos con 7 o menos fases".ljust(32," ")
                    #            print(Fore.GREEN+f"{fe}"+Fore.WHITE+f" {ID} {dep}{mag}{magnitude_type}{rms} {erdep}{erlat}{erlon}"+Fore.RED+f" {fases}"+Fore.WHITE+f"{aut} {tip} {agen}    {comment}   {ubic}")     
                    if ev[1] not in suerr and ev[10] != "not existing" and ev[9] in autors:
                            if ev[11] == "SGC":
                                suerr.append(ID)
                                conteo += 1
                                errevpic+= f"{fe} {ID} {dep}{mag}{magnitude_type}"+Fore.RED+f"{rms}"+f" {erdep}{erlat}{erlon} {fases}{aut} {tip} {agen}\n"
                                comment= "Evento por picar o asociar".ljust(32," ")
                                print(Fore.GREEN+f"{fe}"+Fore.WHITE+f" {ID} {dep}{mag}{magnitude_type}{rms} {erdep}{erlat}{erlon} {fases}"+Fore.RED+f"{aut}"+Fore.WHITE+f" {tip} {agen}    {comment}   {ubic}")
                    if ev[10] != None and ev[3] != None:
                            if ev[10] == "outside of network interest" or ev[9] == "scautoloc_reg" or ev[9] == "scanloc":
                                    if ev[11] == "SGC" and float(ev[3]) >= 5 and ev[1] not in suerr and ev[10] != "not existing":
                                        suerr.append(ID)
                                        conteo += 1
                                        errevint  +=f"{fe} {ID} {dep}{mag}{magnitude_type}"+Fore.RED+f"{rms}"+f" {erdep}{erlat}{erlon} {fases}{aut} {tip} {agen}\n"
                                        comment= "Evento internacional sin asociar".ljust(32," ")
                                        print(Fore.GREEN+f"{fe}"+Fore.WHITE+f" {ID} {dep}"+Fore.RED+f"{mag}"+Fore.WHITE+f"{magnitude_type}{rms} {erdep}{erlat}{erlon} {fases}{aut}"+Fore.RED+f" {tip} {agen}"+Fore.WHITE+f"    {comment}")
                            if ev[10] not in errloc and ev[11] == "SGC" and ev[3] != None:
                                    if float(ev[3]) >= 4 and ev[1] not in IDs:
                                        conteo += 1
                                        comment= "Evento DESTACADO por M>4, sin etiqueta".ljust(32," ")
                                        print(Fore.GREEN+f"{fe}"+Fore.WHITE+f" {ID} {dep}"+Fore.RED+f"{mag}{magnitude_type}"+Fore.WHITE+f"{rms} {erdep}{erlat}{erlon} {fases}{aut} {tip} {agen}    "+Fore.RED+f"{comment}"+Fore.WHITE+f"   {ubic}") 
                            if ev[1] in IDsen and ev[1] not in IDs:
                                    conteo +=1
                                    comment = f"Evento DESTACADO con {feel[ev[1]]} sentidos, sin etiqueta".ljust(32," ")
                                    print(Fore.GREEN+f"{fe}"+Fore.WHITE+f" {ID} {dep}{mag}{magnitude_type}{rms} {erdep}{erlat}{erlon} {fases}{aut} {tip} {agen}    "+Fore.RED+f"{comment}"+Fore.WHITE+f"   {ubic}") 
                    if float(latitude)>0.6 and float(latitude) < 1.8 and float(longitude) > -78.2 and float(longitude) < -76.6 and ev[11] == "SGC" and ev[10] not in errvol:
                            if ut.inside_bna_polygon((longitude,latitude),volcanic_bna_folder) == True:
                                conteo +=1
                                comment = "Volcanico sin etiqueta -not locat."
                                print(Fore.GREEN+f"{fe}"+Fore.WHITE+f" {ID} {dep}{mag}{magnitude_type}{rms} {erdep}{erlat}{erlon} {fases}{aut}"+Fore.RED+f" {tip} "+Fore.WHITE+f"{agen}    "+Fore.RED+f"{comment}"+Fore.WHITE+f"   {ubic}") 
                    if ev[10] == "volcanic eruption" and (ev[1] not in IDs):
                            if ut.inside_bna_polygon((longitude,latitude),volcanic_bna_folder) == True:
                                conteo +=1
                                comment = "Volcanico sin comm. -DESTACADO o sin etiqueta -not locat."
                                print(Fore.GREEN+f"{fe}"+Fore.WHITE+f" {ID} {dep}{mag}{rms} {erdep}{erlat}{erlon} {fases}{aut}"+Fore.RED+f" {tip} "+Fore.WHITE+f"{agen}    "+Fore.RED+f"{comment}"+Fore.WHITE+f"  {ubic}")
                                
                    if float(latitude)>3 and float(latitude) < 7.5  and float(longitude) < -77.2 and ev[10] not in errloc:
                            if float(ev[2]) > 30:
                                conteo +=1
                                comment = "Evento del Pacifico muy profundo"
                                print(Fore.GREEN+f"{fe}"+Fore.WHITE+f" {ID} "+Fore.RED+f"{dep}"+Fore.WHITE+f"{mag}{magnitude_type}{rms} {erdep}{erlat}{erlon} {fases}{aut} {tip} {agen}    "+Fore.RED+f"{comment}"+Fore.WHITE+f"  {ubic}") 
    print(Fore.WHITE+f"\n\t Total de eventos con errores  {conteo} \n\t Total de eventos por revisar  {conteore}")

def run(starttime,endtime,user):
    
    df_sentido = connect2mysql("sentido",starttime,endtime)
    df_destacado = connect2mysql("destacado",starttime,endtime)
    df_normal = connect2mysql("normal",starttime,endtime)

    IDs = destacado_process(df_destacado)
    IDsen = sentido_process(df_sentido)
    normal_process(df_normal,IDs,IDsen,user)

def run_generator(starttime,endtime,user):
    run(starttime,endtime,user)

    update_condition=""

    while update_condition != "Y" and update_condition != "N":
        print(Fore.GREEN +"\n------------------------------------------------------------------------------------------------------------------------------------------------------")
        print("Desea actualizar?", Fore.GREEN + "Y","(si)","    ",Fore.RED + "N","(no)     ")
        update_condition= input().upper()

        if update_condition == "Y":
            os.system('cls' if os.name == 'nt' else 'clear')

            print("\nFecha y hora inicial YYYYMMDD HHMMSS   :  ",starttime.strftime("%Y%m%d %H%M%S"))
            print("Fecha y hora final   YYYYMMDD HHMMSS   :  ",endtime.strftime("%Y%m%d %H%M%S"))

            run(starttime,endtime,user)
            update_condition="" 
        else:
            pass

def read_args():
    prefix = "+"
    ini_msg = "#"*120
 

    parser = argparse.ArgumentParser("Revisi  n de sismicidad. ",prefix_chars=prefix)

    parser.add_argument(prefix+"s",prefix*2+"start",
                        default=None,
                        type=str,
                        metavar='',
                        help="Fecha inicial en formato yyyymmddThhmmss", required = True)

    parser.add_argument(prefix+"e",prefix*2+"end",
                        default=None,
                        type=str,
                        metavar='',
                        help="Fecha final en formato yyyymmddThhmmss", required = True)

    parser.add_argument(prefix+"u",prefix*2+"user",
                        default=None,
                        type=str,
                        metavar='',
                        help="Digitar el nombre del usuario")

    parser.add_argument(prefix+"o",prefix*2+"output",
                        default=None,
                        type=bool,
                        metavar='',
                        help="True si vas a guardar. Adicionar '> archivo.txt'")

    args = parser.parse_args()
    vars_args = vars(args)
    return vars_args

if __name__ == "__main__":
    args = read_args()

    starttime = dt.datetime.strptime(args['start'],"%Y%m%dT%H%M%S")
    endtime = dt.datetime.strptime(args['end'],"%Y%m%dT%H%M%S")
    user = args["user"]
    output = args["output"]

    if output:
        run(starttime,endtime,user)
    else:
        run_generator(starttime,endtime,user)

