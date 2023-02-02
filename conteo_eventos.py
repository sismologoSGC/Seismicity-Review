#!/usr/bin/python3
# coding=utf-8*

"""
Angel Agudelo  <adagudelo@sgc.gov.co> (enero de 2022)
(modificado enero 2023)
Versión en python3
"""

import MySQLdb
import datetime
import os 
import pandas as pd
from colorama import init, Fore, Back


#Parametros de entrada
Fini, Hini = input("\n\tFecha inicial YYYYMMDD HHMMSS:  ").split()
Ffin, Hfin = input("\n\tFecha final   YYYYMMDD HHMMSS:  ").split()
consulta = input("""
    ________________
    digite la letra del tipo de consulta
         (t) todo el registro
         (c) cuadrante
    ________________\n\t:        """)

if consulta == "c":
	print("##Consulta cuadrante")
	loc = input("\n\t Lat_min Lat_max Long_min Long_max (3.54 4.5 -74.5 -73.12): ").strip(")").strip("(").split()

elif consulta == "t":
	loc = "todo"
else:
	print("\n\t LA LETRA INGRESADA NO ES CORRECTA, DIGITE t O c")
	sys.exit()



start_date = Fini[0:4]+"/"+Fini[4:6]+"/"+Fini[6:8]+" "+Hini[0:2]+":"+Hini[2:4]+":"+Hini[4:6]
end_date = Ffin[0:4]+"/"+Ffin[4:6]+"/"+Ffin[6:8]+" "+Hfin[0:2]+":"+Hfin[2:4]+":"+Hfin[4:6]


def read_data(l = ["","",""]):
	l1, l2, l3 = l[0],l[1],l[2]
	
	codex2 = f"Select \
					DATE_FORMAT(Origin.time_value, '%Y/%m/%d') AS 'FECHA', \
					DATE_FORMAT(Origin.time_value, '%H:%i:%S') AS 'HORA UTC', \
					Magnitude.magnitude_value,\
					Magnitude.type AS 'Tipo Magnitud', \
					Origin.quality_usedPhaseCount AS 'Numero Fases Usadas', \
					Origin.latitude_value,\
					Origin.longitude_value,\
					Origin.depth_value, \
					ROUND(Origin.depth_uncertainty,2) AS 'Error_Profundidad', \
					ROUND(Origin.quality_standardError,2) AS 'RMS', \
					Origin.evaluationMode AS 'Estatus', \
					Origin.creationInfo_agencyID AS 'Agencia', \
					Origin.creationInfo_author AS 'Author', \
					POEv.publicID AS 'ID', \
					DATE_FORMAT(Origin._last_modified, '%Y/%m/%d %H:%i:%S') AS 'MODIF', \
					EventDescription.text AS 'Region', \
					AreaOfInfluence.area, \
					Event.type{l1}\
				from \
					Event AS EvMF left join PublicObject AS POEv ON EvMF._oid = POEv._oid\
					left join AreaOfInfluence on AreaOfInfluence.publicId = POEv.publicID\
					left join PublicObject as POOri ON EvMF.preferredOriginID=POOri.publicID  \
					left join Origin ON POOri._oid=Origin._oid left join PublicObject as POMag on EvMF.preferredMagnitudeID=POMag.publicID  \
					left join Magnitude ON Magnitude._oid = POMag._oid  \
					left join Event ON Event._oid= POEv._oid \
					left join EventDescription ON EvMF._oid = EventDescription._parent_oid{l2}\
				where \
					Origin.time_value BETWEEN '{start_date}' AND '{end_date}'{l3}\
					ORDER BY Origin.time_value	"
    
	#Origin.time_value BETWEEN '{start_date}' AND '{end_date}'
	#Origin._last_modified BETWEEN '{start_date}' AND '{end_date}'
	#hos = "172.25.3.135"
	hos = "10.100.100.232"
	db = MySQLdb.connect(host=hos, user="consulta", passwd="consulta", db="seiscomp3")
	cur = db.cursor()
	cur.execute(codex2)
	
	lista_fecha = []
	lista_hora = []
	lista_magn = []
	lista_radio = []
	lista_lat = []
	lista_lon = []
	lista_prof = []
	lista_autor = []
	lista_region = []
	lista_color = []
	lista_tipo = []
	lista_id = []
	no_existente = 0
	
	for ev in cur.fetchall():

		if ev[17] != "not existing":
			#if ev[12][0:9] == "AI_picker":
			fecha= ev[0]
			hora=  ev[1]
			
			if ev[2] != None:                          #Magnitud
				magn = round(ev[2],3)
				e_magn = int(magn)
			if ev[2] == None:
				magn = 0
				e_magn = 0
			
			
			lat=   ev[5]
			lon=   ev[6]
			prof=  ev[7]
			#e_prof= int(prof)
			autor = ev[12]
			id = ev[13]
			region= ev[15]
			tipo = ev[17]

			if len(l[0]) > 0:  #len: ver tamaño 
				destacado = ev[18]
			

			#Radio magnitud
			#m_r = {"m0":14/4,"m1":14/4,"m2":16/4,"m3":18.82/4,"m4":19.72/4,"m5":21.62/4,"m6":22.52/4,"m7":27.24/4,"m8":29.32/4}
			#radio = m_r[f"m{e_magn}"]
			
			#color RGB profundidad 
			#if e_prof >= 0 and e_prof <= 30: color=(255,0,0)
			#if e_prof >= 31 and e_prof <= 70: color=(235,235,0)
			#if e_prof >= 71 and e_prof <= 120: color=(0,255,0)
			#if e_prof >= 121 and e_prof <= 180: color=(0,0,255)
			#if e_prof >= 180: color=(200,0,246) 
			
			lista_fecha.append(fecha)
			lista_hora.append(hora)
			lista_magn.append(magn)
			#lista_radio.append(radio)
			lista_lat.append(lat)
			lista_lon.append(lon)
			lista_prof.append(prof)
			lista_autor.append(autor)
			#lista_color.append(color)
			lista_region.append(region)
			lista_tipo.append(tipo)
			lista_id.append(id)
		
		else:
			no_existente += 1	
			#print(fecha, hora, magn, radio, lat, lon, prof, color, region)
	
	print(no_existente)
	#Diccionario vacio
	Conteo={}
	#Conteo diccionario con cada lista
	Conteo["Fecha"]=lista_fecha
	Conteo["Origen"]=lista_hora
	Conteo["Magnitud"]=lista_magn
	#Conteo["RadioMag"]=lista_radio
	Conteo['Latitud']=lista_lat
	Conteo['Longitud']=lista_lon
	Conteo['Profundidad']=lista_prof
	Conteo['Autor'] = lista_autor
	#Conteo['RGBProf']=lista_color
	Conteo["Region"]=lista_region
	Conteo["Tipo"]=lista_tipo
	Conteo["ID"] = lista_id
	
	return Conteo		




dest = [", Comment.text",", Comment","AND Comment._parent_oid = Event._oid AND (Comment.text LIKE '%DESTACADO%' or Comment.text LIKE '%destacado%')"]
#dest = [", Comment.text",", Comment","and Comment.text LIKE '%DESTACADO%'"]

rect = ["","",f"AND Origin.latitude_value between {loc[0]} and {loc[1]} AND Origin.longitude_value between {loc[2]} AND {loc[3]} "]

file_name = f'eventos{Fini}_{Ffin}.csv'

if consulta == "c":
	data=read_data(rect)
	df = pd.DataFrame(data)
	df.index+=1
	df.index.name= 'N°'
	df.to_csv(file_name)

if consulta == "t":
	data=read_data()
	df = pd.DataFrame(data)
	df.index+=1
	df.index.name= 'N°'
	df.to_csv(file_name)    


data_destacados=read_data(dest)
df_destacado = pd.DataFrame(data_destacados)
df_destacado.index+=1
df_destacado.index.name= 'N°'
df_destacado.to_csv(f'eventos_destacados{Fini}_{Ffin}.csv')

nolocatable = len(df[ (df["Tipo"]=='not locatable')])
#c_a_uno = df[ (df['Magnitud']>=0) & (df['Magnitud']<1) & (df["Tipo"] != "explosion") & (df["Tipo"] != "not locatable") & (df["Tipo"] != 'outside of network interest') ]
#print(c_a_uno)
Numero_M_cero_y_uno= len( df[ (df['Magnitud']>=0) & (df['Magnitud']<1) & (df["Tipo"] != "explosion") & (df["Tipo"] != "not existing") & (df["Tipo"] != "not locatable") & (df["Tipo"] != 'outside of network interest') ] )  #[0,1)
Numero_M_uno_y_dos= len( df[ (df['Magnitud']>=1) & (df['Magnitud']<2) & (df["Tipo"] != "explosion") & (df["Tipo"] != "not existing") & (df["Tipo"] != "not locatable") & (df["Tipo"] != 'outside of network interest')] )  #[1,2)
Numero_M_dos_y_tres= len( df[ (df['Magnitud']>=2) & (df['Magnitud']<3) & (df["Tipo"] != "explosion") & (df["Tipo"] != "not existing") & (df["Tipo"] != "not locatable") & (df["Tipo"] != 'outside of network interest')] )  #[2,3)
Numero_M_tres_y_cuatro= len( df[ (df['Magnitud']>=3) & (df['Magnitud']<4) & (df["Tipo"] != "explosion") & (df["Tipo"] != "not existing") & (df["Tipo"] != "not locatable") & (df["Tipo"] != 'outside of network interest')] )  #[3,4)
Numero_M_cuatro_y_cinco= len( df[ (df['Magnitud']>=4) & (df['Magnitud']<5) & (df["Tipo"] != "explosion") & (df["Tipo"] != "not existing") & (df["Tipo"] != "not locatable") & (df["Tipo"] != 'outside of network interest')] )  #[4,5)
Numero_M_cinco_y_seis= len( df[ (df['Magnitud']>=5) & (df['Magnitud']<6) & (df["Tipo"] != "explosion") & (df["Tipo"] != "not existing") & (df["Tipo"] != "not locatable") & (df["Tipo"] != 'outside of network interest')] ) #[5,6)
Numero_M_mayor_a_seis= len( df[ (df['Magnitud']>=6) & (df["Tipo"] != "explosion") & (df["Tipo"] != "not existing") & (df["Tipo"] != "not locatable") & (df["Tipo"] != 'outside of network interest')]) #>6 
#tot1 = len( df[ (df["Tipo"] == "earthquake")])
#tot2 = len( df[ (df["Tipo"] == None)])
#total_eventos2 = tot1 + tot2
total_eventos = len( df[ (df['Magnitud']>=0) & (df["Tipo"] != "explosion") & (df["Tipo"] != "not existing") & (df["Tipo"] != "not locatable") & (df["Tipo"] != 'outside of network interest') ] )
#total_eventos = len( df[ (df['Magnitud']>=0) & (df["Tipo"] != "not existing") & (df["Tipo"] != "not locatable")] )

Total_destacados= len( df_destacado[(df_destacado["Tipo"] != 'outside of network interest')]) #Destacados
total_destacados_I = len( df_destacado[(df_destacado["Tipo"] == 'outside of network interest')]) #Destacados
internacionales = len(df[ (df["Tipo"] == "outside of network interest")])
explociones = len(df[ (df["Tipo"] == "explosion")])

print(Fore.RED  +"\n\tFecha inicial (UTC) ", start_date )
print(Fore.RED  +"\tFecha final   (UTC) ", end_date )
#print("total de eventos 2  ",  total_eventos2)
print(Fore.RED  +'\n\tTotal eventos localizados nacional  --> ',total_eventos) 
print(Fore.RED  +'\tTotaleventos >=2                      --> ', len(df[ (df['Magnitud']>=2) & (df["Tipo"] != "not existing") & (df["Tipo"] != "explosion") &  (df["Tipo"] != "not locatable") & (df["Tipo"] != 'outside of network interest')])) 
print(Fore.RED  +'\tTotaleventos Intern.                  --> ', len(df[ (df["Tipo"] == "outside of network interest")])) 
print(Fore.RED  +'\n\tNo localizables                     --> ',nolocatable)
print(Fore.RED  +'\tNo Explociones                      --> ',explociones)

print(Fore.RED  +'\tTotal eventos localizados nacionales y no localizables  --> ',total_eventos + nolocatable)


salida = f"""  
           Magnitud  [0,1)  -->      {Numero_M_cero_y_uno}
           Magnitud  [1,2)  -->      {Numero_M_uno_y_dos}
           Magnitud  [2,3)  -->      {Numero_M_dos_y_tres}
           Magnitud  [3,4)  -->      {Numero_M_tres_y_cuatro}
           Magnitud  [4,5)  -->      {Numero_M_cuatro_y_cinco}
           Magnitud  [5,6)  -->      {Numero_M_cinco_y_seis}
           Magnitud  =>6    -->      {Numero_M_mayor_a_seis}
           Destacados N     -->      {Total_destacados}
           Destacados I     -->      {total_destacados_I}
 """
 
print(Fore.RED  +salida)
print(Fore.GREEN +f"\n\tSe creo la tabla {file_name} con los eventos de la consulta")
#print(df.groupby("Autor")["ID"].nunique())

init(autoreset=True)



