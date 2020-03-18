'''

#--------------------------------------------------------LIBRERIAS BASICAS DE PYTHON Y DE PYSPARK-----------------------------------------------------------------------

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
sns.set() 
from time import *

from pyspark.sql import HiveContext 
from pyspark.sql.functions import *
import ConfigParser
from pyspark import SparkContext, SparkConf


#-------------------------------------------------------DEFINIENDO: SparkConf, SparkContext y HiveContext-------------------------------------------------------------
# ENCABEZADO PARA CORRER EL PROGRAMA EN CLOUDERA:
conf=SparkConf().setAppName("Machine_Learning_ORCs")
sc=SparkContext.getOrCreate(conf=conf)
sqlContext=HiveContext(sc)
# Versión de PySpark sobre la que se va a ajecutar el proyecto
print "Versión de PySpark: ", sc.version


#-------------------------------------------------------SELECCIONANDO LA TABLA OBJETIVO: dwhdes.data_ctes_activos_em-------------------------------------------------------------
# Tabla a seleccionar: dwhdes.data_ctes_activos_em con 79,784,766 registros.
print '*******************************************************TABLA ORIGINAL************************************************************'
    
data=sqlContext.sql("SELECT * FROM dwhdes.data_ctes_activos_em")
data.show()


print "Número de registros: ", data.count()
# Imprime el esquema y el tipo de dato
print data.printSchema()
#print data.dtypes

# Imprime las columnas
print "\n"
# print data.columns


#------------------------------------------------SELECCIONANDO VARIIABLES DE INTERES DE LA TABLA OBJETIVO: dwhdes.data_ctes_activos_em-------------------------------------------------------------
df=data.select('edad', 'sexo', 'estado', 'nacionalidad', 'antiguedad_cliente',\
'ing_mensual', 'capacidad_pago', 'estado_civil', 'credito_en_mora', 'monto_del_credito', \
'duracion_credito', 'nivel_endeudamiento','aval', 'cuenta_ahorro', 'creditos_pagados', 'creditos_no_pagados', \
"semanaproceso", 'historial_crediticio')


# ---------------------------------------------------------CAMBIANDO TIPO DE DATO------------------------------------------------------------------------------------------------
from pyspark.sql.types import *
print '*********************************************************CAMBIANDO TIPO DE DATO***********************************************************'
df = df.withColumn('edad', df.edad.cast(IntegerType()))
df = df.withColumn('sexo', df.sexo.cast(StringType()))
df = df.withColumn('estado', df.estado.cast(StringType()))
df = df.withColumn('nacionalidad', df.nacionalidad.cast(StringType()))
df = df.withColumn('antiguedad_cliente', df.antiguedad_cliente.cast(IntegerType()))
# df = df.withColumn('trabajo', df.trabajo.cast(StringType()))
df = df.withColumn('ing_mensual', df.ing_mensual.cast(DoubleType()))
df = df.withColumn('capacidad_pago', df.capacidad_pago.cast(DoubleType()))
df = df.withColumn('estado_civil', df.estado_civil.cast(StringType()))
df = df.withColumn('credito_en_mora', df.credito_en_mora.cast(StringType()))
df = df.withColumn('monto_del_credito', df.monto_del_credito.cast(DoubleType()))
df = df.withColumn('duracion_credito', df.duracion_credito.cast(IntegerType()))
df = df.withColumn('nivel_endeudamiento', df.nivel_endeudamiento.cast(DoubleType()))
df = df.withColumn('aval', df.aval.cast(StringType()))
df = df.withColumn('cuenta_ahorro', df.cuenta_ahorro.cast(StringType()))
df = df.withColumn('creditos_pagados', df.creditos_pagados.cast(IntegerType()))
df = df.withColumn('creditos_no_pagados', df.creditos_no_pagados.cast(IntegerType()))
df = df.withColumn('semanaproceso', df.semanaproceso.cast(IntegerType()))
df = df.withColumn('historial_crediticio', df.historial_crediticio.cast(StringType()))

df.printSchema()
#df.show()

#------------------------------------------------------------------FILTRANDO DEPENDIENDO DE LA SEMANA---------------------------------------------------------------
print '*********************************************************TABLA CON FLITRO semanaproceso***********************************************************'
semana=201952
df=df.filter(df.semanaproceso==semana)
df.show()

df=df.drop("semanaproceso")
df.show()
print "Número de registros de la semana " + str(semana) + ": ", df.count()

#----------------------------------------------------------------SEPARANDO VARIABLES NUMERICAS, CATEGORICAS Y OBJETIVO---------------------------------------------------------------

categoricalColumns=['sexo', 'estado', 'nacionalidad', 'estado_civil', 'credito_en_mora', 'aval', 'cuenta_ahorro']
numericCols=['edad', 'antiguedad_cliente', 'ing_mensual', 'capacidad_pago', 'monto_del_credito', 'duracion_credito', 'nivel_endeudamiento', 'creditos_pagados', 'creditos_no_pagados']
labelCol='historial_crediticio' # Target



# ----------------------------------------------------------------VALORES NULOS--------------------------------------------------------------------------------------------------
print '******************************************************************VALORES NULOS**************************************************************************'
from pyspark.sql import functions as F
from pyspark.sql.functions import col, sum
from pyspark.sql.types import *
df_null = df.select(*(F.sum(F.col(c).isNull().cast('Integer')).alias(c) for c in df.columns))
# si quieres pasarlo a pandas: df_null = data.select(*(F.sum(F.col(c).isNull().cast('Double')).alias(c) for c in data.columns)).toPandas()
df_null.show()

#------------------------------------------------------------------IMPUTANDO DATOS-----------------------------------------------------------------------------------------------
print '**********************************************************IMPUTANDO DATOS***********************************************************'
# Dato con valores nulos es ing_mensual: 264,411

mean_ing_mensual=data.agg({"ing_mensual":"avg"})
mean_ing_mensual.show()

df=df.fillna({"ing_mensual":1930.01})
df.show()

print '**********************************************************YA NO HAY VALORES***********************************************************'
# Comprobando que ya no hay datos nulos
df_null = df.select(*(F.sum(F.col(c).isNull().cast('Integer')).alias(c) for c in df.columns))
# si quieres pasarlo a pandas: df_null = data.select(*(F.sum(F.col(c).isNull().cast('Double')).alias(c) for c in data.columns)).toPandas()
df_null.show()




print "\n" 

#-------------------------------------------------------------------------VALORES UNICOS - CONVIERTE A PANDAS DATAFRAME--------------------------------------------------
print '*****************************************************************VALORES UNICOS - CONVIERTE A PANDAS DATAFRAME***********************************************************'

df_pd=df.toPandas()

#DIMENSIONES ANTES DE LOS FILTROS
print 'Dimension antes de los filtros {0}'.format(df_pd.shape)

# Para saber los valores unicos sin problema
for col in categoricalColumns+['historial_crediticio']:
    col_count = df.select(col).distinct().count()
    print '{0} - Valores unicos: {1}'.format(col, col_count)
    print df_pd[col].value_counts()
    print "\n"
    



# Hay incosistencia se procede a agregar variables    

# SEXO
df_pd.loc[df_pd.sexo=="1", "sexo"]="M"
df_pd.loc[df_pd.sexo=="2", "sexo"]="F"
df_pd.loc[df_pd.sexo=="0", "sexo"]="NA"
print df_pd["sexo"].value_counts()



# NACIONALIDAD: QUERY EN IMPALA: SELECT DISTINCT nacionalidad FROM dwhdes.data_ctes_activos_em WHERE semanaproceso=201952  ORDER BY nacionalidad LIMIT 100 
# SELECT nacionalidad, count(nacionalidad) FROM dwhdes.data_ctes_activos_em WHERE semanaproceso=201952 GROUP BY nacionalidad ORDER BY nacionalidad LIMIT 100
# SELECT nacionalidad, count(nacionalidad) FROM dwhdes.data_ctes_activos_em WHERE semanaproceso=201952 GROUP BY nacionalidad ORDER BY count(nacionalidad) DESC LIMIT 100

'''
 Las compañías que forman Grupo Salinas operan en México (MEXICANA), Estados Unidos (ESTADOUNIDENSE), El Salvador (EL SALVADOR), Guatemala (GUATEMALTECA), Honduras (HONDURAS), Panamá (PANAMA) y Perú (PERU).
'''

df_pd.loc[df_pd.nacionalidad=="ARGENTINA", "nacionalidad"]="ARGENTINO"
df_pd.loc[df_pd.nacionalidad=="CHILE", "nacionalidad"]="CHILENA"
df_pd.loc[df_pd.nacionalidad=="COLOMBIA", "nacionalidad"]="COLOMBIANO"
df_pd.loc[df_pd.nacionalidad=="CUBA", "nacionalidad"]="CUBANO"
df_pd.loc[df_pd.nacionalidad=="ESTADOS UNIDOS", "nacionalidad"]="ESTADOUNIDENSE"
df_pd.loc[df_pd.nacionalidad=="GUATEMALA", "nacionalidad"]="GUATEMALTECA"
df_pd.loc[df_pd.nacionalidad=="HONDUREÑA", "nacionalidad"]="HONDURAS"
df_pd.loc[df_pd.nacionalidad=="MEXICO", "nacionalidad"]="MEXICANA"
df_pd.loc[df_pd.nacionalidad=="NICARAGUA", "nacionalidad"]="NICARAGUENSE"
df_pd.loc[df_pd.nacionalidad=="VENEZUELA", "nacionalidad"]="VENEZOLANO"
df_pd.loc[df_pd.nacionalidad=="SALVADOREÑA", "nacionalidad"]="EL SALVADOR"
df_pd.loc[df_pd.nacionalidad=="PANAMEÑA", "nacionalidad"]="PANAMA"

# Funcion para agregar, solo se considera las nacionalidades donde opera GS, vale la pena poner NO DEFINIDO porque si tiene peso
def N_converter(x):
    if x in ["MEXICANA", "ESTADOUNIDENSE", "EL SALVADOR", "GUATEMALTECA", "HONDURAS", "PANAMA", "PERU", "NO DEFINIDO"]:
        return x
    else:
        return "OTRO"

        
df_pd["nacionalidad"]=df_pd["nacionalidad"].apply(N_converter)       
    

print df_pd["nacionalidad"].value_counts()


# ESTADO: SELECT DISTINCT estado FROM dwhdes.data_ctes_activos_em WHERE semanaproceso=201952  ORDER BY estado LIMIT 1700
# CUENTA: SELECT estado, count(estado) FROM dwhdes.data_ctes_activos_em WHERE semanaproceso=201952 GROUP BY estado ORDER BY estado LIMIT 1700


# Agregando los estados
df_pd.loc[df_pd.estado==".QUINTANA ROO", "estado"]="QUINTANA ROO"
df_pd.loc[df_pd.estado=="4MEXICO", "estado"]="MEXICO"
df_pd.loc[df_pd.estado=="836TAMAULIPAS", "estado"]="TAMAULIPAS"
df_pd.loc[df_pd.estado=="BAJA CALIFORNIA NORTE", "estado"]="BAJA CALIFORNIA"
df_pd.loc[df_pd.estado=="CIUDAD DE MEXICO", "estado"]="CDMX"
df_pd.loc[df_pd.estado=="COAHUILA", "estado"]="COAHUILA DE ZARAGOZA"
df_pd.loc[df_pd.estado=="OAX", "estado"]="MICHOACAN DE OCAMPO"
df_pd.loc[df_pd.estado=="OAXACA", "estado"]="OAXACA DE JUAREZ"
df_pd.loc[df_pd.estado=="PUE", "estado"]="PUEBLA"
df_pd.loc[df_pd.estado=="QUERETARO ARTEAGA", "estado"]="QUERETARO"
df_pd.loc[df_pd.estado=="QUINTANARRO", "estado"]="QUINTANA ROO"
df_pd.loc[df_pd.estado=="SAN LUIS POTOSISOLED", "estado"]="SAN LUIS POTOSI"
df_pd.loc[df_pd.estado=="VERACRUZ               LA LLAV", "estado"]="VERACRUZ"
df_pd.loc[df_pd.estado=="VERACRUZ DE IGNACIO DE LA LLAVE", "estado"]="VERACRUZ"
df_pd.loc[df_pd.estado=="ZAC", "estado"]="ZACATECAS"

# Creando lista de estados


entidades=['AGUASCALIENTES',
 'BAJA CALIFORNIA',
 'BAJA CALIFORNIA SUR',
 'CAMPECHE',
 'CDMX',
 'COAHUILA DE ZARAGOZA',
 'COLIMA',
 'CHIAPAS',
 'CHIHUAHUA',
 'DURANGO',
 'GUANAJUATO',
 'GUERRERO',
 'HIDALGO',
 'JALISCO',
 'MEXICO',
 'MICHOACAN DE OCAMPO',
 'MORELOS',
 'NAYARIT',
 'NUEVO LEON',
 'OAXACA DE JUAREZ',
 'PUEBLA',
 'QUERETARO',
 'QUINTANA ROO',
 'SAN LUIS POTOSI',
 'SINALOA',
 'SONORA',
 'TABASCO',
 'TAMAULIPAS',
 'TLAXCALA',
 'VERACRUZ',
 'YUCATAN',
 'ZACATECAS']


df_pd=df_pd[df_pd.estado.isin(entidades)]

# Fin de las incosistencias
print "\n"

print '**********************************************************TABLA CONSISTENTE***********************************************************'

print "TABLA CONSISTENTE"
# Para saber los valores unicos sin problema
for col in categoricalColumns+['historial_crediticio']:
    col_count = df_pd[col].unique()
    print '{0} - Valores unicos: {1}'.format(col, len(col_count))
    print df_pd[col].value_counts()
    print "\n"
    

print 'Dimension Tabla Consistente {0}'.format(df_pd.shape)
    


#-------------------------------------------------------------------------VALORES NULOS-------------------------------------------------------------------------------------------------
# Cambiar los NA por np.nan ya que los NA son strings
df_pd=df_pd.replace("NA", np.nan)
print df_pd.isnull().sum()

#-------------------------------------------------------------------------BORRANDO VALORES NULOS----------------------------------------------------------------------------------------------------------
print '**********************************************************BORRANDO VALORES NULOS***********************************************************'
print "\n"
df_pd=df_pd.dropna(axis=0)
print df_pd.isnull().sum()


#-------------------------------------------------------------------------CAMBIANDO LOS TARGETS POR 0 y 1-------------------------------------------------------------------------------------------------------------
df_pd["historial_crediticio"]=df_pd["historial_crediticio"].map({'E':1,'M':0})    

#-------------------------------------------------------------------------TABLA CONSISTENTE SIN VALORES NULOS-------------------------------------------------------------------------------------------------
print '***********************************************************TABLA CONSISTENTE SIN VALORES NULOS ******************************************************************************'
# Para saber los valores unicos sin problema

for col in df_pd.columns.tolist()+['historial_crediticio']:
    col_count = df_pd[col].unique()
    print '{0} - Valores unicos: {1}'.format(col, len(col_count))
    print df_pd[col].value_counts()
    print "\n"




#-------------------------------------------------------------------------TRATAMIENTO VARIABLES NUMERICAS-------------------------------------------------------------------------------------------------
print '***********************************************************TRATAMIENTO INCONSITENCIAS VARIABLES NUMERICAS***********************************************************'

''' Se borra, son demasiados datos nulos
ing_mensual - Valores unicos: 4690
0.00        720280


Nivel de endudamiento se borra
nivel_endeudamiento - Valores unicos: 5817
0.00        965053

'''
# Borra filas con esos datos inconsitentes
df_pd.drop(df_pd[['ing_mensual', 'nivel_endeudamiento']], axis = 1, inplace = True) 
df_pd.drop(df_pd[(df_pd['edad'] == -29) | (df_pd['antiguedad_cliente'] == 120)].index, inplace = True) 
df_pd.drop(df_pd[df_pd['edad'] == 1].index, inplace = True) 



for col in df_pd.columns.tolist()+['historial_crediticio']:
    col_count = df_pd[col].unique()
    print '{0} - Valores unicos: {1}'.format(col, len(col_count))
    print df_pd[col].value_counts()
    print "\n"




#-------------------------------------------------------------------------ESTADISTICAS BASICAS Y ANALISIS ANTES DE CATEGORIZAR-------------------------------------------------------------------------------------------------
print '***********************************************************ESTADISTICAS BASICAS Y ANALISIS ANTES DE CATEGORIZAR***********************************************************'
# Estadisticas basicas

print df_pd.describe(include='all')


#-------------------------------------------------------------------------TRATAMIENTO VARIABLES CATEGORICAS-------------------------------------------------------------------------------------------------
print '***********************************************************TABLA CONSISTENTE SIN VALORES NULOS CATEGORIZADA***********************************************************'

# Creando variables dummy para datos categoricos
# Aqui el dataframe tiene nuevas columnas, una nueva por cada categoria

for var_name in categoricalColumns:
    dummy = pd.get_dummies(df_pd[var_name], prefix=var_name, drop_first=True)
    df_pd = df_pd.drop(var_name, axis = 1)
    df_pd = pd.concat([df_pd, dummy], axis = 1)
    
df_pd.head()

'''    
# Aqui el dataframe tiene nuevas columnas, una nueva por cada categoria
for col in df_pd.columns.tolist()+['historial_crediticio']:
    col_count = df_pd[col].unique()
    print '{0} - Valores unicos: {1}'.format(col, len(col_count))
    print df_pd[col].value_counts()
    print "\n"
'''







#df.groupby(df.estado).count().distinct().show(1599,False) 
#df.groupby(df.nacionalidad).count().distinct().show(85,False) 
#df.groupBy('historial_crediticio').count().show()
#df.select('estado').distinct().show()

