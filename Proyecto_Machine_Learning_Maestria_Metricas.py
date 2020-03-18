'''
Mismo programa que Proyecto_Machine_Learning_Maestria_Metrias
Pero calcula estadisticas y metricas 
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

start_time=time()



#-------------------------------------------------------DEFINIENDO: SparkConf, SparkContext y HiveContext-------------------------------------------------------------
# ENCABEZADO PARA CORRER EL PROGRAMA EN CLOUDERA:
conf=SparkConf().setAppName("Machine_Learning_ORCs_v2")
sc=SparkContext.getOrCreate(conf=conf)
sqlContext=HiveContext(sc)
# Versión de PySpark sobre la que se va a ajecutar el proyecto
print "Versión de PySpark: ", sc.version



#-------------------------------------------------------SELECCIONANDO LA TABLA OBJETIVO: dwhdes.data_ctes_activos_em-------------------------------------------------------------
# Tabla a seleccionar: dwhdes.data_ctes_activos_em con 79,784,766 registros.
print '*******************************************************TABLA ORIGINAL************************************************************'
    
data=sqlContext.sql("SELECT * FROM dwhdes.data_ctes_activos_em_v2")
data.show()

print "Número de registros: ", data.count()  # 79,784,766
# Imprime el esquema y el tipo de dato
print data.printSchema()
#print data.dtypes

# Imprime las columnas
print "\n"
# print data.columns



#------------------------------------------------SELECCIONANDO VARIIABLES DE INTERES DE LA TABLA OBJETIVO: dwhdes.data_ctes_activos_em-------------------------------------------------------------

df=data.select('edad', 'sexo', 'estado', 'nacionalidad', 'antiguedad_cliente', 'trabajo', \
'ing_mensual', 'capacidad_pago', 'estado_civil', 'credito_en_mora', 'monto_del_credito', \
'duracion_credito', 'nivel_endeudamiento', 'aval', 'cuenta_ahorro', 'creditos_pagados', 'creditos_no_pagados', \
"semanaproceso", 'historial_crediticio')



# ---------------------------------------------------------CAMBIANDO TIPO DE DATO------------------------------------------------------------------------------------------------
from pyspark.sql.types import *
print '*********************************************************CAMBIANDO TIPO DE DATO***********************************************************'
df = df.withColumn('edad', df.edad.cast(IntegerType()))
df = df.withColumn('sexo', df.sexo.cast(StringType()))
df = df.withColumn('estado', df.estado.cast(StringType()))
df = df.withColumn('nacionalidad', df.nacionalidad.cast(StringType()))
df = df.withColumn('antiguedad_cliente', df.antiguedad_cliente.cast(IntegerType()))
df = df.withColumn('trabajo', df.trabajo.cast(StringType()))
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



#------------------------------------------------------------------FILTRANDO DEPENDIENDO DE LA SEMANA---------------------------------------------------------------
print '*********************************************************TABLA CON FLITRO semanaproceso***********************************************************'
semana=201952
df=df.filter(df.semanaproceso==semana)
df.show()

df=df.drop("semanaproceso")
df.show()
print "Número de registros de la semana " + str(semana) + ": ", df.count()
# registros: 1,438,972


#----------------------------------------------------------------SEPARANDO VARIABLES NUMERICAS, CATEGORICAS Y OBJETIVO---------------------------------------------------------------

categoricalColumns=['sexo', 'estado', 'nacionalidad', 'trabajo', 'estado_civil', 'credito_en_mora', 'aval', 'cuenta_ahorro']
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

print "\n" 



#-------------------------------------------------------------------------VALORES UNICOS - CONVIERTE A PANDAS DATAFRAME--------------------------------------------------
print '*****************************************************************VALORES UNICOS - CONVIERTE A PANDAS DATAFRAME***********************************************************'

df_pd=df.toPandas()

#DIMENSIONES ANTES DE LOS FILTROS
print 'Dimension antes de los filtros {0}'.format(df_pd.shape) # (1438972, 18)

# Para saber los valores unicos sin problema
for col in categoricalColumns+['historial_crediticio']:
    col_count = df.select(col).distinct().count()
    print '{0} - Valores unicos: {1}'.format(col, col_count)
    print df_pd[col].value_counts()
    print "\n"


# Arreglado los estados con un mejor nombre
# SELECT estado, count(estado) FROM dwhdes.data_ctes_activos_em_v2 WHERE semanaproceso=201952 GROUP BY estado ORDER BY count(estado) DESC LIMIT 100

df_pd.loc[df_pd.estado=="MAxico", "estado"]="Mexico"
df_pd.loc[df_pd.estado=="Veracruz de Ignacio de la Llave", "estado"]="Veracruz"
df_pd.loc[df_pd.estado=="MichoacAzn de Ocampo", "estado"]="Michoacan"
df_pd.loc[df_pd.estado=="YucatAzn", "estado"]="Yucatan"
df_pd.loc[df_pd.estado=="Nuevo LeAn", "estado"]="Nuevo_Leon"
df_pd.loc[df_pd.estado=="San Luis PotosA", "estado"]="San_Luis_Potosi"
df_pd.loc[df_pd.estado=="Coahuila de Zaragoza", "estado"]="Coahuila"
df_pd.loc[df_pd.estado=="Ciudad de MAxico", "estado"]="CDMX"
df_pd.loc[df_pd.estado=="QuerAtaro", "estado"]="Queretaro"
df_pd.loc[df_pd.estado=="Quintana Roo", "estado"]="Quintana_Roo"
df_pd.loc[df_pd.estado=="Baja California", "estado"]="Baja_California"
df_pd.loc[df_pd.estado=="Baja California Sur", "estado"]="Baja_California_Sur"


# Funcion para agregar, solo se considera las nacionalidades donde opera GS, vale la pena poner NO DEFINIDO porque si tiene peso
# SELECT nacionalidad, count(nacionalidad) FROM dwhdes.data_ctes_activos_em WHERE semanaproceso=201952 GROUP BY nacionalidad ORDER BY count(nacionalidad) DESC LIMIT 100

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


def N_converter(x):
    if x in ["MEXICANA", "ESTADOUNIDENSE", "EL SALVADOR", "GUATEMALTECA", "HONDURAS", "PANAMA", "PERU", "NO DEFINIDO"]:
        return x
    else:
        return "OTRA_NACIONALIDAD"

        
df_pd["nacionalidad"]=df_pd["nacionalidad"].apply(N_converter)       
    

# Agregando Trabajo
# SELECT trabajo, count(trabajo) FROM dwhdes.data_ctes_activos_em WHERE semanaproceso=201952 GROUP BY trabajo ORDER BY count(trabajo) DESC LIMIT 1000

df_pd.loc[df_pd.nacionalidad=="AMA DE CASA", "trabajo"]="HOGAR"
df_pd.loc[df_pd.nacionalidad=="COMERCIANTE, PROVEEDOR (AL MAYOREO O MENUDEO)", "trabajo"]="COMERCIANTE"
df_pd.loc[df_pd.nacionalidad=="EMPLEADO - OTROS", "trabajo"]="EMPLEADO"
df_pd.loc[df_pd.nacionalidad=="TRABAJADOR INDEPENDIENTE - OTROS", "trabajo"]="POR SU CUENTA"
df_pd.loc[df_pd.nacionalidad=="JUBILADO", "trabajo"]="PENSIONADO"
df_pd.loc[df_pd.nacionalidad=="PESCADORES BAHIA, ESTEREO", "trabajo"]="PESCADORES"
df_pd.loc[df_pd.nacionalidad=="DUEÑO/PROPIETARIO", "trabajo"]="PROPIETARIO"
df_pd.loc[df_pd.nacionalidad=="OTROS", "trabajo"]="OTRO_TRABAJO"


# Funcion para agregar la variable TRABAJO

lt=["EMPLEADO", 
    "NO DEFINIDO",
    "COMERCIANTE",
    "EMPRESA PRIVADA",
    "HOGAR",
    "ALBAÑIL",
    "AGRICULTOR",
    "NA",
    "PROPIETARIO",
    "POR SU CUENTA",
    "COSTURERA",
    "OTROS INGRESOS",
    "COCINERO",
    "OFICIO",
    "ADMINISTRADOR",
    "TRANSPORTISTA",
    "MECANICO",
    "ESTUDIANTE",
    "CHOFER TAXI",
    "PROFESOR",
    "CONSTRUCTOR",
    "PLOMEROS",
    "ENCARGADO DE ALMACEN",
    "PENSIONADO",
    "ABARROTERO",
    "NEGOCIO PROPIO - OTROS",
    "ENFERMERIA",
    "CARPINTERO",
    "ELECTRICISTA",
    "CAJERO",
    "TAQUEROS",
    "ZAPATERO"
    ]


def N_converter_t(x):
    if x in lt:
        return x
    else:
        return "OTRO_TRABAJO"

        
df_pd["trabajo"]=df_pd["trabajo"].apply(N_converter_t)  


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
    

print 'Dimension Tabla Consistente {0}'.format(df_pd.shape) #(1438972,18)



#-------------------------------------------------------------------------CAMBIANDO LOS TARGETS POR 0 y 1-------------------------------------------------------------------------------------------------------------
df_pd["aval"]=df_pd["aval"].map({'0':"NO", '1':"SI"}) 
df_pd["historial_crediticio"]=df_pd["historial_crediticio"].map({'E':1,'M':0})



#-------------------------------------------------------------------------TABLA CONSISTENTE SIN VALORES NULOS-------------------------------------------------------------------------------------------------
print '***********************************************************TABLA CONSISTENTE SIN VALORES NULOS Y ANALIZANDO VARIABLES NNUMERICAS ******************************************************************************'
# Para saber los valores unicos sin problema

for col in df_pd.columns.tolist():
    col_count = df_pd[col].unique()
    print '{0} - Valores unicos: {1}'.format(col, len(col_count))
    print df_pd[col].value_counts()
    print "\n"


# Arreglando variables numericas inconsistentes


# Borra filas con esos datos inconsitentes
df_pd.drop(df_pd[['ing_mensual', 'nivel_endeudamiento']], axis = 1, inplace = True) 
df_pd.drop(df_pd[(df_pd['edad'] == -29) | (df_pd['antiguedad_cliente'] == 120)].index, inplace = True) 
df_pd.drop(df_pd[df_pd['edad'] == 1].index, inplace = True) 
df_pd.drop(df_pd[df_pd['duracion_credito'] == 9999].index, inplace = True) 
df_pd.drop(df_pd[df_pd['sexo'] == '0'].index, inplace = True) 


for col in df_pd.columns.tolist():
    col_count = df_pd[col].unique()
    print '{0} - Valores unicos: {1}'.format(col, len(col_count))
    print df_pd[col].value_counts()
    print "\n"



#-------------------------------------------------------------------------VALORES NULOS-------------------------------------------------------------------------------------------------
# Cambiar los NA por np.nan ya que los NA son strings
df_pd=df_pd.replace("NA", np.nan)


for col in df_pd.columns.tolist():
    col_count = df_pd[col].unique()
    print '{0} - Valores unicos: {1}'.format(col, len(col_count))
    print df_pd[col].value_counts()
    print "\n"


print df_pd.isnull().sum()



#-------------------------------------------------------------------------BORRANDO VALORES NULOS----------------------------------------------------------------------------------------------------------
print '**********************************************************BORRANDO VALORES NULOS***********************************************************'
print "\n"
df_pd=df_pd.dropna(axis=0)
print df_pd.isnull().sum()
print "Dimensiones tabla final: ", df_pd.shape # (1280402,16)



for col in df_pd.columns.tolist():
    col_count = df_pd[col].unique()
    print '{0} - Valores unicos: {1}'.format(col, len(col_count))
    print df_pd[col].value_counts()
    print "\n"



#-------------------------------------------------------------------------ESTADISTICAS BASICAS Y ANALISIS ANTES DE CATEGORIZAR-------------------------------------------------------------------------------------------------
print '***********************************************************ESTADISTICAS BASICAS Y ANALISIS ANTES DE CATEGORIZAR***********************************************************'

# Formato en 2 decimales: 
pd.options.display.float_format = '{:.2f}'.format
# Estadisticas basicas
print df_pd.describe(include='all')
print "Agrupando"
print df_pd.groupby("historial_crediticio").mean()

#pd.crosstab(df_pd["sexo"], df_pd["historial_crediticio"], normalize="all", margins=True).sort_values(0,ascending=0)*100

#pd.crosstab(df_pd["estado"], df_pd["historial_crediticio"], normalize="all",  margins=True).sort_values(0,ascending=0)*100

#pd.crosstab(df_pd["nacionalidad"], df_pd["historial_crediticio"], normalize="all",  margins=True).sort_values(0,ascending=0)*100

#pd.crosstab(df_pd["trabajo"], df_pd["historial_crediticio"], normalize="all",  margins=True).sort_values(0,ascending=0)*100

#pd.crosstab(df_pd["estado_civil"], df_pd["historial_crediticio"], normalize="all",  margins=True).sort_values(0,ascending=0)*100

#pd.crosstab(df_pd["edad"], df_pd["historial_crediticio"], normalize="all",  margins=True).sort_values(0,ascending=0)*100

#pd.crosstab(df_pd["trabajo"], df_pd["sexo"], normalize="all",  margins=True).sort_values("F",ascending=0)*100

#pd.crosstab(df_pd["credito_en_mora"], df_pd["historial_crediticio"], normalize="all",  margins=True).sort_values(0,ascending=0)*100

pd.crosstab(df_pd["creditos_pagados"], df_pd["historial_crediticio"], normalize="all",  margins=True).sort_values(0,ascending=0)*100

pd.crosstab(df_pd["creditos_no_pagados"], df_pd["historial_crediticio"], normalize="all",  margins=True).sort_values(0,ascending=0)*100



#-------------------------------------------------------------------------TRATAMIENTO VARIABLES CATEGORICAS-------------------------------------------------------------------------------------------------
print '***********************************************************TABLA CONSISTENTE SIN VALORES NULOS CATEGORIZADA***********************************************************'

# Creando variables dummy para datos categoricos
# Aqui el dataframe tiene nuevas columnas, una nueva por cada categoria

for var_name in categoricalColumns:
    dummy = pd.get_dummies(df_pd[var_name], prefix=var_name)
    df_pd = df_pd.drop(var_name, axis = 1)
    df_pd = pd.concat([df_pd, dummy], axis = 1)

# (1280402, 93)    
#print df_pd.head()

#print df_pd.columns.values.tolist()



#----------------------------------------------------------------------------SEPARANDO DATOS PARA ENTRENAMIENTO Y TEST----------------------------------------------------------------------------------------
print '**************************************************************SEPARANDO DATOS PARA ENTRENAMIENTO Y PRUEBA***********************************************************'

X=df_pd.drop('historial_crediticio', axis=1)
y=df_pd['historial_crediticio']


from sklearn.model_selection import train_test_split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=1)
print ('Train set:', X_train.shape,  y_train.shape)
print ('Test set:', X_test.shape,  y_test.shape)



#-----------------------------------------------------------------------------------------LIBRESRIAS PARA METRICAS---------------------------------------------------------------------------------------------------

from sklearn.metrics import confusion_matrix
from sklearn.metrics import accuracy_score
from sklearn.metrics import precision_score
from sklearn.metrics import recall_score
from sklearn.metrics import f1_score
from sklearn.metrics import classification_report
from sklearn.metrics import cohen_kappa_score

from sklearn.metrics import roc_curve  
from sklearn.metrics import roc_auc_score



#-----------------------------------------------------------------------------------------MODELOS---------------------------------------------------------------------------------------------------
print '*****************************************************************************MODELOS MACHINE LEARNING***********************************************************************************************'  


# Columna 2 de probs es la probabildiad
# columna 2 si es mayor que 0.5 se hace la compra o el valor 1,
# si es menor que 0.5 será catalogado como negativo como no compra
# list(zip(probs[:,1], prediction ))


# Matriz
# [TN FP]
# [FN TP]



# import GridSearchCV
# from sklearn.model_selection import GridSearchCV
#In case of classifier like knn the parameter to be tuned is n_neighbors
# param_grid = {'n_neighbors':np.arange(1,50)}
# knn = KNeighborsClassifier()
# knn_cv= GridSearchCV(knn,param_grid,cv=5)
# knn_cv.fit(X,y)
# knn_cv.best_score_
# knn_cv.best_params_



# Validacion Cruzada
# from sklearn.model_selection import cross_val_score
# scores = cross_val_score(rf, X, y, cv=5, scoring='roc_auc')
# print scores
# print scores.mean()



# Losgistico---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
from sklearn.linear_model import LogisticRegression
lr = LogisticRegression().fit(X_train, y_train)
print "Regresión logística"
print "Train set score: {:.3f}".format(lr.score(X_train, y_train))
print "Test set score: {:.3f}\n".format(lr.score(X_test, y_test))

y_pred_lr = lr.predict(X_test)
probs_lr = lr.predict_proba(X_test)
probs_lr=probs_lr[:,1]

print "Metricas Model REGRESION LOGISTICA"

cnf_matrix_lr = confusion_matrix(y_test, y_pred_lr)
print cnf_matrix_lr

#cnf_matrix_lr_N = confusion_matrix(y_test, y_pred_lr, normalize="all")
#print cnf_matrix_lr_N

print "Metricas Modelo Logistico"
print "Accuracy:", accuracy_score(y_test, y_pred_lr)
print "Precision:", precision_score(y_test, y_pred_lr)
print "Recall:", recall_score(y_test, y_pred_lr)
print "f1:", f1_score(y_test, y_pred_lr)
print "Kappa Score:", cohen_kappa_score(y_test, y_pred_lr)

print "Clasifficacion Report", classification_report(y_test, y_pred_lr, target_names=['class 0', 'class 1'])

auc_lr = roc_auc_score(y_test, probs_lr)  
print 'AUC: ', auc_lr  

print "\n"


# KNN----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
from sklearn.neighbors import KNeighborsClassifier
Knn = KNeighborsClassifier(n_neighbors=100).fit(X_train, y_train)
print "KNN"
print "Train set score: {:.3f}".format(Knn.score(X_train, y_train))
print "Test set score: {:.3f}\n".format(Knn.score(X_test, y_test))

y_pred_Knn = Knn.predict(X_test)
probs_Knn = Knn.predict_proba(X_test)
probs_Knn=probs_Knn[:,1]

print "Metricas Modelo KNN"

cnf_matrix_Knn = confusion_matrix(y_test, y_pred_Knn)
print cnf_matrix_Knn

#cnf_matrix_Knn_N = confusion_matrix(y_test, y_pred_Knn, normalize="all")
#print cnf_matrix_Knn_N

print "Accuracy:", accuracy_score(y_test, y_pred_Knn)
print "Precision:", precision_score(y_test, y_pred_Knn)
print "Recall:", recall_score(y_test, y_pred_Knn)
print "f1:", f1_score(y_test, y_pred_Knn)
print "Kappa Score:", cohen_kappa_score(y_test, y_pred_Knn)
print "Clasifficacion Report", classification_report(y_test, y_pred_Knn, target_names=['class 0', 'class 1'])

auc_Knn = roc_auc_score(y_test, probs_Knn)  
print 'AUC: ', auc_Knn  

print "\n"


# SVM-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# No tiene el metodo predict_proba hay que calibrarlo
from sklearn.calibration import CalibratedClassifierCV
from sklearn.svm import LinearSVC
lsvm_n = LinearSVC()
lsvm_n_c = CalibratedClassifierCV(lsvm_n) 
lsvm_n_c.fit(X_train, y_train)
print "SVM-Kernel Lineal"
print "Train set score: {:.3f}".format(lsvm_n_c.score(X_train, y_train))
print "Test set score: {:.3f}\n".format(lsvm_n_c.score(X_test, y_test))

y_pred_lsvm_n = lsvm_n_c.predict(X_test)
probs_lsvm_n = lsvm_n_c.predict_proba(X_test)
probs_lsvm_n=probs_lsvm_n[:,1]

print "Metricas Modelo LineaL SVM"

cnf_matrix_lsvm_n = confusion_matrix(y_test, y_pred_lsvm_n)
print cnf_matrix_lsvm_n 

#cnf_matrix_lsvm_n_N = confusion_matrix(y_test, y_pred_lsvm_n, normalize="all")
#print cnf_matrix_lsvm_n_N 

print "Accuracy:", accuracy_score(y_test, y_pred_lsvm_n)
print "Precision:", precision_score(y_test, y_pred_lsvm_n)
print "Recall:", recall_score(y_test, y_pred_lsvm_n)
print "f1:", f1_score(y_test, y_pred_lsvm_n)
print "Kappa Score:", cohen_kappa_score(y_test, y_pred_lsvm_n)
print "Clasifficacion Report", classification_report(y_test, y_pred_lsvm_n, target_names=['class 0', 'class 1'])

auc_lsvm_n = roc_auc_score(y_test, probs_lsvm_n)  
print 'AUC: ', auc_lsvm_n

print "\n"


# DecisionTree-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
from sklearn.tree import DecisionTreeClassifier
tree = DecisionTreeClassifier(random_state=0).fit(X_train, y_train)
print "DecisionTree"
print "Train set score: {:.3f}".format(tree.score(X_train, y_train))
print "Test set score: {:.3f}\n".format(tree.score(X_test, y_test))

y_pred_tree = tree.predict(X_test)
probs_tree = tree.predict_proba(X_test)
probs_tree=probs_tree[:,1]

print "Metricas Modelo ARBOL DE DECISION"

cnf_matrix_tree = confusion_matrix(y_test, y_pred_tree)
print cnf_matrix_tree

#cnf_matrix_tree_N = confusion_matrix(y_test, y_pred_tree, normalize="all")
#print cnf_matrix_tree_N

print "Accuracy:", accuracy_score(y_test, y_pred_tree)
print "Precision:", precision_score(y_test, y_pred_tree)
print "Recall:", recall_score(y_test, y_pred_tree)
print "f1:", f1_score(y_test, y_pred_tree)
print "Kappa Score:", cohen_kappa_score(y_test, y_pred_tree)
print "Clasifficacion Report", classification_report(y_test, y_pred_tree, target_names=['class 0', 'class 1'])

auc_tree = roc_auc_score(y_test, probs_tree)  
print 'AUC: ', auc_tree

print "\n"


# Random Forest-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
from sklearn.ensemble import RandomForestClassifier
forest = RandomForestClassifier(n_estimators=5, random_state=2).fit(X_train, y_train)
print "Random Forest"
print "Train set score: {:.3f}".format(forest.score(X_train, y_train))
print "Test set score: {:.3f}\n".format(forest.score(X_test, y_test))

y_pred_forest = forest.predict(X_test)
probs_forest = forest.predict_proba(X_test)
probs_forest=probs_forest[:,1]

print "Metricas Modelo BOSQUE ALEATORIO"

cnf_matrix_forest = confusion_matrix(y_test, y_pred_tree)
print cnf_matrix_forest

#cnf_matrix_forest_N = confusion_matrix(y_test, y_pred_tree, normalize="all")
#print cnf_matrix_forest_N

print "Accuracy:", accuracy_score(y_test, y_pred_forest)
print "Precision:", precision_score(y_test, y_pred_forest)
print "Recall:", recall_score(y_test, y_pred_forest)
print "f1:", f1_score(y_test, y_pred_forest)
print "Kappa Score:", cohen_kappa_score(y_test, y_pred_forest)
print "Clasifficacion Report", classification_report(y_test, y_pred_forest, target_names=['class 0', 'class 1'])

auc_forest = roc_auc_score(y_test, probs_forest)  
print 'AUC: ', auc_forest

print "\n"


# Redes Neuronales-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
from sklearn.neural_network import MLPClassifier
mlp = MLPClassifier().fit(X_train, y_train) # CON PARAMETROS SE AJUSTA MENOS solver='lbfgs', activation='logistic',random_state=0
print "Red Neuronal"
print("Training set score: {:.3f}".format(mlp.score(X_train, y_train)))
print("Test set score: {:.3f}".format(mlp.score(X_test, y_test)))

y_pred_mlp = mlp.predict(X_test)
probs_mlp = mlp.predict_proba(X_test)
probs_mlp=probs_mlp[:,1]

print "Metricas Modelo RED NEURONAL"

cnf_matrix_mlp = confusion_matrix(y_test, y_pred_mlp)
print cnf_matrix_mlp

#cnf_matrix_mlp_N = confusion_matrix(y_test, y_pred_mlp, normalize="all")
#print cnf_matrix_mlp_N

print "Accuracy:", accuracy_score(y_test, y_pred_mlp)
print "Precision:", precision_score(y_test, y_pred_mlp)
print "Recall:", recall_score(y_test, y_pred_mlp)
print "f1:", f1_score(y_test, y_pred_mlp)
print "Kappa Score:", cohen_kappa_score(y_test, y_pred_mlp)
print "Clasifficacion Report", classification_report(y_test, y_pred_mlp, target_names=['class 0', 'class 1'])

auc_mlp = roc_auc_score(y_test, probs_mlp)  
print 'AUC: ', auc_mlp

print "\n"


#-------------------------------------------------------------------------------------TIEMPO---------------------------------------------------------------------------------------------------
print '*****************************************************************************TIEMPO TOTAL***********************************************************************************************'

end_time=time()
time_in_minutes = int(float(end_time-start_time)/60)

print "Tiempo total en minutos: {0}".format(time_in_minutes)


