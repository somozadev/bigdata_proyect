import pandas as pd
import numpy as np
import streamlit as st
import time
import pyspark
import folium
from pyspark.pandas.frame import CachedDataFrame
from pyspark.sql import SparkSession, dataframe
from pyspark.sql import functions as F
from pyspark.sql.types import  DoubleType,IntegerType,StringType 
@st.cache
def run_fxn(n: int) -> list:
    return range(n)
def main():
   #st.title("Almeria la ciudad!")
   #st.text("Texto: Hola Streamlit")
  
    spark = SparkSession.builder.getOrCreate()
    cards_dataset = spark.read.csv('cards.csv',sep = '|', header = True)

    cards_dataset = ValueTypesSetup(cards_dataset)
  #  cards_dataset.printSchema()
    
    
   #dado un comercio se muestre porcentajes de ventas de cada categoria
   # 
   #     
    sales_given_commerce = cards_dataset.filter(F.col('CP_COMERCIO') == '04006')
   # sales_given_commerce.show(sales_given_commerce.count(),False)
   # print("COunt> ", sales_given_commerce.count())

    sales_given_commerce.createTempView('sales_given_commerce')
    num_alim = spark.sql("SELECT * FROM sales_given_commerce WHERE SECTOR == 'ALIMENTACION'").count() #num alimentacion
    num_auto = spark.sql("SELECT * FROM sales_given_commerce WHERE SECTOR == 'AUTO'").count() #num alimentacion
    num_belleza = spark.sql("SELECT * FROM sales_given_commerce WHERE SECTOR == 'BELLEZA'").count() #num alimentacion
    num_hogar = spark.sql("SELECT * FROM sales_given_commerce WHERE SECTOR == 'HOGAR'").count() #num alimentacion
    num_moda = spark.sql("SELECT * FROM sales_given_commerce WHERE SECTOR == 'MODA Y COMPLEMENTOS'").count() #num alimentacion
    num_ocio = spark.sql("SELECT * FROM sales_given_commerce WHERE SECTOR == 'OTROS'").count() #num alimentacion
    num_otros = spark.sql("SELECT * FROM sales_given_commerce WHERE SECTOR == 'OCIO Y TIEMPO LIBRE'").count() #num alimentacion
    num_restauracion = spark.sql("SELECT * FROM sales_given_commerce WHERE SECTOR == 'RESTAURACION'").count() #num alimentacion
    num_salud = spark.sql("SELECT * FROM sales_given_commerce WHERE SECTOR == 'SALUD'").count() #num alimentacion
    num_tecnologia = spark.sql("SELECT * FROM sales_given_commerce WHERE SECTOR == 'TECNOLOGIA'").count() #num alimentacion
    
    


    #printFilters(getSectorFiltersDataset(cards_dataset),cards_dataset.count())  #% de ventas por sector 
    with st.expander("ver ventas por sector"):
        for i in (getSectorFiltersDataset(cards_dataset))[1:]:
            st.write(i[1] ,(getPercentaje(i[0].count(), cards_dataset.count())), "%")
            st.progress(int(getPercentaje(i[0].count(), cards_dataset.count())))
            
              

    #printFilters(getCpComercioFiltersDataset(cards_dataset),cards_dataset.count()) #% de ventas por tienda (cp)
    with st.expander("ver ventas por tienda"):
        for i in (getCpComercioFiltersDataset(cards_dataset))[1:]:
            st.write(i[1],(getPercentaje(i[0].count(), cards_dataset.count())),"%")
            st.progress(int(getPercentaje(i[0].count(), cards_dataset.count())))
    

     
   #getSoldsByCommerce(cards_dataset,'04006')


def ValueTypesSetup(database):
    database = database.withColumn('CP_CLIENTE', F.col('CP_CLIENTE').cast(StringType()))
    database = database.withColumn('CP_COMERCIO', F.col('CP_COMERCIO').cast(StringType()))
    database = database.withColumn('IMPORTE', F.col('IMPORTE').cast(StringType()))
    database = database.withColumn('NUM_OP', F.col('NUM_OP').cast(IntegerType()))
    return database
    
def SectorPercentajePerCommerce(dataset): #hay  9 comercios (04001 - 04009)
    print("sector per commerce is: ", dataset.filter(F.col('CP_CLIENTE') == 'CP_CLIENTE').groupBy('CP_CLIENTE').count())
    






#region SECTOR % 
def getSectorFiltersDataset(cards_dataset): #crea una lista de tuplas (dataset,nombre) del dataset
    sector_filters_dataset = [
        (cards_dataset, 'CARDS'),
        (getDataframe_sectorFilter(cards_dataset,'ALIMENTACION'),'ALIMENTACION'), 
        (getDataframe_sectorFilter(cards_dataset,'AUTO'),'AUTO'),
        (getDataframe_sectorFilter(cards_dataset,'BELLEZA'),'BELLEZA'),
        (getDataframe_sectorFilter(cards_dataset,'HOGAR'),'HOGAR'),
        (getDataframe_sectorFilter(cards_dataset,'MODA Y COMPLEMENTOS'),'MODA Y COMPLEMENTOS'),
        (getDataframe_sectorFilter(cards_dataset,'OCIO Y TIEMPO LIBRE'),'OCIO Y TIEMPO LIBRE'),
        (getDataframe_sectorFilter(cards_dataset,'OTROS'),'OTROS'),
        (getDataframe_sectorFilter(cards_dataset,'RESTAURACION'),'RESTAURACION'),
        (getDataframe_sectorFilter(cards_dataset,'SALUD'),'SALUD'),
        (getDataframe_sectorFilter(cards_dataset,'TECNOLOGIA'),'TECNOLOGIA')]

    return sector_filters_dataset
def getDataframe_sectorFilter(dataset, filter): #filtra el sector de la dataset dada en base al tipo (filter) pasado
    return dataset.filter(F.col('SECTOR') == filter)
#endregion

#region CP_COMERCIO %
def getCpComercioFiltersDataset(cards_dataset): #crea una lista de tuplas (dataset,nombre) del dataset
    cp_comercio_filters_dataset = [
        (cards_dataset, 'CARDS'),
        (getDataframe_CpComercioFilter(cards_dataset,'04001'),'04001'), 
        (getDataframe_CpComercioFilter(cards_dataset,'04002'),'04002'),
        (getDataframe_CpComercioFilter(cards_dataset,'04003'),'04003'),
        (getDataframe_CpComercioFilter(cards_dataset,'04004'),'04004'),
        (getDataframe_CpComercioFilter(cards_dataset,'04005'),'04005'),
        (getDataframe_CpComercioFilter(cards_dataset,'04006'),'04006'),
        (getDataframe_CpComercioFilter(cards_dataset,'04007'),'04007'),
        (getDataframe_CpComercioFilter(cards_dataset,'04008'),'04008'),
        (getDataframe_CpComercioFilter(cards_dataset,'04009'),'04009'),]
    return cp_comercio_filters_dataset
def getSoldsByCommerce(dataset,comercio):
    cp = getDataframe_CpComercioFilter(dataset, comercio)
    cp.show()
def getDataframe_CpComercioFilter(dataset, filter): #filtra el sector de la dataset dada en base al tipo (filter) pasado
    return dataset.filter(F.col('CP_COMERCIO') == filter)
#endregion

#region %
def printFilters(dataset, max): #recorre e imprime el porcentaje de la lista de tuplas (dataset,nombre)  
    for i in dataset[1:]:
        print(i[1], end=": ")
        printPercentaje(i[0].count(), max) 
def printPercentaje(min,max): #calcula un porcentaje
    print(round(min/max * 100,2) , "%")
def getPercentaje(min,max):
    return (round(min/max * 100, 2))
#endregion


if __name__ == "__main__":
    main()