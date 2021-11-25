import pandas as pd
import numpy as np
import streamlit as st
import streamlit.components.v1 as components
import time
import pyspark
import folium
import plotly.express as px
import plotly.figure_factory as ff
import altair as alt
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

    components.iframe("https://api.mapbox.com/styles/v1/somozadev/ckw8o8s8l048914rrmowskqoy.html?title=false&access_token=pk.eyJ1Ijoic29tb3phZGV2IiwiYSI6ImNrdzU4b3V4ZmVtOGsybnM3YXF4ZzZzOW4ifQ.b6Pq5mbghDbGzNDuyR-rxQ&zoomwheel=false#13/36.842566/-2.462058", width= 720, height= 500,scrolling = False)    

    
    
    with st.expander("ver ventas por categoria del cp 04006"):
        aux = getCPSectorFilterDataset(spark,cards_dataset,'04006')
        for i in(aux)[1:]:
            st.write(i[1],(getPercentaje(i[0],int(aux[0][0]))), "%", i[0], "ventas")
            st.progress(int(getPercentaje(i[0],int(aux[0][0]))))

    #printFilters(getSectorFiltersDataset(cards_dataset),cards_dataset.count())  #% de ventas por sector 
    with st.expander("ver ventas por sector"):
        for i in (getSectorFiltersDataset(cards_dataset))[1:]:
            st.write(i[1] ,(getPercentaje(i[0].count(), cards_dataset.count())), "%")
            st.progress(int(getPercentaje(i[0].count(), cards_dataset.count())))
            
              

    #printFilters(getCpComercioFiltersDataset(cards_dataset),cards_dataset.count()) #% de ventas por tienda (cp)
    with st.expander("ver ventas por codigo postal"):
        for i in (getCpComercioFiltersDataset(cards_dataset))[1:]:
            st.write(i[1],(getPercentaje(i[0].count(), cards_dataset.count())),"%")
            st.progress(int(getPercentaje(i[0].count(), cards_dataset.count())))
    

     
    #getSoldsByCommerce(cards_dataset,'04006')

    #Grafica lineal de las ventas en un cp
    df = pd.DataFrame(getCPSectorFilterDataset(spark, cards_dataset, '04006'), columns = ['Ventas', 'Comercio'])
    fig = px.line(df, x = 'Comercio', y = 'Ventas', title='Ventas')
    st.plotly_chart(fig, use_container_width=True)

    df = pd.DataFrame(getSectorFilterDataset(spark, cards_dataset), columns = ['Ventas', 'Comercio'])
    st.write(df)
    fig = px.line(df, x = 'Comercio', y = 'Ventas', title='Ventas')
    st.plotly_chart(fig, use_container_width=True)


def ValueTypesSetup(database):
    database = database.withColumn('CP_CLIENTE', F.col('CP_CLIENTE').cast(StringType()))
    database = database.withColumn('CP_COMERCIO', F.col('CP_COMERCIO').cast(StringType()))
    database = database.withColumn('IMPORTE', F.col('IMPORTE').cast(StringType()))
    database = database.withColumn('NUM_OP', F.col('NUM_OP').cast(IntegerType()))
    return database
    
def SectorPercentajePerCommerce(dataset): #hay  9 comercios (04001 - 04009)
    print("sector per commerce is: ", dataset.filter(F.col('CP_CLIENTE') == 'CP_CLIENTE').groupBy('CP_CLIENTE').count())
    


def getCPSectorFilterDataset(spark, cards_dataset, cp):
    sales_given_commerce = cards_dataset.filter(F.col('CP_COMERCIO') == str(cp))# sales_given_commerce.createTempView('sales_given_commerce')
    sales_given_commerce.createOrReplaceTempView("sales_given_commerce")
    num_alim = spark.sql("SELECT * FROM sales_given_commerce WHERE SECTOR == 'ALIMENTACION'").count() #num alimentacion
    num_auto = spark.sql("SELECT * FROM sales_given_commerce WHERE SECTOR == 'AUTO'").count() #num alimentacion
    num_belleza = spark.sql("SELECT * FROM sales_given_commerce WHERE SECTOR == 'BELLEZA'").count() #num alimentacion
    num_hogar = spark.sql("SELECT * FROM sales_given_commerce WHERE SECTOR == 'HOGAR'").count() #num alimentacion
    num_moda = spark.sql("SELECT * FROM sales_given_commerce WHERE SECTOR == 'MODA Y COMPLEMENTOS'").count() #num alimentacion
    num_ocio = spark.sql("SELECT * FROM sales_given_commerce WHERE SECTOR == 'OCIO Y TIEMPO LIBRE'").count() #num alimentacion
    num_otros = spark.sql("SELECT * FROM sales_given_commerce WHERE SECTOR == 'OTROS'").count() #num alimentacion
    num_restauracion = spark.sql("SELECT * FROM sales_given_commerce WHERE SECTOR == 'RESTAURACION'").count() #num alimentacion
    num_salud = spark.sql("SELECT * FROM sales_given_commerce WHERE SECTOR == 'SALUD'").count() #num alimentacion
    num_tecnologia = spark.sql("SELECT * FROM sales_given_commerce WHERE SECTOR == 'TECNOLOGIA'").count() #num alimentacion
    
    cpSector_filters_dataset = [
        (sales_given_commerce.count(),'CARDS'),
        (num_alim, 'ALIMENTACION'),
        (num_auto, 'AUTO'),
        (num_belleza, 'BELLEZA'),
        (num_hogar, 'HOGAR'),
        (num_moda, 'MODA Y COMPLEMENTOS'),
        (num_ocio, 'OCIO Y TIEMPO LIBRE'),
        (num_otros, 'OTROS'),
        (num_restauracion, 'RESTAURACION'),
        (num_salud, 'SALUD'),
        (num_tecnologia, 'TECNOLOGIA')]
    return cpSector_filters_dataset

def getSectorFilterDataset(spark, cards_dataset):
    sales_given_commerce = cards_dataset.filter(F.col('CP_COMERCIO') == 'CP_COMERCIO')# sales_given_commerce.createTempView('sales_given_commerce')
    sales_given_commerce.createOrReplaceTempView("sales_given_commerce")
    num_alim = spark.sql("SELECT * FROM sales_given_commerce WHERE SECTOR == 'ALIMENTACION'").count() #num alimentacion
    num_auto = spark.sql("SELECT * FROM sales_given_commerce WHERE SECTOR == 'AUTO'").count() #num alimentacion
    num_belleza = spark.sql("SELECT * FROM sales_given_commerce WHERE SECTOR == 'BELLEZA'").count() #num alimentacion
    num_hogar = spark.sql("SELECT * FROM sales_given_commerce WHERE SECTOR == 'HOGAR'").count() #num alimentacion
    num_moda = spark.sql("SELECT * FROM sales_given_commerce WHERE SECTOR == 'MODA Y COMPLEMENTOS'").count() #num alimentacion
    num_ocio = spark.sql("SELECT * FROM sales_given_commerce WHERE SECTOR == 'OCIO Y TIEMPO LIBRE'").count() #num alimentacion
    num_otros = spark.sql("SELECT * FROM sales_given_commerce WHERE SECTOR == 'OTROS'").count() #num alimentacion
    num_restauracion = spark.sql("SELECT * FROM sales_given_commerce WHERE SECTOR == 'RESTAURACION'").count() #num alimentacion
    num_salud = spark.sql("SELECT * FROM sales_given_commerce WHERE SECTOR == 'SALUD'").count() #num alimentacion
    num_tecnologia = spark.sql("SELECT * FROM sales_given_commerce WHERE SECTOR == 'TECNOLOGIA'").count() #num alimentacion
    
    Sector_filters_dataset = [
        (sales_given_commerce.count(),'CARDS'),
        (num_alim, 'ALIMENTACION'),
        (num_auto, 'AUTO'),
        (num_belleza, 'BELLEZA'),
        (num_hogar, 'HOGAR'),
        (num_moda, 'MODA Y COMPLEMENTOS'),
        (num_ocio, 'OCIO Y TIEMPO LIBRE'),
        (num_otros, 'OTROS'),
        (num_restauracion, 'RESTAURACION'),
        (num_salud, 'SALUD'),
        (num_tecnologia, 'TECNOLOGIA')]
    return Sector_filters_dataset


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