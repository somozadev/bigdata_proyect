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
from pyspark.sql import SparkSession, column, dataframe
from pyspark.sql import functions as F
from pyspark.sql.types import  DateType, DoubleType,IntegerType,StringType 

@st.cache
def run_fxn(n: int) -> list:
    return range(n)
def main():
   #st.title("Almeria la ciudad!")
   #st.text("Texto: Hola Streamlit")
  
    spark = SparkSession.builder.getOrCreate()
    cards_dataset = spark.read.csv('cards.csv',sep = '|', header = True)
    weather_dataset = spark.read.csv('weather.csv', sep = ';', header = True)

    cards_dataset = ValueTypesSetup(cards_dataset)
    weather_dataset = ValueTypesSetup_weather(weather_dataset)


    listaDataframes = getFechaRangoTemperatura(spark, weather_dataset)

    listaFechasRango0 = list(listaDataframes[0].select('FECHA').toPandas()['FECHA'])
    listaFechasRango1 = list(listaDataframes[1].select('FECHA').toPandas()['FECHA'])
    listaFechasRango2 = list(listaDataframes[2].select('FECHA').toPandas()['FECHA'])

    #st.write(data[0].show(10))

    #st.write(ventasTemperatura(spark, cards_dataset, weather_dataset))

    components.iframe("https://api.mapbox.com/styles/v1/somozadev/ckw8o8s8l048914rrmowskqoy.html?title=false&access_token=pk.eyJ1Ijoic29tb3phZGV2IiwiYSI6ImNrdzU4b3V4ZmVtOGsybnM3YXF4ZzZzOW4ifQ.b6Pq5mbghDbGzNDuyR-rxQ&zoomwheel=false#13/36.842566/-2.462058", width= 720, height= 500,scrolling = False)    

    #Creacion de dataframes para la pesca de juntar los DataFrames
    DisplayDropdownsCPSector(spark, cards_dataset, '04001')
    #Grafica lineal de las ventas en el cp 04001
    df = pd.DataFrame(getCPSectorFilterDataset(spark, cards_dataset, '04001'), columns = ['Ventas', 'Comercio'])
    df = df.drop([0], axis=0)
    fig = px.histogram(df, x = 'Comercio', y = 'Ventas', color = 'Comercio', title='Ventas')
    st.plotly_chart(fig, use_container_width=True)

    DisplayDropdownsCPSector(spark, cards_dataset, '04002')
    #Grafica lineal de las ventas en el cp 04002
    df = pd.DataFrame(getCPSectorFilterDataset(spark, cards_dataset, '04002'), columns = ['Ventas', 'Comercio'])
    df = df.drop([0], axis=0)
    fig = px.histogram(df, x = 'Comercio', y = 'Ventas', color = 'Comercio', title='Ventas')
    st.plotly_chart(fig, use_container_width=True)

    DisplayDropdownsCPSector(spark, cards_dataset, '04003')
    #Grafica lineal de las ventas en el cp 04003
    df = pd.DataFrame(getCPSectorFilterDataset(spark, cards_dataset, '04003'), columns = ['Ventas', 'Comercio'])
    df = df.drop([0], axis=0)
    fig = px.histogram(df, x = 'Comercio', y = 'Ventas', color = 'Comercio', title='Ventas')
    st.plotly_chart(fig, use_container_width=True)

    DisplayDropdownsCPSector(spark, cards_dataset, '04004')
    #Grafica lineal de las ventas en el cp 04004
    df = pd.DataFrame(getCPSectorFilterDataset(spark, cards_dataset, '04004'), columns = ['Ventas', 'Comercio'])
    df = df.drop([0], axis=0)
    fig = px.histogram(df, x = 'Comercio', y = 'Ventas', color = 'Comercio', title='Ventas')
    st.plotly_chart(fig, use_container_width=True)

    DisplayDropdownsCPSector(spark, cards_dataset, '04005')
    #Grafica lineal de las ventas en el cp 04005
    df = pd.DataFrame(getCPSectorFilterDataset(spark, cards_dataset, '04005'), columns = ['Ventas', 'Comercio'])
    df = df.drop([0], axis=0)
    fig = px.histogram(df, x = 'Comercio', y = 'Ventas', color = 'Comercio', title='Ventas')
    st.plotly_chart(fig, use_container_width=True)

    DisplayDropdownsCPSector(spark, cards_dataset, '04006')
    #Grafica lineal de las ventas en el cp 04006
    df = pd.DataFrame(getCPSectorFilterDataset(spark, cards_dataset, '04006'), columns = ['Ventas', 'Comercio'])
    df = df.drop([0], axis=0)
    fig = px.histogram(df, x = 'Comercio', y = 'Ventas', color = 'Comercio', title='Ventas')
    st.plotly_chart(fig, use_container_width=True)

    DisplayDropdownsCPSector(spark, cards_dataset, '04007')
    #Grafica lineal de las ventas en el cp 04007
    df = pd.DataFrame(getCPSectorFilterDataset(spark, cards_dataset, '04007'), columns = ['Ventas', 'Comercio'])
    df = df.drop([0], axis=0)
    fig = px.histogram(df, x = 'Comercio', y = 'Ventas', color = 'Comercio', title='Ventas')
    st.plotly_chart(fig, use_container_width=True)

    DisplayDropdownsCPSector(spark, cards_dataset, '04008')
    #Grafica lineal de las ventas en el cp 04008
    df = pd.DataFrame(getCPSectorFilterDataset(spark, cards_dataset, '04008'), columns = ['Ventas', 'Comercio'])
    df = df.drop([0], axis=0)
    fig = px.histogram(df, x = 'Comercio', y = 'Ventas', color = 'Comercio', title='Ventas')
    st.plotly_chart(fig, use_container_width=True)

    DisplayDropdownsCPSector(spark, cards_dataset, '04009')
    #Grafica lineal de las ventas en el cp 04009
    df = pd.DataFrame(getCPSectorFilterDataset(spark, cards_dataset, '04009'), columns = ['Ventas', 'Comercio'])
    df = df.drop([0], axis=0)
    fig = px.histogram(df, x = 'Comercio', y = 'Ventas', color = 'Comercio', title='Ventas')
    st.plotly_chart(fig, use_container_width=True)

    #printFilters(getSectorFiltersDataset(cards_dataset),cards_dataset.count())  #% de ventas por sector 
    with st.expander("Ver ventas por sector"):
        for i in (getSectorFiltersDataset(cards_dataset))[1:]:
            st.write(i[1] ,(getPercentaje(i[0].count(), cards_dataset.count())), "%")
            st.progress(int(getPercentaje(i[0].count(), cards_dataset.count())))
            
    


    #printFilters(getCpComercioFiltersDataset(cards_dataset),cards_dataset.count()) #% de ventas por tienda (cp)
    with st.expander("Ver ventas por codigo postal"):
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

def ValueTypesSetup_weather(database_weather):
    database_weather = database_weather.withColumn('FECHA', F.col('FECHA').cast(DateType()))
    database_weather = database_weather.withColumn('TMed', F.col('TMed').cast(DoubleType()))
    database_weather = database_weather.withColumn('Precip', F.col('Precip').cast(DoubleType()))
    return database_weather

    
def SectorPercentajePerCommerce(dataset): #hay  9 comercios (04001 - 04009)
    print("Sector por comercio es: ", dataset.filter(F.col('CP_CLIENTE') == 'CP_CLIENTE').groupBy('CP_CLIENTE').count())
    
def DisplayDropdownsCPSector(spark, cards_dataset,cp):
    titulo = ("Ver ventas por categoría del CP: {}".format(cp))
    with st.expander(titulo):
        aux = getCPSectorFilterDataset(spark,cards_dataset,cp)
        st.write(cp ," ha tenido un total de ", aux[0][0] , "ventas.")
        for i in(aux)[1:]:
            st.write(i[1],(getPercentaje(i[0],(aux[0][0]))), "%")
            st.write("Con un total de", i[0], "ventas en esta categoría.")

            st.progress(int(getPercentaje(i[0],(aux[0][0]))))
 

def getCPSectorFilterDataset(spark, cards_dataset, cp):
    sales_given_commerce = cards_dataset.filter(F.col('CP_COMERCIO') == str(cp))# sales_given_commerce.createTempView('sales_given_commerce')
    sales_given_commerce.createOrReplaceTempView("sales_given_commerce")
    num_alim = spark.sql("SELECT * FROM sales_given_commerce WHERE SECTOR == 'ALIMENTACION'").count() #num alimentacion
    num_auto = spark.sql("SELECT * FROM sales_given_commerce WHERE SECTOR == 'AUTO'").count() #num auto
    num_belleza = spark.sql("SELECT * FROM sales_given_commerce WHERE SECTOR == 'BELLEZA'").count() #num belleza
    num_hogar = spark.sql("SELECT * FROM sales_given_commerce WHERE SECTOR == 'HOGAR'").count() #num hogar
    num_moda = spark.sql("SELECT * FROM sales_given_commerce WHERE SECTOR == 'MODA Y COMPLEMENTOS'").count() #num moda
    num_ocio = spark.sql("SELECT * FROM sales_given_commerce WHERE SECTOR == 'OCIO Y TIEMPO LIBRE'").count() #num ocio
    num_otros = spark.sql("SELECT * FROM sales_given_commerce WHERE SECTOR == 'OTROS'").count() #num otros
    num_restauracion = spark.sql("SELECT * FROM sales_given_commerce WHERE SECTOR == 'RESTAURACION'").count() #num restauracion
    num_salud = spark.sql("SELECT * FROM sales_given_commerce WHERE SECTOR == 'SALUD'").count() #num salud
    num_tecnologia = spark.sql("SELECT * FROM sales_given_commerce WHERE SECTOR == 'TECNOLOGIA'").count() #num tecnologia
    
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

def getFechaRangoTemperatura(spark, weather_dataset):
    database = weather_dataset
    database.createOrReplaceTempView("database")
    fechas0 = []
    fechas0 = spark.sql("SELECT FECHA FROM database WHERE TMed >= 0.0 AND TMed < 10.0")
    fechas1 = spark.sql("SELECT FECHA FROM database WHERE TMed >= 10.01 AND TMed < 20.0")
    fechas2 = spark.sql("SELECT FECHA FROM database WHERE TMed >= 20.01 AND TMed < 30.0")

    #cpSector_filters_dataset = [
    #    (fechas0, 'rango 0-10'),
    #    (fechas1, 'rango 10-20'),
    #    (fechas2, 'rango 20-30')]
    cpSector_filters_dataset = []
    cpSector_filters_dataset.append(fechas0)
    cpSector_filters_dataset.append(fechas1)
    cpSector_filters_dataset.append(fechas2)
    return cpSector_filters_dataset


def ventasTemperatura(spark, cards_dataset, weather_dataset):
    fechas = getFechaRangoTemperatura(spark, weather_dataset)
    ventas = 0
    cards = cards_dataset
    cards.createOrReplaceTempView("cards")
    for fecha in fechas:
        absa = spark.sql("SELECT * FROM cards WHERE FECHA = %s", (fecha)).count()
        ventas += absa
    return ventas

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