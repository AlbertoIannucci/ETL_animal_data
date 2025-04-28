from modello_base import ModelloBase
import pandas as pd
import numpy as np
from dateutil import parser # Libreria più intelligente a capire formati strani di date senza doverli pre-processare manualmente.
import pymysql

class DatasetCleaner(ModelloBase):

    def __init__(self, dataset_path):
        self.dataframe = pd.read_csv(dataset_path, sep=';')
        self.dataframe_sistemato = self.sistemazione()

    # Metodo di sistemazione del dataframe
    def sistemazione(self):
        # Copia del dataframe
        df_sistemato = self.dataframe.copy()

        # Drop Animal code
        df_sistemato = df_sistemato.dropna(axis=1, how="all")

        # Drop Animal name
        df_sistemato = df_sistemato.drop(["Animal name"], axis=1)

        # Sistemazione variabile Animal type
        animal_type_mapping = {
            "European bison™": "European bison",
            "European bisson": "European bison",
            "European buster": "European bison",
            "lynx?": "lynx",
            "red squirrel": "red squirel",
            "red squirrell": "red squirel",
            "wedgehod": "hedgehog",
            "ledgehod": "hedgehog"
        }
        df_sistemato["Animal type"] = df_sistemato["Animal type"].replace(animal_type_mapping)

        # Sistemazione variabile Country
        country_mapping = {
            "PL": "Poland",
            "HU": "Hungary",
            "Hungry": "Hungary",
            "DE": "Germany",
            "Czech": "Czech Republic",
            "CZ": "Czech Republic",
            "CC": "Australia"
        }
        df_sistemato["Country"] = df_sistemato["Country"].replace(country_mapping)

        # Sistemazione variabile Observation date
        df_sistemato["Observation date"] = df_sistemato["Observation date"].apply(
            lambda x: parser.parse(x, dayfirst=True).date() if pd.notnull(x) else np.nan)
        df_sistemato["Observation date"] = pd.to_datetime(df_sistemato["Observation date"])

        # Sostituzione valori nan camuffati variabile Gender
        df_sistemato["Gender"] = df_sistemato["Gender"].replace("not determined", np.nan)

        # Sostituzione valori nan quantitative
        variabili_quantitative = ["Weight kg", "Body Length cm", "Latitude", "Longitude"]
        for col in df_sistemato.columns:
            if col in variabili_quantitative:
                df_sistemato[col] = df_sistemato[col].fillna(df_sistemato[col].median())

        # Sostituzione outliers
        colonne_con_outliers = ["Weight kg", "Body Length cm"]
        for col in colonne_con_outliers:
            q1 = df_sistemato[col].quantile(0.25)
            q3 = df_sistemato[col].quantile(0.75)
            iqr = q3 - q1
            limite_inferiore = q1 - 1.5 * iqr
            limite_superiore = q3 + 1.5 * iqr
            df_sistemato[col] = np.where(df_sistemato[col] < limite_inferiore, limite_inferiore, df_sistemato[col])
            df_sistemato[col] = np.where(df_sistemato[col] > limite_superiore, limite_superiore, df_sistemato[col])

        # Drop valori nan
        df_sistemato = df_sistemato.dropna()

        # Drop valori duplicati se esistono
        if df_sistemato.duplicated().any():
            df_sistemato = df_sistemato.drop_duplicates().reset_index(drop=True)

        # Rimappatura etichette
        df_sistemato = df_sistemato.rename(columns={
            "Animal type":"animal_type",
            "Country":"country",
            "Weight kg":"weight_kg",
            "Body Length cm":"body_length_cm",
            "Gender":"gender",
            "Latitude":"latitude",
            "Longitude":"longitude",
            "Observation date":"observation_date",
            "Data compiled by":"data_compiled_by"
        })

        return df_sistemato

def getconnection():
    return pymysql.connect(
        host="localhost",
        port=3306,
        user="root",
        password="",
        database="animal_db"
    )

def creazione_tabella():
    try:
        connection = getconnection()
        try:
            with connection.cursor() as cursor:
                sql = ("CREATE TABLE IF NOT EXISTS animal("
                       "id_animal INT AUTO_INCREMENT PRIMARY KEY,"
                       "animal_type VARCHAR(50) NOT NULL,"
                       "country VARCHAR(25) NOT NULL,"
                       "weight_kg FLOAT NOT NULL,"
                       "body_length_cm FLOAT NOT NULL,"
                       "gender VARCHAR(6) NOT NULL,"
                       "latitude FLOAT NOT NULL,"
                       "longitude FLOAT NOT NULL,"
                       "observation_date DATE NOT NULL,"
                       "data_compiled_by VARCHAR(50) NOT NULL"
                       ");")
                cursor.execute(sql)
                connection.commit()
                return cursor.rowcount
        finally:
            connection.close()
    except Exception as e:
        print(e)
        return None

def load(df):
    try:
        connection = getconnection()
        try:
            with connection.cursor() as cursor:
                # Prepara una lista di tuple con i dati da inserire
                values = [
                    (
                        row["animal_type"],
                        row["country"],
                        row["weight_kg"],
                        row["body_length_cm"],
                        row["gender"],
                        row["latitude"],
                        row["longitude"],
                        row["observation_date"],
                        row["data_compiled_by"]
                    )
                    for _, row in df.iterrows()
                ]
                
                # SQL per l'inserimento dei dati
                sql = """
                INSERT INTO animal (animal_type, country, weight_kg, body_length_cm, gender, latitude, 
                                    longitude, observation_date, data_compiled_by) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                # Esegui l'inserimento in batch con executemany
                cursor.executemany(sql, values)
                connection.commit()
                print("Dati caricati correttamente")
        finally:
            connection.close()
    except Exception as e:
        print(e)
        return None


modello = DatasetCleaner("../Dataset/dataset.csv")
# Passo 1. Specifico nella creazione del dataframe che i dati sono separati da ';'
# Passo 2. Analisi generali del dataset
#modello.analisi_generali(modello.dataframe)
# Risultati:
# Osservazioni= 1011; Variabili= 11; Tipi= object e float; Valori nan= presenti
# Presenza di variabile nulla (Animal code)
# Presenza di variabile con >50% valori nan (Animal name)
# Passo 3. Drop Animal code e Animal name
modello.analisi_generali(modello.dataframe_sistemato)
# Passo 4. Analisi dei valori univoci
# modello.analisi_valori_univoci(modello.dataframe_sistemato, ["Weight kg", "Body Length cm",
#                                                              "Latitude", "Longitude"])
# Risultati:
# Animal type diversi modi di scrivere: European bison, lynx, red squirrel e hedgehog
# Country diversi modi di srivere: Poland, Hungary, Czech Republic, Germany e Australia
# Gender valori nan nascosti con not determined
# Observation date date separate da '.' in formato dd/mm/aaaa, maggio scritto in diversi formati (May e may)
# Passo 5. Sistemazione variabile  Animal Type
# Passo 6. Sistemazione variabile Country
# Passo 7. Sistemazione variabile Observation date
# Passo 8. Sostituzione valori nan camuffati variabile Gender
# Passo 9. Strategia valori nan variabili quantitative
# Variabile con più valori nan = Latitude (913) -> Valori nan = 1007-913= 94
# 94 nan corrispondono al 9.33% del dataset -> no drop
# Passo 10. Analisi degli outliers
# modello.individuazione_outliers(modello.dataframe_sistemato, ["Animal type", "Country",
#                                                               "Gender", "Observation date", "Data compiled by"])
# Risultati:
# Weight kg= 13.30%
# Body Length= 14.59%
# Latitude= 0.59%
# Longitude = 0.39%
# Passo 11. Sostituzione valori nan quantitative con mediana
# Passo 12. Analisi degli outliers
# Risultati:
# Weight kg= 13.30%
# Body Length= 14.59%
# Latitude= 0.59%
# Longitude = 0.39%
# Le prime due varibili sono al limite (<10/15%)
# Passo 13. Sostituzione gli outliers con il limite inferiore o il limite superiore
# Risultati:
# Weight kg= 0%
# Body Length= 0%
# Latitude= 0.59%
# Longitude = 0.39%
# Passo 14. Strategia valori nan variabili categoriali
# Variabile con più valori nan = Gender (987) -> Valori nan = 1007-987= 20
# 20 nan corrispondono al 1.98% del dataset -> drop
# Passo 15. Controllo e drop dei valori duplicati
# Passo 16. Rimappatura etichette
# Passo 17. Stabilisco una connessione con il database animal_db
# Passo 18. Creao la tabella animal
#creazione_tabella()
# Passo 19. Caricamento dei dati
#load(modello.dataframe_sistemato)
