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

        # Drop variabile nulla Animal Code
        df_sistemato = df_sistemato.dropna(axis=1, how="all")

        # Drop variabile Animal name
        df_sistemato = df_sistemato.drop(["Animal name"], axis=1)

        # Sistemazione Animal type
        animal_type_mapping = {
            "European bison™": "European bison",
            "European bisson": "European bison",
            "European buster": "European bison",
            "lynx?": "lynx",
            "red squirel": "red squirrel",
            "red squirrell": "red squirrel",
            "wedgehod": "hedgehog",
            "ledgehod": "hedgehog"
        }
        df_sistemato["Animal type"] = df_sistemato["Animal type"].replace(animal_type_mapping)

        # Sistemazione Country
        country_mapping = {
            "PL": "Poland",
            "HU": "Hungary",
            "Hungry": "Hungary",
            "DE": "Germany",
            "Czech": "Czech Republic",
            "CZ": "Czech Republic",
            "CC": "Austria",
            "Australia":"Austria"
        }
        df_sistemato["Country"] = df_sistemato["Country"].replace(country_mapping)

        # Sistemazione variabile Gender
        df_sistemato["Gender"] = df_sistemato["Gender"].fillna("not determined")

        # Rimappatura variabile Observation date
        df_sistemato["Observation date"] = df_sistemato["Observation date"].apply(
            lambda x: parser.parse(x, dayfirst=True).date() if pd.notnull(x) else np.nan)
        df_sistemato["Observation date"] = pd.to_datetime(df_sistemato["Observation date"])

        # Sistemazione Weight kg
        df_sistemato["Weight kg"] = df_sistemato["Weight kg"].abs()

        # Sistemazione Body Length cm
        df_sistemato["Body Length cm"] = df_sistemato["Body Length cm"].abs()

        # Sistemazione Latitude
        df_sistemato["Latitude"] = df_sistemato["Latitude"].abs()

        # Sostituzione nan variabili categoriali con la moda
        variabili_categoriali = ["Animal type", "Country"]
        for col in df_sistemato.columns:
            if col in variabili_categoriali:
                df_sistemato[col] = df_sistemato[col].fillna(df_sistemato[col].mode()[0])

        # Sostituzione nan variabili quantitative con la mediana
        variabili_quantitative = ["Weight kg", "Body Length cm", "Latitude", "Longitude"]
        for col in variabili_quantitative:
            df_sistemato[col] = df_sistemato.groupby(["Animal type", "Country"])[col].transform(
                lambda x: x.fillna(x.median())
            )

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

        # Drop valori duplicati se esistono
        if df_sistemato.duplicated().any():
            df_sistemato = df_sistemato.drop_duplicates().reset_index(drop=True)

        # Rimappatura etichette
        df_sistemato = df_sistemato.rename(columns={
            "Animal type": "animal_type",
            "Country": "country",
            "Weight kg": "weight_kg",
            "Body Length cm": "body_length_cm",
            "Gender": "gender",
            "Latitude": "latitude",
            "Longitude": "longitude",
            "Observation date": "observation_date",
            "Data compiled by": "data_compiled_by"
        })

        return df_sistemato

# Funzione per stabile una connessione con il db
def getconnection():
    return pymysql.connect(
        host="localhost",
        port=3306,
        user="root",
        password="",
        database="animal_db"
    )

# Funzione per creare una tabella nel db
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
                print("Tabella animal creata con successo")
        finally:
            connection.close()
    except Exception as e:
        print(e)
        return None

# Funzione per caricare i dati nel db
def load(df):
    try:
        connection = getconnection()
        try:
            with connection.cursor() as cursor:
                # Preparo una lista di tuple con i dati da inserire
                valori = [
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

                # Eseguo l'inserimento in batch con executemany
                cursor.executemany(sql, valori)
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
#modello.analisi_generali(modello.dataframe_sistemato)
# Passo 4. Analisi valori univoci variabili categoriale
#modello.analisi_valori_univoci(modello.dataframe_sistemato, ["Weight kg", "Body Length cm",
#                                                             "Latitude", "Longitude"])
# Risultati:
# Animal type: diversi nomi per la stessa specie -> operazione di sostituzione
# Country: diversi nomi per lo stesso paese -> operazione di sostituzione
# Gender: presenste la categoria not determined -> sostituzione nan con not determined
# Observation date: date scritte in formato diverso -> rimappatura date
# Passo 5. Sistemazione variabile Animal type
# Passo 6. Sistemazione variabile Country
# Passo 7. Sistemazione variabile Gender
# Passo 8. Rimappatura variabile Observation date
# Passo 9. Analisi valori univoci variabile quantitative
#modello.analisi_valori_univoci(modello.dataframe_sistemato, ["Animal type", "Country", "Gender",
#                                                             "Observation date", "Data compiled by"])
# Risultati:
# Weight kg: alcuni valori sono negativi -> Sostituisco con il valore assoluto
# Body Length cm: alcuni valori sono negativi -> Sostituisco con il valore assoluto
# Latitude: essendo che i Paesi sono Paesi Europei sopra l'equatore, non possono avere latitudine negativa
# -> Sostituisco con il valore assoluto
# Passo 10. Sistemazione variabile Weight kg
# Passo 11. Sitemazione variabile Body Lenght cm
# Passo 12. Sistemazione variabile Latitude
# Passo 13. Strategia valori nan
# Animal type= 20 nan (1.97% del dataset)
# Country = 12 nan (1.18% del dataset)
# Weight kg= 27 nan (2.67% del dataset)
# Body Length cm= 27 nan (2.67% del dataset)
# Latitude= 98 nan (9.8% del dataset)
# Longitude= 98 nan (9.8% del dataset)
# Drop: 28% del dataset in meno -> No drop
# Passo 14. Sostituisco i valori nan delle variabili categoriali con la moda
# Passo 15. Analisi outliers
# modello.individuazione_outliers(modello.dataframe_sistemato, ["Animal type", "Country", "Gender",
#                                                              "Observation date", "Data compiled by"])
# Risultati:
# Weight kg= 13.30%
# Body Length= 13.94%
# Latitude= 0.59%
# Longitude = 0.39%
# Passo 16. Sostituisco nan variabili quantitative con la mediana
# La mediana è calcolata per gruppo: Animal type e Country
# Passo 17. Analisi outliers
# modello.individuazione_outliers(modello.dataframe_sistemato, ["Animal type", "Country", "Gender",
#                                                              "Observation date", "Data compiled by"])
# Risultati:
# Weight kg= 13.25%
# Body Length= 13.94%
# Latitude= 0.59%
# Longitude = 0.39%
# Outliers non cresicuti. Mantengo la sostituzuone.
# Le prime due varibili sono al limite (<10/15%)
# Passo 18. Sostituzione gli outliers con il limite inferiore o il limite superiore
# Passo 19. Controllo e drop dei valori duplicati
# Passo 20. Rimappatura etichette
# Passo 21. Stabilisco una connessione con il database animal_db
# Passo 22. Creao la tabella animal
#creazione_tabella()
# Passo 23. Caricamento dei dati
#load(modello.dataframe_sistemato)