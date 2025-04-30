from abc import ABC


class ModelloBase(ABC):

    # Metodo per effettuare un'analisi generale sul dataframe
    @staticmethod
    def analisi_generali(df):
        print("*******ANALISI GENERALI DATAFRAME**********")
        # Mostra le prime cinque osservazioni del dataframe
        print("Prime cinque osservazioni:", df.head().to_string(), sep='\n')
        # Mostra le ultime cinque osservazioni del dataframe
        print("Ultime cinque osservazioni:", df.tail().to_string(), sep='\n')
        # Mostra informazioni generali sul dataframe (tipo di dati, numero di valori non nulli, ecc.)
        print("Informazioni generali del dataframe:")
        df.info()

    # Metodo per controllare i valori univoci nelle variabili
    @staticmethod
    def analisi_valori_univoci(df, variabili_da_droppare=None):
        print("*******VALORI UNIVOCI DATAFRAME**********")
        # Se ci sono variabili da escludere, rimuovile dal dataframe
        if variabili_da_droppare:
            df = df.drop(variabili_da_droppare, axis=1)
        # Itera su ogni colonna del dataframe
        for col in df.columns:
            # Mostra il numero di valori univoci nella colonna
            print(f"In colonna {col} abbiamo {df[col].nunique()} valori univoci")
            # Elenca ogni valore unico presente nella colonna
            for value in df[col].unique():
                print(value)

    # Metodo per analizzare gli indici statistici descrittivi del dataframe
    @staticmethod
    def analisi_indici_statistici(df):
        print("*******ANALISI INDICI DATAFRAME**********")
        # Calcola e mostra gli indici statistici generali (media, deviazione standard, ecc.)
        indici_generali = df.describe()
        print("Indici generali: ", indici_generali.to_string(), sep='\n')
        # Per ogni colonna, mostra il valore della moda (il più frequente)
        for col in df.columns:
            print(f"Moda colonna {col}: {df[col].mode().iloc[0]}")

    # Metodo per individuare gli outliers nelle variabili quantitative
    @staticmethod
    def individuazione_outliers(df, variabili_da_droppare=None):
        print("*******INDIVIDUAZIONE OUTLIERS DATAFRAME**********")
        # Se ci sono variabili da escludere, rimuovile dal dataframe
        if variabili_da_droppare:
            df = df.drop(variabili_da_droppare, axis=1)
        # Itera su ogni colonna del dataframe
        for col in df.columns:
            # Calcola il primo e terzo quartile (Q1 e Q3) della colonna
            q1 = df[col].quantile(0.25)
            q3 = df[col].quantile(0.75)
            # Calcola l'intervallo interquartile (IQR)
            iqr = q3 - q1

            # Calcola i limiti inferiore e superiore per l'individuazione degli outliers
            limite_inferiore = q1 - 1.5 * iqr
            limite_superiore = q3 + 1.5 * iqr

            # Seleziona i dati che sono al di fuori dei limiti, ossia gli outliers
            outliers = df[(df[col] < limite_inferiore) | (df[col] > limite_superiore)]
            # Mostra quanti outliers ci sono e la loro percentuale sul totale dei dati
            print(
                f"Nella colonna {col} sono presenti n {len(outliers)} ({len(outliers) / len(df) * 100:.4f}%) outliers")
