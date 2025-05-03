# ETL_animal_data 📦🔄📊

Uno script Python completo per eseguire un processo ETL (Extract - Transform - Load) su dati di animali, partendo da un file CSV grezzo fino al caricamento dei dati puliti in un database MySQL.

## ⚙️ Funzionalità

- **Caricamento e pulizia dei dati da file CSV**
- **Rilevamento e sostituzione di valori anomali** (`"not determined"`)
- **Gestione dei valori mancanti**:
  - Per le **variabili categoriali**: sostituzione tramite **moda**
  - Per le **variabili quantitative**: imputazione tramite **mediana** calcolato su un raggruppamento
- **Conversione tipi di dati**
- **Gestione outliers**: sostituzione con limite inferiore o limite superiore
- **Gestione valori duplicati**: drop
- **Rimappatura delle etichette delle colonne** per coerenza con lo schema del database
- **Creazione automatica della tabella** MySQL se non esiste
- **Caricamento dei dati puliti** nel database
- **Estendibile tramite superclasse astratta `ModelloBase`** con metodi di analisi dei dati integrati

## 🧱 Architettura

Il modulo principale è composto da:
- `DatasetCleaner`: classe principale che eredita da `ModelloBase`, responsabile di tutta la logica di pulizia e trasformazione.
- Funzioni standalone per la **connessione al database**, **creazione della tabella** e **inserimento dei dati**.

## 🧪 Guida all'Uso

Consulta il file 'USAGE' per un percorso dettagliato che ti accompagna passo-passo nell’utilizzo del progetto ETL per trasformare e caricare dati di vendita in un database MySQL.

## 🔒 Licenza

Distribuito sotto licenza MIT. Vedi il file `LICENSE` per maggiori dettagli.
