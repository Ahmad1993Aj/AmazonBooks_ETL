Projekt: AmazonBooks_ETL
======================

Kurzbeschreibung
----------------
Dieses Projekt ist eine einfache Airflow-basierte ETL-Pipeline, die Buchdaten von Amazon (Suchergebnisse für "data engineering books") extrahiert, in eine kleine Zwischenform (Pandas DataFrame / XCom) überführt und in eine Postgres-Datenbank schreibt.

Zielgruppe
---------
- Data Engineers, die eine Beispiel-ETL mit Airflow, Web-Scraping und Postgres sehen möchten.
- Entwickler, die ein kleines Beispielprojekt für Airflow-DAGs und Provider-Hooks suchen.

Architektur & Komponenten
-------------------------
- Airflow DAG: `dags/dag.py`
  - DAG-ID: `Fetch_and_store_amazon_books`
  - Schedule: täglich (`@daily`), Startdatum 2025-01-01
  - Tasks (PythonOperator):
    1. `create_books_table` — erstellt die Zieltabelle in Postgres (mittels `PostgresHook`).
    2. `fetch_amazon_books` — scrapt Amazon-Seiten nach Büchern (Requests + BeautifulSoup), wandelt Ergebnisse in ein Pandas DataFrame um und pusht die Daten als XCom (`key: amazonbooks`).
    3. `insert_books_into_postgres` — liest XCom-Daten und schreibt sie in die Postgres-Tabelle mittels `PostgresHook`.
  - Die Task-Abhängigkeiten sind: `create_books_table` -> `fetch_amazon_books` -> `insert_books_into_postgres`.

- Scraping
  - HTTP-Client: `requests`
  - HTML-Parsing: `beautifulsoup4` (BeautifulSoup)
  - User-Agent/Header-Set: in `dags/dag.py` definiert
  - Für direkte Tests ruft `get_amazon_data_books(num_books)` den Scraper zurück (gibt Daten zurück, wenn kein Airflow `ti` übergeben wird).

- Persistenz
  - Postgres: Verbindung über Airflow Connection mit `conn_id='books_connection'`.
  - Tabelle: `books` mit Spalten (id, title, authors, price, rating)

Datei-/Ordnerstruktur (wichtigste Dateien)
-----------------------------------------
- `dags/dag.py` — Haupt-DAG und Task-Implementierungen
- `docker-compose.yaml` — (vorhanden) kann zur lokalen Airflow/Postgres-Ausführung genutzt werden
- `config/airflow.cfg` — (projekt-spezifische Airflow-Konfiguration)
- `plugins/`, `logs/` — Airflow-Standardordner

Abhängigkeiten
--------------
Wichtigste Python-Packages (empfohlenes `requirements.txt`):
- apache-airflow
- apache-airflow-providers-postgres
- pandas
- requests
- beautifulsoup4
- psycopg2-binary

Installation (lokal)
--------------------
1. Python-Umgebung erstellen (empfohlen: venv)
   - Windows (cmd.exe):

```cmd
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
```

2. Airflow-Initialisierung (oder `docker-compose` nutzen)
   - Wenn Airflow lokal installiert ist:

```cmd
set AIRFLOW_HOME=%CD%\airflow_home
airflow db init
```

3. Postgres starten (z. B. Docker / docker-compose). Das Projekt enthält `docker-compose.yaml`, damit kann ein Postgres-Container zusammen mit Airflow gestartet werden.

Airflow Connection konfigurieren
-------------------------------
Create a connection named `books_connection` that zeigt auf Ihre Postgres-Datenbank. Beispiel (CLI):

```cmd
airflow connections add "books_connection" --conn-uri "postgresql://username:password@host:5432/dbname"
```

Hinweis: Ersetzen Sie `username`, `password`, `host`, `dbname` durch Ihre Werte.

Schnelltest / Entwicklung
------------------------
- Funktion ohne Airflow-Test:
  - `get_amazon_data_books` ist so implementiert, dass es beim direkten Aufruf (ohne `ti`) die Daten zurückgibt. Zum Testen in einer Python-Shell:

```cmd
python -c "from dags.dag import get_amazon_data_books; import json; print(json.dumps(get_amazon_data_books(5), indent=2))"
```

- Direktes Testen der DB-Insert-Logik erfordert eine konfigurierte Airflow-Connection. Alternativ können Sie die `insert_book_data_into_postgres`-Funktion lokal anpassen, um eine direkte psycopg2-Verbindung zu verwenden.

Troubleshooting / Hinweise
--------------------------
- Scraping von Amazon kann fehlschlagen oder Amazon kann die Seitenstruktur ändern. Die Selektoren in `dags/dag.py` (z. B. Klassen `s-result-item`, `a-text-normal`, `a-price-whole`) sind fragil und können angepasst werden.
- Beim Betrieb in Airflow stellen Sie sicher, dass:
  - Die `books_connection` richtig gesetzt ist.
  - Die Airflow-Umgebung Internetzugang hat (für Scraping).
  - Eventuell Rate-Limits/Blockierungen seitens Amazon berücksichtigt werden.
- Logging: Airflow-Logs befinden sich im `logs/`-Ordner; prüfen Sie die Task-Logs bei Fehlern.

Sicherheit & Legal
------------------
- Dieses Projekt führt Web-Scraping auf Amazon durch. Prüfen Sie die Nutzungsbedingungen von Amazon und beachten Sie rechtliche Einschränkungen und robots.txt-Richtlinien. Für produktive Projekte ist es besser, offizielle APIs oder lizenzierte Datenquellen zu verwenden.

Nächste sinnvolle Schritte (Empfehlungen)
----------------------------------------
- Tests hinzufügen (Unit-Tests für Parsing-Logik, Integrationstest für DB-Insert mit Test-DB)
- Robustere Selektoren/strategien für das Scraping (z. B. Amazon Product Advertising API, oder Headless-Browser mit Resilienz)
- Fehlerbehandlung verbessern (Retries, Backoff, Dead-letter-Mechanismus)
- Optional: Schema-Migrationstool (Alembic) für die `books`-Tabelle

Lizenz
------
- (Fügen Sie hier Ihre gewünschte Lizenz hinzu.)

Kontakt
-------
- Bei Fragen: Projekt-Repository / Projektverantwortlicher.

