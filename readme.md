# Raportowanie i analiza wydajności – README

Ten moduł odpowiada za **zbieranie danych testowych (RAW)**, ich **agregację** oraz **generowanie raportu PDF**. Może działać zarówno lokalnie (testowo), jak i jako worker podłączony do reszty systemu przez RabbitMQ.

---

## 📁 Struktura projektu

```
aggregates.py
main.py
rabbit_worker.py
report.pdf
repo.py
report_pdf.py
requirements.txt
sample_request_results.json
schema.sql
storage.py
```

---

## 🧠 Odpowiedzialności plików

### `main.py`
- Lokalny **test end‑to‑end** bez RabbitMQ.
- Inicjalizuje bazę danych (`init_db`).
- Wczytuje przykładowe dane z `sample_request_results.json`.
- Zapisuje RAW do bazy.
- Liczy agregaty (`compute_aggregates`).
- Odczytuje dane przez `ReportRepository`.
- (Opcjonalnie) generuje PDF.

Używany wyłącznie do testów i debugowania pipeline’u.

---

### `storage.py`
- Warstwa zapisu danych RAW.
- `init_db(db_path, schema.sql)` – tworzy tabele i indeksy.
- `insert_raw_result(db_path, dto)` – zapisuje **jeden** event requestu do bazy.

Nie zawiera logiki agregacji ani raportowania.

---

### `schema.sql`
- Definicja struktury bazy SQLite:
  - `request_results` – surowe dane (RAW)
  - `job_summary` – agregaty globalne per job
  - `endpoint_summary` – agregaty per endpoint + metoda
  - `timeseries_summary` – agregaty czasowe (bucketed)
- Indeksy pod szybkie zapytania raportowe.

---

### `aggregates.py`
- Logika **agregacji danych**.
- `compute_aggregates(db_path, job_id, bucket_seconds)`:
  - czyta RAW z `request_results`
  - liczy metryki (avg, p50, p90, p95, p99, max)
  - zapisuje wyniki do tabel `*_summary` (UPSERT)
- `bucket_seconds` określa rozdzielczość analizy czasowej.

---

### `repo.py`
- Read‑only warstwa dostępu do danych przygotowanych pod raport.
- `ReportRepository`:
  - `get_job_summary(job_id)`
  - `get_endpoint_summary(job_id)`
  - `get_timeseries_overall(job_id, bucket_seconds)`

Generator PDF korzysta wyłącznie z tego repozytorium.

---

### `report_pdf.py`
- Generator raportu PDF (ReportLab).
- `generate_report_pdf(db_path, job_id, out_path, bucket_seconds)`:
  - pobiera dane z tabel `*_summary`
  - generuje raport PDF:
    - strona tytułowa
    - podsumowanie testu
    - statystyki per endpoint
    - analiza trendu w czasie
- Obsługuje UTF‑8 (polskie znaki) przez fonty DejaVu.

---

### `rabbit_worker.py`
- Worker integracyjny z RabbitMQ (tryb produkcyjny).
- Subskrybuje dwie kolejki:

**`perf.raw`**
- przyjmuje **pojedynczy DTO lub listę DTO**
- zapisuje dane RAW do bazy (`insert_raw_result`)

**`perf.ctrl`**
- odbiera komendę `{"cmd": "job_end", "job_id": X}`
- publikuje komunikat gotowości na `perf.ready`

Worker **nie generuje PDF** – tylko synchronizuje dane.

---

### `sample_request_results.json`
- Przykładowe dane testowe (lista eventów requestów).
- Używane przez `main.py` do testów lokalnych.

---

### `report.pdf`
- Przykładowy wygenerowany raport (artefakt testowy).

---

### `requirements.txt`
Minimalne zależności:
```
reportlab
pika   # tylko jeśli używany RabbitMQ
```

---

## 🔄 Przepływ danych (high‑level)

1. Backend / testy → wysyłają dane RAW
2. `storage.py` zapisuje RAW do SQLite
3. Po zakończeniu joba → `compute_aggregates`
4. Agregaty zapisane w `*_summary`
5. `report_pdf.py` generuje PDF na podstawie agregatów

---

## ℹ️ Uwagi integracyjne

- Moduł **nie wymaga zmian**, jeśli backend dostarcza dane w uzgodnionym DTO.
- RabbitMQ jest opcjonalny – cały pipeline działa lokalnie.
- Projekt celowo rozdziela:
  - RAW data
  - agregację
  - raportowanie
  - komunikację

Dzięki temu łatwo go podłączyć do reszty systemu.

