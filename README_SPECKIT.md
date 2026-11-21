# Minimalistyczny Spec-Driven Development (1 plik `.md` na moduł)

W projekcie stosujemy minimalistyczne podejście Spec-Driven: **1 krótki plik `docs/<moduł>.md` (po angielsku, 40–60 linijek)**

---

## Na co automatycznie patrzy agent AI?

1. **[`AGENTS.md`](AGENTS.md)** (w głównym katalogu repozytorium) – **Konstytucja projektu ładowana automatycznie** przez agenta AI przy starcie każdej rozmowy.
2. **[`src/common/schema.py`](src/common/schema.py) oraz [`docs/event-schema.md`](docs/event-schema.md)** – wspólny kontrakt danych dla SQS FIFO, warstwy Bronze i warstwy Silver.
3. **[`docs/<module-name>.md`](docs/_module-template.md)** – krótki (40–60 linijek) opis pojedynczego modułu po angielsku wraz z tabelą testów do raportu (zobacz wzorzec: [`docs/monitoring.md`](docs/monitoring.md)).

---

## Jak wdrożyć nową funkcjonalność (Krok po kroku)

### Krok 1: Utwórz branch
```bash
git checkout main && git pull
git checkout -b feature/<module-name>
```

### Krok 2: Wygeneruj 1 krótki plik `docs/<module-name>.md`
Otwórz swojego agenta AI (Antigravity, Codex, Claude, Cursor) i wklej:

> Chcę zaimplementować moduł: **[OPIS CO MA ROBIĆ MODUŁ, np. transformacja Gold / serwis zapytań / replay z S3 do SQS]**.
> Zgodnie z `AGENTS.md` i szablonem `docs/_module-template.md` stwórz najpierw tylko krótki plik `docs/<module-name>.md` po angielsku (wejście/wyjście, zmieniane pliki, kontrola kosztów Free Tier i tabela testów do raportu) i poczekaj na moją akceptację.

Przeczytaj powstały plik `docs/<module-name>.md` (zajmie to 1 minutę) i popraw, jeśli coś chcesz zmienić.

### Krok 3: Implementacja i testy
Napisz agentowi:

> Zaakceptowane. Zaimplementuj teraz kod i testy zgodnie z `docs/<module-name>.md` i uruchom testy.

### Krok 4: Weryfikacja i uzupełnienie tabeli do raportu
1. Uruchom testy lokalnie:
   - Python: `PYTHONPATH=src pytest -q`
   - Terraform (w `infra/`): `terraform fmt -check && terraform validate && terraform test`
2. Po wdrożeniu na AWS wpisz w ostatniej kolumnie tabeli w `docs/<module-name>.md` faktyczny wynik - przyda się do raportu.
3. Zrób commit i Pull Request do `main`.

---

## Praca w kolejnych etapach projektu (Milestone 3 i finał)

Na starcie każdego kolejnego etapu (warstwa Gold, ML, własny serwis zapytań na EC2, dashboard Streamlit):

1. **Wspólna aktualizacja kontraktów na `main` (przed podziałem zadań)**:
   - W [`AGENTS.md`](AGENTS.md) aktualizujemy aktualną fazę (*Current phase*) oraz zatwierdzone decyzje architektoniczne (np. wybór technologii dla warstwy Gold lub własnego serwisu zapytań na EC2).
   - W [`docs/event-schema.md`](docs/event-schema.md) dopisujemy wspólny schemat nowej warstwy (np. kolumny dokumentu `gold/` albo endpointy REST API między `query_service` a `dashboard`), żeby każdy mógł pracować nad swoim modułem równolegle.
2. **Nowe moduły = nowe pliki `docs/<module-name>.md` (starych nie kasujemy)**:
   - Każdy nowy moduł dostaje swój krótki plik w `docs/` wg [`docs/_module-template.md`](docs/_module-template.md):
     - *Gold / Replay (`src/processing/`)*: reguły joinów czasowo-przestrzennych i idempotentność odtwarzania strumienia,
     - *ML (`src/ml/`)*: wejściowe cechy z Gold, format predykcji ryzyka i tabela z metrykami modelu,
     - *Serwowanie i Dashboard (`src/query_service/`, `src/dashboard/`)*: kontrakt API, kontrola kosztów instancji EC2/Fargate i testy end-to-end ze screenshotami UI.
   - Pliki `docs/*.md` z poprzednich etapów zostają w repozytorium jako gotowe sekcje i tabele testów do raportu końcowego.
