# Spec Kit development - minimum (Codex, ale jak chcecie uzyc czegos innego to wtedy sami somie musicy specify init zrobic)

Spec Kit jest już w repo. **Nic nie instalujesz, nie robisz.** Tylko `git pull`.

## Najpierw: jeśli Codex nie widzi komend `$speckit-...`

> Jakie skille/komendy speckit są dostępne w tym repo?

i potem pisz: *"Przeczytaj `.agents/skills/speckit-<krok>/SKILL.md` i wykonaj jego instrukcje na: ..."* (np. `speckit-specify`, `speckit-plan`).

## Co to jest "moduł"

Jeden kawałek systemu z jasnym wejściem i wyjściem, który robi jedna osoba, np. "poller OpenSky" (Lambda pobiera dane z API i wysyła zdarzenia do Kinesis), "odbiornik WebSocket SeismicPortal", "zapis z Kinesis do S3", "joiny i cechy w Glue". Podział na osoby jest w raporcie PM1. Na każdy moduł zakładasz **osobny branch** (nazwa dowolna, np. `feature/opensky-poller`), żeby nie wchodzić sobie w pliki.

## Minimum: 4 kroki na moduł

Branch: `git checkout -b feature/<nazwa-modułu>`. Potem w nowym wątku Codexa, po kolei:

1. **`$speckit-specify <opis>`**: *co* ma robić moduł. Wpisz cel, skąd bierze dane, co zwraca, kryteria "gotowe gdy...".
   Przykładowy prompt: `$speckit-specify Poller OpenSky: funkcja Lambda uruchamiana co ok. 22 s pobiera snapshot lotów z OpenSky REST dla zadanego bounding boxa, zamienia go na zdarzenia we wspólnym schemacie (event_time, ingestion_time, source, location, id, UTC) i wysyła do Kinesis. Gotowe gdy: działa na AWS, respektuje limit 4000 kredytów/dzień, duplikaty nie trafiają do strumienia, są testy.`
2. **`$speckit-plan <technologie>`**: *jak*. Wpisz język, usługi AWS, biblioteki, ograniczenia.
   Przykładowy prompt: `$speckit-plan Python 3.12, AWS Lambda + EventBridge Scheduler, boto3, requests, sekrety w Secrets Manager, schemat z docs/event-schema.md, testy pytest.`
3. **`$speckit-tasks`**: bez opisu. Agent sam rozbija plan na zadania w `tasks.md`.
   Czyli pizsecie prompt doslownie "$speckit-tasks" i tyle
4. **`$speckit-implement`**: bez opisu. Agent wykonuje zadania z `tasks.md`.
   Czyli pizsecie prompt doslownie "$speckit-implement" i tyle


Po każdym kroku **przeczytaj, co powstało w `specs/`, i popraw** (to zwykłe pliki .md). Na końcu uruchom kod i testy sam.

Pozostałe skille (`clarify`, `analyze`, `checklist`, `converge`) są opcjonalne. Pomiń je, jeśli nie wiesz, po co ci są.

## Co gdzie wpisywać

- **Architektura całego projektu** jest w constitution (`.specify/memory/constitution.md`): nie powtarzasz jej.
- **Twój moduł:** w `specify` *co*, w `plan` *jak*.
- **Wspólny schemat rekordu:** `docs/event-schema.md`. Nie zmieniaj bez uzgodnienia.

## Gdzie co jest

| Co | Gdzie |
|---|---|
| Zasady projektu | `.specify/memory/constitution.md` (zmiana tylko po uzgodnieniu w zespole) |
| Twoje spec / plan / zadania | `specs/<numer-nazwa>/` | (folder specs jset tworzony gdy $speckit-specify opis)
| Skille Codexa | `.agents/skills/` (nie ruszamy) |

## Zasady

- Żadnych kluczy API w repo ani w promptach (Secrets Manager / zmienne środowiskowe).
- Każdy moduł ma testy; logi i screenshoty zapisujemy do dokumentacji testów.
- Małe commity, PR do `main`.

