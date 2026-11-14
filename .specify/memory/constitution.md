# flight-disturbance Constitution

# Constitution: Monitoring and Predicting Flight Disruptions in Near Real Time

## Cel projektu

Platforma near-real-time dla analityków operacji lotniczych. Łączy pozycje samolotów na żywo z danymi pogodowymi, sejsmicznymi i kosmicznymi, żeby wykrywać zakłócenia ruchu lotniczego, szacować ryzyko zakłóceń w najbliższych godzinach i wyjaśniać główne czynniki. Analiza opiera się na korelacji czasowo-przestrzennej (czas zdarzenia + bliskość geograficzna). Obecny etap (Project Milestone 2): moduły akwizycji i preprocessingu danych.

## Architektura (obowiązująca)

Architektura w stylu lambda na AWS. 

- **Ingestia:** EventBridge Scheduler uruchamia Lambdy pollujące REST (OpenSky, Tomorrow.io, NOAA SWPC). ECS Fargate obsługuje WebSocket (SeismicPortal EMSC).
- **Bufor:** Amazon SQS FIFO. Każde zdarzenie ma stabilny identyfikator, `MessageGroupId` (wg źródła lub logicznej partycji) i deduplication id. Kolejka martwych wiadomości (DLQ) izoluje wiadomości, które wielokrotnie się nie powiodły.
- **Speed layer:** Lambdy konsumujące z SQS FIFO (event source mapping, batche, partial batch failure). Walidują, normalizują, wyliczają proste cechy i zapisują wynik do S3. Okienkowe cechy i scoring w near-real-time też w Lambdach.
- **Data lake (S3):** bronze (surowe, niezmienne), silver (oczyszczone), gold (połączone cechy, etykiety, agregaty). Lambdy archiwizują wiadomości bezpośrednio do S3.
- **Batch layer:** EMR Serverless / Glue (batch) dla historycznych cech i korelacji, w granicach kredytów i kosztów projektu.
- **Replay:** własny producent czyta archiwum z S3 i publikuje zdarzenia z powrotem do SQS z zachowaniem oryginalnych `event_time`.
- **Serwowanie (etapy późniejsze):** Step Functions + SageMaker (retrening, rejestr modeli), DynamoDB (widoki real-time), Athena (widoki batch), API Gateway + Lambda, Streamlit na ECS Fargate.
- **Komponent własny:** jeden z komponentów AWS zostanie zastąpiony własnym serwisem na EC2 (prawdopodobnie Athena). Decyzja jeszcze nieostateczna.

## Źródła danych i ich limity

| Źródło | Dostęp | Limity / częstotliwość |
|---|---|---|
| OpenSky Network | REST | 4000 kredytów/dzień, bounding box = 1 kredyt, snapshot co ok. 22 s |
| Tomorrow.io | REST | 500 żądań/dzień, 25/h, 3/s na klucz, dwa klucze = jedno wywołanie co ok. 86 s |
| SeismicPortal (EMSC) | WebSocket | brak limitów, nieregularnie, kilkaset wiadomości dziennie |
| NOAA SWPC | JSON po HTTP (pull) | wiatr słoneczny ok. 1 min, Kp co 3 h |

Pollery **muszą** respektować te limity.

## Zasady rekordu i przetwarzania

1. Każdy rekord ma `event_time`, `ingestion_time`, `source`, `location` i stabilny identyfikator. Wszystkie znaczniki czasu w UTC.
2. Wszystkie moduły używają wspólnego schematu zdarzenia z `docs/event-schema.md`. Zmiana schematu wymaga uzgodnienia w zespole.
3. Dostarczanie at-least-once, więc przetwarzanie **musi być idempotentne**: stabilne identyfikatory zdarzeń i deterministyczne klucze obiektów S3.
4. Duplikaty snapshotów i poprawione zdarzenia sejsmiczne rozwiązujemy po ID źródła i czasie aktualizacji.
5. Surowe dane w bronze są niezmienne.

## Bezpieczeństwo i koszty

- Klucze API wyłącznie w Secrets Manager lub zmiennych środowiskowych. **Nigdy** w kodzie, repo ani w promptach do AI.
- IAM z zasadą najmniejszych uprawnień.
- Projekt ma działać w limitach AWS Free Tier i dostępnych kredytów. Preferuj batching, kontrolowaną częstotliwość pollingu i efektywny zapis do S3.

## Jakość i testy

- Każdy moduł ma testy funkcjonalne (akwizycja, preprocessing) i testy jakości transformacji danych.
- Dokumentacja testów w formie tabeli: cel testu, kroki, wynik oczekiwany, wynik faktyczny (log, screenshot lub raport). Wyniki faktyczne pochodzą z naszych uruchomień.
- Monitoring: CloudWatch (logi, metryki, alarmy), w tym głębokość kolejki, wiek wiadomości, błędy Lambd i aktywność DLQ.

## Użycie AI

AI pomaga w pracy rutynowej (konektory, testy, dokumentacja, zapytania, przegląd kodu). Nie generuje naszych wyników eksperymentalnych. Zespół sprawdza wszystko, co wygeneruje AI, sam uruchamia testy i opiera fakty zewnętrzne na źródłach. Kluczowe decyzje projektowe i modelowe podejmuje zespół.

## Zarządzanie

- Zmiana tej constitution lub wspólnego schematu zdarzenia wymaga uzgodnienia w całym zespole.
- Zespół: Aleksandra Idczak, Mikołaj Twardowski, Igor Lechoszest, Piotr Bartosiewicz.
- Praca w osobnych branchach per moduł, małe commity, PR do `main`.


**Version**: 1.0.0 | **Ratified**: 2026-11-14 | **Last Amended**: 2026-11-14