"""Job AWS Glue (Spark): raw/ (JSONL) -> lake/bronze (Parquet) -> lake/silver (Parquet).

Tryby (--mode):
  nightly   przetwarza dni z raw/ (ingest_date < dziś UTC), które nie mają znacznika
            lake/_done/ingest_date=<dzień>. Bez --day sam nadrabia zaległe dni.
            Z --day przelicza tylko ten dzień (np. ponowienie), z --legacy true dołącza
            stare pliki JSON z bronze/ o ingestion_time w tym dniu (tylko przy przejściu).
  backfill  jednorazowo przenosi stare pliki JSON z bronze/ do Parquet (--from, --before).
  check     drukuje liczby wierszy i liczbę starych rekordów, których brakuje w lake/bronze.

Idempotencja: zapis nadpisuje tylko partycje ingest_date z danego dnia
(spark.sql.sources.partitionOverwriteMode=dynamic), więc powtórka daje ten sam wynik.
Seismic w silver to jeden wiersz na trzęsienie (wygrywa najnowszy lastupdate) i jest
liczony z całego bronze.

Pakiet zależności (common/, processing/) dostarcza --extra-py-files s3://.../glue/deps.zip.
"""
import argparse
from datetime import date, datetime, timedelta, timezone

import boto3
from pyspark.sql import SparkSession, Window, functions as F

from processing.batch_rows import COLS, parse, pending_days as _pending, to_doc
from processing.batch_schemas import DATASETS, ddl, unknown_fields

p = argparse.ArgumentParser()
p.add_argument("--bucket", required=True)
p.add_argument("--mode", choices=["nightly", "backfill", "check"], default="nightly")
p.add_argument("--day")                       # nightly: tylko ten dzień ingestion
p.add_argument("--legacy", default="false")   # nightly: "true" = dołącz stare JSON-y z tego dnia
p.add_argument("--from", dest="from_day", default="0000-00-00")  # backfill: od (włącznie)
p.add_argument("--before")                    # backfill: do (bez tego dnia)
a, _ = p.parse_known_args()                   # Glue dokłada własne argumenty

B = f"s3://{a.bucket}"
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
sc = spark.sparkContext
s3 = boto3.client("s3")


# ----------------------------------------------------------------------- S3 / znaczniki
def _list_days(prefix, key_prefix):
    """Dni z kluczy/folderów postaci <prefix><key_prefix>YYYY-MM-DD..."""
    days, token = set(), None
    while True:
        kw = {"Bucket": a.bucket, "Prefix": prefix + key_prefix, "Delimiter": "/"}
        if token:
            kw["ContinuationToken"] = token
        r = s3.list_objects_v2(**kw)
        for item in [c["Prefix"] for c in r.get("CommonPrefixes", [])] + [o["Key"] for o in r.get("Contents", [])]:
            days.add(item[len(prefix + key_prefix):][:10])
        token = r.get("NextContinuationToken")
        if not token:
            return days


def pending_days():
    today = datetime.now(timezone.utc).date().isoformat()
    raw = _list_days("raw/", "ingest_date=")
    done = _list_days("lake/_done/", "ingest_date=")
    return _pending(raw, done, today)


def mark_done(day):
    s3.put_object(Bucket=a.bucket, Key=f"lake/_done/ingest_date={day}", Body=b"")


# ---------------------------------------------------------------------------- bronze
def legacy_rdd(*event_dates):
    """Stare pliki JSON (jeden rekord w pliku). Bez argumentów -> wszystkie."""
    paths = ([f"{B}/bronze/source=*/date={d}/*.json" for d in event_dates]
             if event_dates else [f"{B}/bronze/source=*/date=*/*.json"])
    return spark.read.option("wholetext", True).text(paths).rdd.map(lambda r: r.value)


def to_df(rdd):
    return spark.createDataFrame(rdd.map(parse), COLS)


def write_bronze(df, day=None):
    ingest = F.lit(day) if day else F.substring("ingestion_time", 1, 10)
    df = df.withColumn("ingest_date", ingest)
    w = Window.partitionBy("event_id").orderBy("ingestion_time")
    df = df.withColumn("n", F.row_number().over(w)).where("n = 1").drop("n")  # dedup po event_id
    (df.repartition("ingest_date", "source").write.mode("overwrite")
       .partitionBy("ingest_date", "source").parquet(f"{B}/lake/bronze"))


# ---------------------------------------------------------------------------- silver
def build_silver(day=None):
    bronze = spark.read.parquet(f"{B}/lake/bronze")
    for name, source, series in DATASETS:  # NOAA dzielimy na trzy zbiory (wind, mag, kp)
        src = bronze.where(F.col("source") == source)
        if series:
            src = src.where(F.col("payload").contains(f'"series":"{series}"'))
        if day and source != "seismic":  # seismic zawsze liczymy z całej historii
            src = src.where(F.col("ingest_date") == day)
        docs = src.rdd.map(to_doc).filter(lambda x: x is not None)
        if docs.isEmpty():
            continue
        extra = unknown_fields(name, docs.take(2000))  # wykrywa nowe pola w źródle
        if extra:
            print(f"UWAGA {name}: pola spoza schematu zostaną pominięte: {sorted(extra)}")
        silver = spark.read.schema(ddl(name)).json(docs)
        out = f"{B}/lake/silver/{name}"
        if name == "seismic":  # jeden wiersz na trzęsienie, wygrywa najnowszy lastupdate
            w = Window.partitionBy("event_id").orderBy(F.col("data.lastupdate").desc())
            silver = silver.withColumn("n", F.row_number().over(w)).where("n = 1").drop("n")
            silver.coalesce(1).write.mode("overwrite").parquet(out)
        else:
            (silver.repartition("ingest_date").write.mode("overwrite")
                   .partitionBy("ingest_date").parquet(out))
        print(f"silver {name}: {silver.count()} wierszy")


# ----------------------------------------------------------------------------- tryby
def process_day(day, legacy=False):
    parts = []
    try:
        parts.append(to_df(sc.textFile(f"{B}/raw/ingest_date={day}/*/*.jsonl")))
    except Exception as e:  # noqa: BLE001  brak plików z tego dnia
        print("brak raw dla", day, e)
    if legacy:  # stare pliki, których ingestion_time wypada w tym dniu
        prev = (date.fromisoformat(day) - timedelta(days=1)).isoformat()
        parts.append(to_df(legacy_rdd(prev, day)).where(F.substring("ingestion_time", 1, 10) == day))
    if not parts:
        return
    df = parts[0]
    for extra in parts[1:]:
        df = df.unionByName(extra)
    write_bronze(df, day)
    build_silver(day)
    mark_done(day)
    print("dzień", day, "gotowy")


if a.mode == "nightly":
    days = [a.day] if a.day else pending_days()
    print("dni do przetworzenia:", days)
    for d in days:
        process_day(d, legacy=a.legacy == "true")

elif a.mode == "backfill":
    df = to_df(legacy_rdd()).where(
        (F.substring("ingestion_time", 1, 10) >= a.from_day)
        & (F.substring("ingestion_time", 1, 10) < a.before))
    write_bronze(df)  # ingest_date z ingestion_time
    build_silver()

else:  # check
    bronze = spark.read.parquet(f"{B}/lake/bronze")
    bronze.groupBy("source").agg(F.count("*").alias("rows")).show()
    legacy = to_df(legacy_rdd()).select("event_id")
    missing = legacy.join(bronze.select("event_id"), "event_id", "left_anti").count()
    print("CHECK brakujące stare rekordy w bronze:", missing)
