---
title: "Pipeline-en kjørte 'success' men tabellen er tom"
category: troubleshoot
slug: empty-table
---

Pipeline kan rapportere success uten å ha skrevet noe — typisk når
filteret ikke matcher noe i kilden. Sjekk:

1. **Sammenlign runtime**: en typisk Silver-jobb tar minutter. Hvis den
   tok < 1 min, mistenker tomt resultat.
2. **Sjekk filter i Spark-jobben**: er event-typer/dato-filter korrekt?
3. **Inspiser Bronze-data**:

   ```python
   from pyspark.sql import SparkSession, functions as F
   spark = SparkSession.builder.getOrCreate()
   df = spark.read.format("delta").load("s3a://bronze/<domene>/<tabell>")
   df.groupBy("event_type").count().show()
   ```

4. **Sjekk validate_quality**: hvis et produkt har tom tabell, vil
   `expect_table_row_count_to_be_between(min_value=1)` fange det opp og
   markere kvalitet som < 100%.

Eksempel: residence_history-bug i april 2026 — filteret matchet kun
`event.relocation`, men kilden inneholdt kun `citizen.created`. Fikset
ved å utvide filteret. Se commit-historikk for `folkeregister_residence_history.py`.
