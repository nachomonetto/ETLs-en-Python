# ETLs-en-Python
Aquí se almacenan scripts .py para la realización de procesos ETL en Python. La mayoría se los procesos consisten en:
1) Extracción de datos de Datalake on Cloud (GCP).
2) Algunas transformaciones en los campos.
3) Eventuales cruces con otras tablas a través de relaciones PK-FK.
4) Generación de subrogados.
5) Detección de novedades (registros nuevos) o cambios de valores existentes. En otras palabras, existen un escenario de insert, y un escenario de update.
6) Insert y/o Update en tabla de BD Oracle (Data Warehouse).
