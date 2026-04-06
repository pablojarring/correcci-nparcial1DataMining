# Examen Teórico — Data Mining — Respuestas

**Universidad San Francisco de Quito**
Ingeniería en Ciencias de la Computación
Materia: Data Mining
Profesor: MSc. Erick Ñauñay Cantos

---

## 1) ¿Qué afirmación describe mejor por qué un contenedor es "más ligero" que una VM?

**Respuesta: B**
> Comparte el kernel del host y aísla procesos con mecanismos del SO.

Un contenedor no incluye su propio kernel; utiliza el del host. Aísla procesos mediante namespaces y cgroups del sistema operativo, lo cual lo hace mucho más liviano que una VM que necesita un SO completo.

---

## 2) Sobre imágenes Docker y capas (layers), ¿cuáles son correctas?

**Respuesta: A, B, D**

- **A.** Las imágenes se componen de capas reutilizables entre builds. ✅
- **B.** Las capas de imagen son inmutables; el contenedor agrega una capa de escritura. ✅
- **C.** Cada RUN del Dockerfile siempre sobrescribe la capa anterior sin crear nuevas capas. ❌ — Cada `RUN` crea una nueva capa.
- **D.** Copy-on-write permite eficiencia al modificar archivos sin duplicar toda la imagen. ✅

---

## 3) ¿Cuál es el riesgo principal de usar `latest` en producción?

**Respuesta: C**
> Introduce cambios no deterministas y rompe trazabilidad de despliegues.

---

## 4) En Docker Compose, ¿qué diferencia es crítica entre `depends_on` y un `healthcheck`?

**Respuesta: C**
> `depends_on` define orden de arranque; `healthcheck` valida salud real del servicio.

---

## 5) En la comparación Kubernetes vs Docker:

**Respuesta: A, C**

- **A.** Docker es principalmente runtime/build; Kubernetes orquesta despliegue. ✅
- **C.** Kubernetes soporta scheduling, auto-healing, escalado y service discovery. ✅

---

## 6) ¿Qué definición distingue mejor "orquestación" de "automatización"?

**Respuesta: B**
> Automatización ejecuta tareas; orquestación coordina dependencias, estado y retries.

---

## 7) En un DAG de orquestación, ¿qué propiedad debe cumplirse?

**Respuesta: B**
> Debe ser acíclico; no puede contener ciclos.

---

## 8) Respecto a backfill/catchup e idempotencia:

**Respuesta: A, B, C**

- **A.** Un backfill recomputa ventanas pasadas. ✅
- **B.** Catchup ejecuta corridas pendientes. ✅
- **C.** Idempotencia implica que reejecutar no duplica datos. ✅

---

## 9) ¿Qué diseño reduce mejor duplicados con reintentos?

**Respuesta: C**
> Idempotencia por partición + upsert/merge.

---

## 10) Sobre observabilidad en pipelines:

**Respuesta: A, B, D**

- **A.** Métricas (duración, éxito/falla). ✅
- **B.** Alertas accionables. ✅
- **D.** Lineage/metadata. ✅

---

## 11) ¿Qué describe mejor un Data Warehouse moderno (OLAP) en la nube?

**Respuesta: B**
> Análisis + separación compute/storage.

---

## 12) ¿Qué diferencia resume mejor ETL vs ELT?

**Respuesta: B**
> ETL transforma antes; ELT transforma dentro del DWH.

---

## 13) Pruning/data skipping:

**Respuesta: A, B, D**

- **A.** Omite particiones irrelevantes. ✅
- **B.** Reduce costo/tiempo. ✅
- **D.** Depende de metadata/estadísticas. ✅

---

## 14) ¿Qué caracteriza un Lakehouse?

**Respuesta: A**
> Object storage + tablas ACID + metadatos.

---

## 15) Iceberg/Delta vs Snowflake:

**Respuesta: B**
> Usan metadata/logs para ACID y skipping.

---

## 16) ¿Qué describe mejor Analytics Engineering?

**Respuesta: B**
> Modelado, tests, CI/CD de transformaciones.

---

## 17) En dbt, diferencia entre tests genéricos y singulares:

**Respuesta: A**
> Genéricos → columnas comunes; singulares → SQL custom.

---

## 18) Source freshness en dbt:

**Respuesta: A, B, D**

- **A.** Campo `loaded_at`. ✅
- **B.** Umbrales warn/error. ✅
- **D.** Detecta fuentes stale. ✅

---

## 19) ¿Qué es un snapshot en dbt?

**Respuesta: B**
> Historización tipo SCD2.

---

## 20) Star schema vs OBT:

**Respuesta: A, B, C**

- **A.** OBT reduce joins. ✅
- **B.** Star mejora semántica. ✅
- **C.** Depende del motor y consultas. ✅

---

## 21) Secuencia Job → Stage → Task en Spark:

**Respuesta: B**
> Acción → job; shuffle → stages; tasks por partición.

---

## 22) ¿Por qué DataFrames > RDDs?

**Respuesta: B**
> Catalyst optimizer mejora ejecución.

---

## 23) Operaciones que causan shuffle:

**Respuesta: A, B, D**

- **A.** `groupBy`. ✅
- **B.** `orderBy` global. ✅
- **D.** `join` grande sin broadcast. ✅

---

## 24) ¿Qué join evita shuffle con tabla pequeña?

**Respuesta: B**
> Broadcast join.

---

## 25) `repartition` vs `coalesce`:

**Respuesta: B**
> `repartition` hace shuffle; `coalesce` reduce con menos movimiento.
