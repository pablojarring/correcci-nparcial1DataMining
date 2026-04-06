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

Usar `latest` significa que cada `pull` puede traer una versión diferente, haciendo los despliegues no reproducibles.

---

## 4) En Docker Compose, ¿qué diferencia es crítica entre `depends_on` y un `healthcheck`?

**Respuesta: C**
> `depends_on` define orden de arranque; `healthcheck` valida salud real del servicio.

`depends_on` solo garantiza que el contenedor se inicie antes, no que esté listo para aceptar conexiones. El `healthcheck` verifica que el servicio esté realmente funcional.

---

## 5) En la comparación Kubernetes vs Docker:

**Respuesta: A, C**

- **A.** Docker es principalmente runtime/build; Kubernetes orquesta despliegue. ✅
- **B.** Kubernetes reemplaza completamente la necesidad de imágenes Docker. ❌ — K8s usa imágenes de contenedores.
- **C.** Kubernetes soporta scheduling, auto-healing, escalado y service discovery. ✅
- **D.** Docker Compose y Kubernetes cumplen exactamente el mismo rol en producción. ❌ — Compose es para desarrollo local; K8s para producción a escala.

---

## 6) ¿Qué definición distingue mejor "orquestación" de "automatización"?

**Respuesta: B**
> Automatización ejecuta tareas; orquestación coordina dependencias, estado y retries.

La automatización ejecuta tareas individuales. La orquestación va más allá: coordina el orden, maneja dependencias entre tareas, gestiona estado y define políticas de reintento.

---

## 7) En un DAG de orquestación, ¿qué propiedad debe cumplirse?

**Respuesta: B**
> Debe ser acíclico; no puede contener ciclos.

DAG = Directed Acyclic Graph. Por definición, no puede tener ciclos. Si tuviera ciclos, las tareas nunca terminarían de ejecutarse.

---

## 8) Respecto a backfill/catchup e idempotencia:

**Respuesta: A, B, C**

- **A.** Un backfill recomputa ventanas pasadas. ✅
- **B.** Catchup ejecuta corridas pendientes. ✅
- **C.** Idempotencia implica que reejecutar no duplica datos. ✅
- **D.** Idempotencia se logra desactivando retries. ❌ — Idempotencia se logra con diseño (upsert, merge, particiones), no desactivando retries.

---

## 9) ¿Qué diseño reduce mejor duplicados con reintentos?

**Respuesta: C**
> Idempotencia por partición + upsert/merge.

Este patrón asegura que si una tarea se reejecuta, los datos se sobrescriben en lugar de duplicarse. Combinar particionamiento con operaciones de merge/upsert es la mejor práctica.

---

## 10) Sobre observabilidad en pipelines:

**Respuesta: A, B, D**

- **A.** Métricas (duración, éxito/falla). ✅
- **B.** Alertas accionables. ✅
- **C.** Solo logs. ❌ — Los logs son parte de la observabilidad, pero no son suficientes solos.
- **D.** Lineage/metadata. ✅

---

## 11) ¿Qué describe mejor un Data Warehouse moderno (OLAP) en la nube?

**Respuesta: B**
> Análisis + separación compute/storage.

Los DWH modernos en la nube (Snowflake, BigQuery, Redshift) separan cómputo de almacenamiento, permitiendo escalar cada uno independientemente y optimizando costos.

---

## 12) ¿Qué diferencia resume mejor ETL vs ELT?

**Respuesta: B**
> ETL transforma antes; ELT transforma dentro.

En ETL, los datos se transforman antes de cargarlos al destino. En ELT, los datos se cargan crudos y se transforman dentro del DWH usando su motor de cómputo.

---

## 13) Pruning/data skipping:

**Respuesta: A, B, D**

- **A.** Omite particiones irrelevantes. ✅
- **B.** Reduce costo/tiempo. ✅
- **C.** Solo funciona con índices OLTP. ❌ — Pruning funciona con particiones y metadata en OLAP.
- **D.** Depende de metadata/estadísticas. ✅

---

## 14) ¿Qué caracteriza un Lakehouse?

**Respuesta: A**
> Object storage + tablas ACID + metadatos.

El Lakehouse combina la flexibilidad del Data Lake (object storage) con las garantías del DWH (ACID, esquema, metadatos).

---

## 15) Iceberg/Delta vs Snowflake:

**Respuesta: B**
> Usan metadata/logs para ACID y skipping.

Apache Iceberg y Delta Lake usan archivos de metadata y transaction logs para proporcionar transacciones ACID y data skipping sobre object storage.

---

## 16) ¿Qué describe mejor Analytics Engineering?

**Respuesta: B**
> Modelado, tests, CI/CD de transformaciones.

Analytics Engineering se enfoca en aplicar prácticas de ingeniería de software (versionado, tests, CI/CD) a las transformaciones de datos y el modelado analítico.

---

## 17) En dbt, diferencia entre tests genéricos y singulares:

**Respuesta: A**
> Genéricos → columnas comunes; singulares → SQL custom.

Los tests genéricos se aplican a columnas con configuración YAML (unique, not_null, relationships). Los tests singulares son consultas SQL personalizadas que validan lógica de negocio específica.

---

## 18) Source freshness en dbt:

**Respuesta: A, B, D**

- **A.** Campo `loaded_at`. ✅ — Se usa para saber cuándo fue la última carga.
- **B.** Umbrales warn/error. ✅ — Se configuran tiempos máximos tolerables.
- **C.** Sustituye tests de integridad. ❌ — Freshness y tests de integridad son complementarios.
- **D.** Detecta fuentes stale. ✅ — Alerta cuando los datos fuente están desactualizados.

---

## 19) ¿Qué es un snapshot en dbt?

**Respuesta: B**
> Historización tipo SCD2.

Los snapshots en dbt implementan Slowly Changing Dimensions Type 2, registrando cambios históricos con campos `valid_from` y `valid_to`.

---

## 20) Star schema vs OBT:

**Respuesta: A, B, C**

- **A.** OBT reduce joins. ✅ — One Big Table desnormaliza todo en una sola tabla.
- **B.** Star mejora semántica. ✅ — El esquema estrella ofrece mejor organización y claridad.
- **C.** Depende del motor y consultas. ✅ — La elección depende del caso de uso y motor analítico.
- **D.** OBT siempre superior. ❌ — No hay solución universal; depende del contexto.

---

## 21) Secuencia Job → Stage → Task en Spark:

**Respuesta: B**
> Acción → job; shuffle → stages; tasks por partición.

Cuando se ejecuta una acción (collect, save), Spark crea un Job. Cada shuffle boundary divide el job en Stages. Cada stage se divide en Tasks, una por partición de datos.

---

## 22) ¿Por qué DataFrames > RDDs?

**Respuesta: B**
> Catalyst optimizer mejora ejecución.

Los DataFrames pasan por el Catalyst Optimizer de Spark, que optimiza el plan de ejecución lógico y físico. Los RDDs no tienen esta optimización.

---

## 23) Operaciones que causan shuffle:

**Respuesta: A, B, D**

- **A.** `groupBy`. ✅ — Necesita redistribuir datos por clave de agrupación.
- **B.** `orderBy` global. ✅ — Requiere mover datos entre particiones para ordenar globalmente.
- **C.** `select` columnas. ❌ — Es una transformación narrow, no requiere shuffle.
- **D.** `join` grande sin broadcast. ✅ — Sort-merge join o shuffle hash join requieren shuffle.

---

## 24) ¿Qué join evita shuffle con tabla pequeña?

**Respuesta: B**
> Broadcast join.

El broadcast join envía la tabla pequeña completa a todos los nodos, evitando el shuffle de la tabla grande. Se activa con `broadcast()` hint o automáticamente si la tabla es menor al threshold.

---

## 25) `repartition` vs `coalesce`:

**Respuesta: B**
> `repartition` hace shuffle; `coalesce` reduce con menos movimiento.

`repartition(n)` redistribuye todos los datos mediante un full shuffle (puede aumentar o disminuir particiones). `coalesce(n)` solo reduce particiones fusionando las existentes sin shuffle completo, lo cual es más eficiente para reducir.
