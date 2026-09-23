# Databricks notebook source
# MAGIC %md
# MAGIC # Laboratorio guiado · Proyecto final
# MAGIC ## Practicar el método sobre `tpch` antes de aplicarlo a su caso
# MAGIC
# MAGIC **Big Data (ISD-25)** · Semana 8
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC Como en los laboratorios anteriores, trabajamos sobre `samples.tpch`, el caso de distribución
# MAGIC que ya conocen. **Ustedes aplican después el mismo método a su propio caso** en la plantilla
# MAGIC `EA3_plantilla`.
# MAGIC
# MAGIC Cada paso trae un ejemplo resuelto, un ejercicio 🔧 que completan ustedes, y un recuadro
# MAGIC **➡️ En su proyecto** que dice cómo llevarlo a su caso.
# MAGIC
# MAGIC | Paso | Qué practican | Sección del proyecto | Tiempo |
# MAGIC |---|---|---|---|
# MAGIC | 1 | Construir una capa oro que se lea sola | 2 · Capa oro | 20 min |
# MAGIC | 2 | Medir sin engañarse | 3 · Medición inicial | 15 min |
# MAGIC | 3 | Leer el plan y encontrar el costo | 3 · Medición inicial | 15 min |
# MAGIC | 4 | Optimizar y explicar qué cambió | 4 · Optimización | 20 min |
# MAGIC | 5 | Decir cuándo no sirve | 5 · Límites | 10 min |
# MAGIC | 6 | Armar la respuesta a la pregunta orientadora | 6 · Cierre | 10 min |
# MAGIC
# MAGIC ⏱️ **Unos 90 minutos.** Pueden partirlo en dos sesiones.
# MAGIC
# MAGIC > 💡 **Cómo trabajarlo:** ejecuten el ejemplo, lean la explicación, y **solo entonces**
# MAGIC > intenten el ejercicio 🔧. Las soluciones están en el apéndice, para comparar después.
# MAGIC
# MAGIC > ⚠️ **Sobre la cuota:** medir exige ejecutar varias veces la misma consulta. No dejen este
# MAGIC > laboratorio para la última noche.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
from pyspark.sql.window import Window
import time, statistics

CATALOGO = "lab_final"
ESQUEMA  = "practica"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOGO}")
spark.sql(f"USE CATALOG {CATALOGO}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ESQUEMA}")
spark.sql(f"USE SCHEMA {ESQUEMA}")

lineas   = spark.table("samples.tpch.lineitem")
pedidos  = spark.table("samples.tpch.orders")
clientes = spark.table("samples.tpch.customer")
prov     = spark.table("samples.tpch.supplier")
partes   = spark.table("samples.tpch.part")
paises   = spark.table("samples.tpch.nation")
regiones = spark.table("samples.tpch.region")

for nombre, df in [("lineitem", lineas), ("orders", pedidos), ("customer", clientes),
                   ("supplier", prov), ("part", partes), ("nation", paises), ("region", regiones)]:
    print(f"{nombre:<10}{df.count():>14,} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # PASO 1 · Una capa oro que se lea sola
# MAGIC
# MAGIC La capa oro no es «la plata agregada»: es **una tabla que responde una pregunta concreta para
# MAGIC alguien concreto**, y que esa persona entiende sin que ustedes estén al lado.
# MAGIC
# MAGIC | Una buena capa oro se lee… | Cómo se logra |
# MAGIC |---|---|
# MAGIC | **Sin identificadores** | Nombres en vez de `c_custkey` o `n_nationkey` |
# MAGIC | **Sin calculadora** | Las métricas ya vienen calculadas |
# MAGIC | **Sin buscar** | Ordenada para que lo importante quede arriba |
# MAGIC
# MAGIC ## Ejemplo resuelto
# MAGIC
# MAGIC **Pregunta:** *¿qué segmento de clientes genera más ingresos, y con qué ticket promedio?*
# MAGIC **Quién la usa:** la gerencia comercial, para decidir dónde enfocar la fuerza de ventas.

# COMMAND ----------

oro_segmentos = (pedidos.alias("o")
    .join(clientes.alias("c"), F.col("o.o_custkey") == F.col("c.c_custkey"))
    .groupBy(F.trim(F.col("c.c_mktsegment")).alias("segmento"))
    .agg(
        F.countDistinct("c.c_custkey").alias("clientes"),
        F.count("*").alias("pedidos"),
        F.round(F.sum("o.o_totalprice"), 0).alias("ingresos_totales"),
        F.round(F.avg("o.o_totalprice"), 2).alias("ticket_promedio"),
    )
    .withColumn("pedidos_por_cliente", F.round(F.col("pedidos") / F.col("clientes"), 1))
    .orderBy(F.desc("ingresos_totales")))

TABLA = f"{CATALOGO}.{ESQUEMA}.oro_segmentos"
oro_segmentos.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TABLA)
display(spark.table(TABLA))

# COMMAND ----------

# MAGIC %md
# MAGIC **Fíjense en tres cosas:** los nombres de las columnas dicen qué son (`ticket_promedio`, no
# MAGIC `avg_o_totalprice`); hay una métrica que **no existe en el origen** (`pedidos_por_cliente`); y
# MAGIC la tabla quedó **guardada**, no solo mostrada — eso permite después apuntarle un tablero o Genie.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Ejercicio 1
# MAGIC
# MAGIC **Pregunta:** *¿qué región del mundo concentra más ingresos, y cuántos clientes tiene?*
# MAGIC
# MAGIC Crucen `orders` → `customer` → `nation` → `region`. **Pistas:** `c_nationkey` se une con
# MAGIC `n_nationkey`; `n_regionkey` con `r_regionkey`; el nombre está en `r_name` y trae espacios al
# MAGIC final, usen `F.trim()`.
# MAGIC
# MAGIC **La tabla debe tener:** región, clientes, pedidos, ingresos totales, ticket promedio, y **qué
# MAGIC porcentaje del total representa cada región** — para eso:
# MAGIC `F.col("ingresos_totales") / F.sum("ingresos_totales").over(Window.partitionBy()) * 100`

# COMMAND ----------

# 🔧 Completen aquí
oro_regiones = None


# COMMAND ----------

# MAGIC %md
# MAGIC ## Documentar la tabla
# MAGIC
# MAGIC Los comentarios en las columnas dicen qué significa cada una. Si van por los puntos opcionales,
# MAGIC **son lo que le permite a Genie entender la tabla**. Escríbanlos en lenguaje de negocio.

# COMMAND ----------

spark.sql(f"COMMENT ON TABLE {TABLA} IS 'Ingresos y comportamiento de compra por segmento de clientes'")
for col, txt in {
    "segmento":            "Segmento de mercado del cliente",
    "clientes":            "Número de clientes distintos del segmento",
    "pedidos":             "Número total de pedidos realizados",
    "ingresos_totales":    "Suma de los montos de todos los pedidos",
    "ticket_promedio":     "Monto promedio por pedido",
    "pedidos_por_cliente": "Cuántos pedidos hace en promedio cada cliente",
}.items():
    spark.sql(f"ALTER TABLE {TABLA} ALTER COLUMN {col} COMMENT '{txt}'")
display(spark.sql(f"DESCRIBE TABLE {TABLA}"))

# COMMAND ----------

# MAGIC %md
# MAGIC > ### ➡️ En su proyecto
# MAGIC > Tomen **una** de las preguntas de negocio que plantearon en la EA1 y construyan la capa oro
# MAGIC > que la responde, con los tres «sin» y con las columnas documentadas.
# MAGIC >
# MAGIC > Un detalle que en su caso importa: **si cruzan tablas con distinto nivel de detalle** —por
# MAGIC > ejemplo, reservas con reseñas—, resuman primero la más detallada y después crucen. Si no, las
# MAGIC > filas se repiten y los promedios salen inflados.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # PASO 2 · Medir sin engañarse
# MAGIC
# MAGIC | Razón | Qué pasa |
# MAGIC |---|---|
# MAGIC | La primera ejecución siempre es más lenta | Spark planea, lee metadatos y a veces enciende cómputo |
# MAGIC | Hay variación normal | La misma consulta tarda distinto sin que nada cambie |
# MAGIC
# MAGIC **El método:** una ejecución de calentamiento que se descarta, tres mediciones, y la mediana.

# COMMAND ----------

def medir(descripcion, funcion, repeticiones=3):
    """Calienta, repite y reporta la mediana. Es la misma función de la plantilla."""
    funcion()
    tiempos = []
    for _ in range(repeticiones):
        inicio = time.time()
        funcion()
        tiempos.append(time.time() - inicio)
    mediana = statistics.median(tiempos)
    print(f"{descripcion}")
    print(f"   mediciones : {', '.join(f'{t:.2f}s' for t in tiempos)}")
    print(f"   MEDIANA    : {mediana:.2f}s")
    print(f"   variación  : {max(tiempos)-min(tiempos):.2f}s\n")
    return mediana

# COMMAND ----------

# Ejemplo: lo que pasa si miden una sola vez
consulta_simple = lambda: pedidos.filter("o_orderstatus = 'F'").count()
inicio = time.time(); consulta_simple(); en_frio = time.time() - inicio
print(f"Una sola ejecución, en frío: {en_frio:.2f}s\n")
medir("La misma consulta, medida bien", consulta_simple)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Ejercicio 2
# MAGIC
# MAGIC Midan esta consulta, que cruza las líneas de pedido con las partes. **Antes de ejecutar,
# MAGIC anoten su predicción** de cuánto va a tardar.

# COMMAND ----------

# Desactivamos la optimización automática para ver el caso base
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

ventas_por_tipo = (lineas.alias("l")
    .join(partes.alias("p"), F.col("l.l_partkey") == F.col("p.p_partkey"))
    .groupBy(F.col("p.p_type"))
    .agg(F.round(F.sum(F.col("l.l_extendedprice") * (1 - F.col("l.l_discount"))), 0).alias("ventas")))

# 🔧 tiempo_antes = medir(...)


# COMMAND ----------

# MAGIC %md
# MAGIC **Predicción:** *…* · **Mediana real:** *…* · **Variación entre mediciones:** *…*
# MAGIC
# MAGIC > ### ➡️ En su proyecto
# MAGIC > Elijan **un cruce entre una tabla grande y una más pequeña**. En su caso, las tablas de
# MAGIC > navegación son las más grandes. Midan con la misma función `medir()`, que viene en la plantilla.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # PASO 3 · Leer el plan y encontrar el costo
# MAGIC
# MAGIC Se lee **de abajo hacia arriba**. Busquen estas cuatro palabras:
# MAGIC
# MAGIC | Palabra | Qué significa | ¿Es cara? |
# MAGIC |---|---|---|
# MAGIC | `Scan` | Leer datos | Depende de cuánto |
# MAGIC | `Filter` | Descartar filas | Barata |
# MAGIC | **`Exchange`** | **Mover datos entre máquinas** | **Cara** |
# MAGIC | `HashAggregate` | Agrupar | Moderada |
# MAGIC
# MAGIC > **Optimizar en Spark es, casi siempre, quitar o reducir un `Exchange`.**
# MAGIC
# MAGIC ## 🔧 Ejercicio 3
# MAGIC
# MAGIC Ejecuten el plan de la consulta del ejercicio 2 y respondan:
# MAGIC
# MAGIC 1. ¿Qué estrategia de cruce eligió Spark? *(la palabra que termina en `Join`)*
# MAGIC 2. ¿Sobre cuántas tablas aparece un `Exchange`?
# MAGIC 3. `lineitem` tiene 30 millones de filas y `part` un millón. ¿Cuál pesa más en ese movimiento?

# COMMAND ----------

# 🔧 ventas_por_tipo.explain()


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # PASO 4 · Optimizar y explicar qué cambió
# MAGIC
# MAGIC ## La idea, con la analogía del estadio
# MAGIC
# MAGIC Estamos moviendo a 40.000 asistentes entre tribunas para cruzarlos con una lista de socios. Si
# MAGIC la lista es corta, **es mucho más barato darle una copia a cada contador**. Nadie se mueve.
# MAGIC Eso es un ***broadcast join***.
# MAGIC
# MAGIC ## Ejemplo resuelto, con `supplier` (10.000 filas)

# COMMAND ----------

antes = (lineas.alias("l").join(prov.alias("s"), F.col("l.l_suppkey") == F.col("s.s_suppkey"))
         .groupBy("s.s_nationkey").count())
despues = (lineas.alias("l").join(broadcast(prov.alias("s")), F.col("l.l_suppkey") == F.col("s.s_suppkey"))
           .groupBy("s.s_nationkey").count())

print("════ PLAN ANTES ════");   antes.explain()
print("\n════ PLAN DESPUÉS ════"); despues.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC | | Antes | Después |
# MAGIC |---|---|---|
# MAGIC | Estrategia | `SortMergeJoin` | **`BroadcastHashJoin`** |
# MAGIC | Movimiento | `Exchange` sobre **las dos** tablas | **`BroadcastExchange` solo sobre `supplier`** |
# MAGIC | Ordenamiento | `Sort` sobre las dos | **Ninguno** |
# MAGIC
# MAGIC **La tabla grande dejó de moverse.** Eso es todo el truco.
# MAGIC
# MAGIC ## 🔧 Ejercicio 4
# MAGIC
# MAGIC Apliquen `broadcast` a la consulta del ejercicio 2 —el cruce con `part`—, vean el plan nuevo
# MAGIC y midan.
# MAGIC
# MAGIC > ⚠️ `part` tiene un millón de filas, cien veces más que `supplier`. **Puede que mejore menos,
# MAGIC > que no mejore, o que empeore.** Esa es la gracia del ejercicio: no sabemos el resultado de
# MAGIC > antemano, igual que en su proyecto.

# COMMAND ----------

# 🔧 ventas_por_tipo_opt = ...
# 🔧 ventas_por_tipo_opt.explain()
# 🔧 tiempo_despues = medir(...)


# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔧 Su conclusión, con esta estructura
# MAGIC
# MAGIC **Qué hacía antes:** *…* · **Qué cambió en el plan:** *…* ·
# MAGIC **El número:** la mediana pasó de ___ a ___ segundos.
# MAGIC
# MAGIC > ### ➡️ En su proyecto
# MAGIC > Sus tablas tienen **cientos de miles** de filas, no decenas de millones como `lineitem`. Es
# MAGIC > posible que la mejora sea pequeña, o que se pierda en la variación. **Eso no es un fracaso:
# MAGIC > es un resultado, y se reporta igual.** Lo que cuenta es explicar qué cambió en el plan y
# MAGIC > por qué el número salió como salió.

# COMMAND ----------

spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")
print("Configuración restaurada.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # PASO 5 · Decir cuándo no sirve
# MAGIC
# MAGIC Difundir significa **copiar la tabla completa a cada máquina**. Funciona si esa tabla es
# MAGIC pequeña. Si crece, copiarla muchas veces cuesta más que moverla una vez — y si no cabe en
# MAGIC memoria, **la consulta no se pone lenta: falla**.
# MAGIC
# MAGIC | Situación | ¿Conviene broadcast? |
# MAGIC |---|---|
# MAGIC | Tabla pequeña | ✅ Sí |
# MAGIC | Tabla mediana | ⚠️ Depende |
# MAGIC | Las dos grandes | ❌ No |
# MAGIC | Poco volumen total | ⚠️ La diferencia no se distingue del ruido |
# MAGIC
# MAGIC ## 🔧 Ejercicio 5
# MAGIC
# MAGIC ¿El *broadcast* de `part` mejoró tanto como el de `supplier`? ¿Por qué sí o por qué no?
# MAGIC
# MAGIC *…*
# MAGIC
# MAGIC > ### ➡️ En su proyecto
# MAGIC > Respondan: **¿qué tendría que cambiar en sus datos para que su optimización dejara de
# MAGIC > servir?** Atenlo a sus tablas y sus cifras — «si la tabla de propiedades pasara a millones de
# MAGIC > filas…» dice mucho más que «si la tabla fuera grande».

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # PASO 6 · La pregunta orientadora
# MAGIC
# MAGIC > Una empresa recibe datos de su portal web, de Facebook, de Instagram, de TikTok y de
# MAGIC > WhatsApp Business. **¿Bajo qué paradigma, con qué herramientas y con qué arquitectura
# MAGIC > debería analizarlos?**
# MAGIC
# MAGIC No es un ensayo: son **tres decisiones**, cada una apoyada en algo que construyeron en el curso.
# MAGIC
# MAGIC | Decisión | Pregunta para guiarse | Dónde lo trabajaron |
# MAGIC |---|---|---|
# MAGIC | **Paradigma** | ¿Esos datos tienen estructura fija o cambian? | EA1, justificación del modelo |
# MAGIC | **Herramientas** | ¿Qué necesita cada fuente para no perder información? | EA1 y EA2 |
# MAGIC | **Arquitectura** | ¿Cómo organizarían las capas y quién vería qué? | EA2, capas y gobierno |
# MAGIC
# MAGIC **Una pista:** las cinco fuentes no llegan igual. El portal da eventos, las redes entregan
# MAGIC respuestas anidadas cuyo formato cambian sin avisar, y WhatsApp trae texto libre.
# MAGIC
# MAGIC > **Y reconozcan el costo de lo que elijan.** Ninguna arquitectura es gratis.
# MAGIC
# MAGIC > ### ➡️ En su proyecto
# MAGIC > Su propio caso tiene eventos de navegación, datos anidados y texto libre. **Úsenlo como
# MAGIC > evidencia de su respuesta**: lo que construyeron es la prueba de que su propuesta funciona.
# MAGIC
# MAGIC ## 🔧 Ejercicio 6 · Su borrador
# MAGIC
# MAGIC **Paradigma:** *…* · **Herramientas:** *…* · **Arquitectura:** *…* ·
# MAGIC **El costo que asumimos:** *…*

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # ✅ ¿Listos para su proyecto?
# MAGIC
# MAGIC - [ ] Construí una capa oro que se lee sin identificadores, sin calculadora y sin buscar
# MAGIC - [ ] Sé por qué medir una sola vez engaña
# MAGIC - [ ] Encuentro el `Exchange` en un plan de ejecución
# MAGIC - [ ] Sé explicar qué cambió entre dos planes, no solo los tiempos
# MAGIC - [ ] Puedo decir cuándo una optimización deja de servir
# MAGIC - [ ] Tengo un borrador de la pregunta orientadora
# MAGIC
# MAGIC Ahora pasen a la plantilla `EA3_plantilla` y apliquen todo esto **a su caso**.
# MAGIC
# MAGIC **Entrega: domingo 27 de septiembre, 11:59 p. m.**

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Apéndice · Soluciones
# MAGIC
# MAGIC Para comparar **después** de intentarlo.

# COMMAND ----------

# --- Solución · Ejercicio 1
oro_regiones_sol = (pedidos.alias("o")
    .join(clientes.alias("c"), F.col("o.o_custkey") == F.col("c.c_custkey"))
    .join(paises.alias("n"),   F.col("c.c_nationkey") == F.col("n.n_nationkey"))
    .join(regiones.alias("r"), F.col("n.n_regionkey") == F.col("r.r_regionkey"))
    .groupBy(F.trim(F.col("r.r_name")).alias("region"))
    .agg(F.countDistinct("c.c_custkey").alias("clientes"),
         F.count("*").alias("pedidos"),
         F.round(F.sum("o.o_totalprice"), 0).alias("ingresos_totales"),
         F.round(F.avg("o.o_totalprice"), 2).alias("ticket_promedio"))
    .withColumn("pct_de_ingresos",
                F.round(F.col("ingresos_totales") / F.sum("ingresos_totales").over(Window.partitionBy()) * 100, 1))
    .orderBy(F.desc("ingresos_totales")))
display(oro_regiones_sol)

# COMMAND ----------

# --- Solución · Ejercicios 2 y 4
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
t_antes = medir("ANTES · SortMergeJoin con part", lambda: ventas_por_tipo.collect())

ventas_opt_sol = (lineas.alias("l")
    .join(broadcast(partes.alias("p")), F.col("l.l_partkey") == F.col("p.p_partkey"))
    .groupBy(F.col("p.p_type"))
    .agg(F.round(F.sum(F.col("l.l_extendedprice") * (1 - F.col("l.l_discount"))), 0).alias("ventas")))
ventas_opt_sol.explain()
t_despues = medir("DESPUÉS · BroadcastHashJoin con part", lambda: ventas_opt_sol.collect())

print(f"Mejora: {(t_antes - t_despues) / t_antes * 100:.1f}%")
spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")

# COMMAND ----------

# MAGIC %md
# MAGIC **Ejercicio 3.** `SortMergeJoin`, con `Exchange hashpartitioning` sobre las dos tablas. Pesa
# MAGIC más `lineitem`: mover 30 millones de filas por la red domina el tiempo.
# MAGIC
# MAGIC **Ejercicio 5.** Lo esperable es que el *broadcast* de `part` mejore **menos** que el de
# MAGIC `supplier`: copiar un millón de filas a cada máquina ya tiene un costo apreciable. **Ese es el
# MAGIC límite de la técnica, medido.** El número exacto depende del cómputo del momento; lo que
# MAGIC importa es poder explicar el que obtuvieron.

# COMMAND ----------

# spark.sql(f"DROP CATALOG IF EXISTS {CATALOGO} CASCADE")
