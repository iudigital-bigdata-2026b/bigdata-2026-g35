# Big Data (ISD-25) — Grupo 35

**IU Digital de Antioquia** · Ingeniería de Software y Datos · Semestre V · 2026-2

---

## Integrantes

| Nombre | Correo institucional | Usuario de GitHub |
|Paula Andrea Celis Cano| paula.celis@est.iudigital.edu.co|Paula-Celis|


## Caso de estudio
Plataforma de reservas de alojamientos 

El caso de estudio es Wanderbricks, una plataforma simulada para reservar 
alojamientos,este sistema guarda información sobre usuarios, anfitriones,
propiedades, destinos, reservas, pagos y reseñas.Tambien registra las 
actividades de navegación mediante eventos de clickstream, visitas a páginas
y registros de soporte.Estos datos permiten analizar el comportamiento de los
usuarios, la demanda de las propiedades, los ingresos generados por las
reservas y conocer la satisfacción de los huéspedes.
Para este proyecto se utilizará una arquitectura Lakehouse, que permite 
almacenar y organizar diferentes tipos de datos. Se trabajará con las capas 
Bronze y Silver para preparar y transformar los datos, y con Delta Lake para 
realizar consultas y analisis. 
La fuente de datos es el dataset simulado samples.wanderbricks, 
proporcionado por Databricks.

## Evidencias 

| Evidencia | Carpeta | Entrega | Estado | Video |
|---|---|---|---|---|
| EA1 — /ea1/EA1_Wanderbricks.ipynb | [`/ea1`](./ea1) | 23 de agosto | ⬜ | |
| EA2 — Infraestructura y gobierno | [`/ea2`](./ea2) | 6 de septiembre | ⬜ | |
| EA3 — Procesamiento distribuido | [`/ea3`](./ea3) | 20 de septiembre | ⬜ | |
| EA4 — Proyecto integrador | [`/ea4`](./ea4) | 27 de septiembre | ⬜ | |

---

## Estructura del repositorio

```
/ea1  /ea2  /ea3  /ea4   Un notebook por evidencia, en formato .ipynb
/datos                   Archivos auxiliares pequeños (no datasets completos)
/docs                    Diagramas, capturas y documentos de apoyo
```

---

## Reglas de trabajo

1. **Cada integrante confirma sus cambios desde su propia cuenta de GitHub.**
   El historial de commits se contrasta con el reparto declarado en el notebook
   y con lo que cada uno sustenta en el video.
2. Los notebooks se versionan en `.ipynb`, nunca en `.dbc`.
3. No se suben datasets completos al repositorio.
4. El repositorio es privado durante el semestre. Al terminar el curso, el grupo
   puede hacerlo público como portafolio.

---

## Configuración inicial (una sola vez)

1. Aceptar la invitación a la organización del curso.
2. En Databricks: **Settings → Linked accounts → Add Git credential**, vinculando GitHub
   **con el correo correcto**. Si no se define el correo, Databricks usa el nombre de
   usuario y la atribución de los commits queda inservible.
3. Crear el Git folder en el workspace apuntando a este repositorio.
4. Hacer un commit de prueba desde cada cuenta y verificar que el autor aparezca bien.

Ver [`docs/GUIA_ENTREGA.md`](./docs/GUIA_ENTREGA.md) para el detalle.

