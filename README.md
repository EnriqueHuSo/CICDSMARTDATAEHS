# Proyecto Final de Ingeniería de Datos con Azure Databricks

## 1. Descripción del proyecto

El proyecto implementa una solución de ingeniería de datos para integrar y analizar información comercial de ventas, clientes, productos, almacenes e inventario. La solución utiliza Azure Data Lake Storage Gen2 como almacenamiento de datos y Azure Databricks como plataforma de procesamiento, aplicando la arquitectura Medallion para organizar los datos en las capas Bronze, Silver y Golden.

El propósito es construir un flujo de datos que permita ingerir archivos CSV, conservar los datos originales, aplicar reglas de limpieza y calidad, integrar las entidades comerciales y generar un modelo dimensional preparado para su análisis y visualización.

Los datos utilizados son sintéticos y tienen fines académicos.

## 2. Objetivos

- Implementar una arquitectura Medallion en Azure Databricks.
- Almacenar los archivos de origen en Azure Data Lake Storage Gen2.
- Acceder al almacenamiento mediante Azure Managed Identity.
- Procesar los datos utilizando PySpark, Spark SQL y Delta Lake.
- Aplicar reglas de estandarización, validación, deduplicación y control de calidad.
- Construir dimensiones y tablas de hechos en la capa Golden.
- Administrar los datos mediante Unity Catalog.
- Preparar la información consolidada para su consumo en un dashboard de Databricks (AI/BI).
- Mantener el código y los entregables del proyecto en un repositorio público de GitHub y automatizar el despliegue CI/CD mediante GitHub Actions.

## 3. Caso de uso y datasets

El caso corresponde a un Lakehouse comercial que integra cinco conjuntos de datos:

| Archivo | Descripción | Registros de origen |
|---|---|---:|
| `clientes.csv` | Clientes y sus atributos comerciales. | 10,012 |
| `productos.csv` | Catálogo de productos, categoría, marca, costo y precio de referencia. | 1,000 |
| `ventas.csv` | Transacciones con fecha, cliente, producto, almacén, cantidad, precio y descuento. | 100,020 |
| `inventario.csv` | Existencias por producto y almacén para una fecha de corte. | 10,000 |
| `almacenes.csv` | Catálogo de almacenes y su ubicación. | 20 |
| **Total** | | **121,052** |

Los archivos incluyen situaciones de calidad intencionales, como registros duplicados, valores nulos, inconsistencias de formato y referencias a entidades inexistentes. Estas condiciones permiten aplicar y demostrar reglas de validación, tratamiento y registro de datos rechazados.

Las relaciones principales entre los datos son:

- `ventas.cliente_id` → `clientes.cliente_id`
- `ventas.producto_id` → `productos.producto_id`
- `ventas.almacen_id` → `almacenes.almacen_id`
- `inventario.producto_id` → `productos.producto_id`
- `inventario.almacen_id` → `almacenes.almacen_id`

El diccionario de datos y las reglas de los datasets se documentan en `documentacion/LEEME_diccionario_y_reglas.md`, y las reglas de negocio y calidad de Silver en `documentacion/Reglas_Negocio_y_Calidad_Silver_v1.1.md`.

## 4. Servicios y componentes implementados

La solución utiliza los siguientes servicios y componentes:

| Servicio o componente | Uso en el proyecto |
|---|---|
| Azure Resource Group | Agrupa los recursos de Azure del proyecto. |
| Azure Data Lake Storage Gen2 | Almacena los archivos CSV originales en el contenedor `raw` y los datos procesados en el contenedor `datalake`. |
| Azure Databricks Workspace | Proporciona el entorno de trabajo para desarrollar y ejecutar los procesos de datos. |
| Azure Databricks Access Connector | Proporciona una identidad administrada para el acceso de Databricks al almacenamiento. |
| Azure Managed Identity | Permite el acceso a ADLS Gen2 sin almacenar claves de cuenta en los notebooks. |
| Unity Catalog | Centraliza la organización y administración de los datos mediante un catálogo, esquemas y objetos de datos. |
| Storage Credential | Configura la identidad administrada utilizada por Unity Catalog para acceder al almacenamiento. |
| External Locations | Definen las ubicaciones externas de Raw y Datalake en ADLS Gen2. |
| Permisos de Unity Catalog (GRANTS) | Asignan privilegios sobre el catálogo y los esquemas Medallion. |
| Delta Sharing | Permite compartir las tablas de la capa Golden. |
| Clúster de Databricks | Ejecuta los notebooks de procesamiento en el entorno de producción. |
| GitHub | Aloja el código fuente y la documentación del proyecto. |
| GitHub Actions | Automatiza el despliegue de los notebooks a producción y la ejecución del Job `WF_ADB`. |
| Dashboard de Databricks (AI/BI) | Visualiza y analiza la información de la capa Golden. |

### Recursos principales aprovisionados

- Grupo de recursos: `rg-proyecto-final-ehs`
- Región: `East US 2`
- Storage Account: `stproyectofinalehs`
- Contenedores: `raw` y `datalake`
- Access Connector: `ac-proyecto-final-ehs`
- Workspace: `dbw-proyecto-final-ehs`
- Catálogo Unity Catalog: `dbw_proyecto_final_ehs`
- Storage Credential: `cred_adls_proyecto_final`
- External Location Raw: `extloc_adls_raw`
- External Location Datalake: `extloc_adls_datalake`

El acceso a los archivos Raw se configura mediante Managed Identity. Los archivos originales se mantienen en ADLS Gen2; no se utiliza DBFS ni Databricks Volumes como ubicación de Raw.

## 5. Arquitectura de la solución

La solución sigue la arquitectura Medallion:

```text
                 FUENTES DE DATOS
       clientes.csv | productos.csv | ventas.csv
          inventario.csv | almacenes.csv
                         |
                         v
             Azure Data Lake Storage Gen2
                  Contenedor: raw
                         |
             Acceso mediante Managed Identity
                         |
                         v
              AZURE DATABRICKS / PYSPARK
                         |
                         v
                 CAPA BRONZE (Delta)
             Datos ingeridos desde los CSV
             con metadatos de ingesta
                         |
                         v
                 CAPA SILVER (Delta)
          Estandarización y validación de datos
             Deduplicación y reglas de negocio
              Registros rechazados en tablas
                         |
                         v
                  CAPA GOLDEN (Delta)
               Modelo dimensional analítico
               Dimensiones y tablas de hechos
                         |
                         v
           DASHBOARD DE DATABRICKS (AI/BI)
              Análisis y visualización
```

Los datos procesados se almacenan en el contenedor `datalake` de ADLS Gen2 y se registran en Unity Catalog bajo el catálogo `dbw_proyecto_final_ehs`.

### 5.1 Bronze

La capa Bronze conserva la información recibida desde los cinco archivos CSV en formato Delta. Durante la ingesta se incorporan metadatos técnicos, como el archivo de origen y la fecha/hora de ingesta.

| Tabla | Registros |
|---|---:|
| `bronze.almacenes` | 20 |
| `bronze.clientes` | 10,012 |
| `bronze.inventario` | 10,000 |
| `bronze.productos` | 1,000 |
| `bronze.ventas` | 100,020 |
| **Total** | **121,052** |

### 5.2 Silver

La capa Silver aplica las transformaciones y reglas de calidad definidas para cada entidad. Entre las operaciones realizadas se encuentran la estandarización de campos, eliminación de duplicados excedentes, validación de claves foráneas, tratamiento de valores nulos y separación de registros rechazados.

Algunas reglas aplicadas son:

- Los clientes duplicados se identifican por `cliente_id`; se conserva un registro válido y los excedentes se registran como rechazados.
- Los productos sin `costo_unitario` se rechazan.
- Los nombres y atributos de almacenes se estandarizan mediante limpieza de espacios.
- El inventario se valida contra los productos de origen y se rechazan las referencias inexistentes.
- Las ventas se validan por identificadores, fecha y referencias a clientes, productos y almacenes. Los descuentos nulos se normalizan a cero y los registros inválidos se registran en tablas de rechazados.
- Las validaciones de referencias se realizan contra las tablas maestras de Bronze, conservando la trazabilidad de los datos de origen.

Resultados de las tablas Silver:

| Tabla | Registros |
|---|---:|
| `silver.almacenes` | 20 |
| `silver.clientes` | 10,000 |
| `silver.inventario` | 9,980 |
| `silver.productos` | 995 |
| `silver.ventas` | 99,920 |

Los registros rechazados se conservan en tablas de control o cuarentena según el proceso, en lugar de descartarse silenciosamente.

**Tablas de registros rechazados**

| Tabla | Descripción | Registros |
|---|---|---:|
| `silver.clientes_rechazados` | Clientes duplicados excedentes (`REGISTRO_DUPLICADO`). | 12 |
| `silver.productos_rechazados` | Productos sin `costo_unitario` (`COSTO_UNITARIO_NULO`). | 5 |
| `silver.inventario_rechazados` | Inventario con productos inexistentes en Bronze (`PRODUCTO_NO_EXISTE`). | 20 |
| `silver.ventas_rechazados` | Ventas duplicadas, con referencias inexistentes o con `fecha_venta` nula. | 100 |

`silver.almacenes` no tiene tabla de rechazados, ya que no se detectaron registros inválidos.

### 5.3 Golden

La capa Golden contiene el modelo dimensional preparado para el análisis. Se construyeron cuatro dimensiones y dos tablas de hechos.

**Dimensiones**

| Tabla | Descripción | Registros |
|---|---|---:|
| `golden.dim_fecha` | Dimensión calendario. | 365 |
| `golden.dim_cliente` | Información de clientes. | 10,000 |
| `golden.dim_producto` | Información de productos. | 995 |
| `golden.dim_almacen` | Información de almacenes. | 20 |

**Tablas de hechos**

| Tabla | Descripción | Registros |
|---|---|---:|
| `golden.fact_ventas` | Hechos de ventas con métricas de importes, descuentos, costos y utilidad bruta estimada. | 99,407 |
| `golden.fact_inventario` | Hechos de inventario por fecha, producto y almacén. | 9,915 |

Durante la construcción de las tablas de hechos, los registros que no pudieron asociarse con las dimensiones se conservaron en tablas de registros no mapeados para mantener la trazabilidad.

En `fact_ventas` se registraron 513 ventas no mapeadas por productos que no contaban con correspondencia en la dimensión de productos. En `fact_inventario` se registraron 40 registros no mapeados y 25 registros rechazados por stock negativo.

**Tablas de control**

| Tabla | Descripción | Registros |
|---|---|---:|
| `golden.fact_ventas_no_mapeadas` | Ventas sin correspondencia en `dim_producto`. | 513 |
| `golden.fact_inventario_no_mapeados` | Registros de inventario sin correspondencia en las dimensiones. | 40 |
| `golden.fact_inventario_rechazados` | Registros de inventario rechazados por stock negativo. | 25 |

Las métricas de costo y utilidad de ventas son estimaciones calculadas con el costo disponible en la dimensión de productos; no representan necesariamente el costo histórico de cada transacción.

## 6. Organización del repositorio

El repositorio mantiene las once carpetas definidas para la entrega del proyecto:

```text
CICDSMARTDATAEHS/
├── prepAmb/
├── datasets/
├── dashboard/
├── reversion/
├── workflow/
├── seguridad/
├── delta-sharing/
├── proceso/
├── certificaciones/
├── evidencias/
└── documentacion/
```

| Carpeta | Propósito |
|---|---|
| `prepAmb/` | Notebook de preparación y validación del ambiente de trabajo. |
| `datasets/` | Archivos CSV de entrada. |
| `dashboard/` | Elementos relacionados con el dashboard de Databricks (AI/BI). |
| `reversion/` | Notebook destinado a la reversión controlada del proceso en caso de algún inconveniente. |
| `workflow/` | Archivos relacionados con el flujo de trabajo y CI/CD. |
| `seguridad/` | Configuraciones y documentación de seguridad y permisos. |
| `delta-sharing/` | Notebook para compartir las tablas de la capa Golden mediante Delta Sharing. |
| `proceso/` | Notebooks de preparación, ingesta Bronze, transformaciones Silver y construcción Golden destinados al procesamiento. |
| `certificaciones/` | Certificaciones obtenidas en Databricks Academy. |
| `evidencias/` | Capturas y evidencias de ejecución y configuración. |
| `documentacion/` | Documentación complementaria, diccionario de datos y reglas de negocio. |

Los notebooks de procesamiento que corresponden a la ejecución productiva se mantienen en `proceso/`. El notebook de limpieza controlada de las capas Medallion se mantiene separado en `reversion/` y no forma parte de la ejecución habitual.

## 7. Notebooks de procesamiento

Los notebooks principales de la ETL se organizan en el siguiente orden:

| Orden | Etapa | Notebook |
|---:|---|---|
| 1 | Preparación | `1.Preparacion_Ambiente` |
| 2 | Bronze | `2.Ingesta_Bronze_Almacenes` |
| 2 | Bronze | `2.Ingesta_Bronze_Clientes` |
| 2 | Bronze | `2.Ingesta_Bronze_Inventario` |
| 2 | Bronze | `2.Ingesta_Bronze_Productos` |
| 2 | Bronze | `2.Ingesta_Bronze_Ventas` |
| 3 | Silver | `3.Transformacion_Silver_Clientes` |
| 3 | Silver | `3.Transformacion_Silver_Productos` |
| 3 | Silver | `3.Transformacion_Silver_Almacenes` |
| 3 | Silver | `3.Transformacion_Silver_Inventario` |
| 3 | Silver | `3.Transformacion_Silver_Ventas` |
| 4 | Golden | `4.Construccion_Golden_Dimensiones` |
| 4 | Golden | `4.Construccion_Golden_Fact_Ventas` |
| 4 | Golden | `4.Construccion_Golden_Fact_Inventario` |

`1.Preparacion_Ambiente` verifica y prepara los recursos necesarios para el procesamiento. `1.Drop_Medallion`, ubicado en `reversion/`, es un notebook de limpieza controlada y se encuentra fuera del flujo habitual de ejecución.

## 8. Tecnologías utilizadas

- Microsoft Azure
- Azure Data Lake Storage Gen2
- Azure Databricks
- Azure Managed Identity
- Unity Catalog
- PySpark
- Spark SQL
- Delta Lake
- Git y GitHub
- GitHub Actions (CI/CD)
- Delta Sharing
- Databricks AI/BI Dashboards

## 9. Ejecución general del procesamiento

El procesamiento se realiza mediante los notebooks ubicados en `proceso/`, siguiendo el orden indicado en la sección 7:

1. Preparar y validar el ambiente de trabajo.
2. Ingerir los archivos CSV desde Raw hacia Bronze.
3. Ejecutar las transformaciones y controles de calidad de Silver.
4. Construir las dimensiones de Golden.
5. Construir la tabla de hechos de ventas.
6. Construir la tabla de hechos de inventario.

La ejecución requiere que los recursos de Azure, las credenciales de Unity Catalog y el clúster de producción estén configurados y disponibles. Las rutas de almacenamiento y el catálogo corresponden a los recursos indicados en la sección 4.

En producción, el workflow de GitHub Actions `deploy-notebook.yml` despliega los notebooks de `proceso/` y crea o actualiza el Job `WF_ADB`, que ejecuta las tareas en el orden y con las dependencias indicadas.

## 10. Repositorio

El código fuente y la documentación del proyecto se alojan en un repositorio público de GitHub:

https://github.com/EnriqueHuSo/CICDSMARTDATAEHS

## 11. Consideraciones

- Los datos de entrada son sintéticos y no representan clientes ni transacciones reales.
- Las capas Bronze, Silver y Golden se almacenan en formato Delta y se administran mediante Unity Catalog.
- Raw permanece en Azure Data Lake Storage Gen2 y el acceso se realiza mediante Managed Identity.
- Los registros rechazados y no mapeados se conservan para facilitar la revisión y trazabilidad de la calidad de los datos.
- La visualización de los datos de Golden se realiza mediante el dashboard de Databricks (AI/BI) ubicado en `dashboard/`.

