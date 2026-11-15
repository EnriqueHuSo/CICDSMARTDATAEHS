# Reglas de negocio y calidad de datos --- Capa Silver

**Proyecto:** Proyecto final de Ingeniería de Datos con Azure
Databricks\
**Versión:** 1.1\
**Fecha:** 09/11/2026\
**Estado:** Documento de trabajo. Los supuestos de negocio deben
confirmarse con el responsable del proceso en un entorno real.

## 1. Objetivo y alcance

Centralizar las reglas de negocio, calidad y transformación aplicadas al
pasar de Bronze a Silver para los datasets `clientes`, `productos`,
`almacenes`, `inventario` y `ventas`.

Los notebooks contienen la implementación PySpark. Este documento
explica qué regla se aplica, por qué y cuál es el resultado esperado.

## 2. Principios de transformación

1.  Bronze conserva los datos originales y no se modifica durante la
    limpieza.
2.  Silver contiene registros depurados y estructurados.
3.  No se inventan valores sin una regla justificada.
4.  Los registros que incumplen reglas de aceptación se conservan en
    tablas de rechazados, con motivo y fecha de proceso cuando
    corresponda.
5.  Se conserva la trazabilidad mediante `_source_file` y
    `_ingestion_timestamp`.
6.  Se concilian los registros de origen con los válidos y rechazados,
    sin pérdidas ni doble contabilización.

**Catálogo:** `dbw_proyecto_final_ehs`\
**Esquemas:** `bronze`, `silver`\
**Contenedor físico:** `datalake`

## 3. Resumen de tablas Silver

  Dataset      Tabla válida          Tabla de rechazados
  ------------ --------------------- --------------------------------
  Clientes     `silver.clientes`     `silver.clientes_rechazados`
  Productos    `silver.productos`    `silver.productos_rechazados`
  Almacenes    `silver.almacenes`    No aplica
  Inventario   `silver.inventario`   `silver.inventario_rechazados`
  Ventas       `silver.ventas`       `silver.ventas_rechazados`

## 4. Reglas comunes

  ------------------------------------------------------------------------
  Código                  Regla                   Tratamiento
  ----------------------- ----------------------- ------------------------
  COM-001                 Preservar origen        No modificar Bronze.

  COM-002                 Trazabilidad            Conservar `_source_file`
                                                  y `_ingestion_timestamp`
                                                  en Silver.

  COM-003                 Cuarentena              Registrar
                                                  `motivo_rechazo` y
                                                  `fecha_proceso` en
                                                  rechazados.

  COM-004                 Conciliación            Validar Bronze =
                                                  Silver + rechazados por
                                                  registros de origen
                                                  únicos.

  COM-005                 Persistencia            Guardar tablas Silver en
                                                  formato Delta y
                                                  registrarlas en Unity
                                                  Catalog.
  ------------------------------------------------------------------------

## 5. Clientes

### Diagnóstico

  Indicador                             Resultado
  ----------------------------------- -----------
  Bronze                                   10,012
  Grupos duplicados de `cliente_id`            12
  Excedentes por duplicidad                    12
  `ciudad` nula                                30

Los duplicados son pares: uno contiene el marcador `DUPLICADO` en el
nombre y el otro conserva el nombre sin ese marcador; los demás campos
coinciden.

### Reglas

  ------------------------------------------------------------------------
  Código           Regla            Tratamiento                  Resultado
  ---------------- ---------------- ---------------- ---------------------
  CLI-001          Duplicado        Conservar el             12 rechazados
                   marcado          registro sin     
                   `DUPLICADO`      marcador y       
                                    enviar el        
                                    marcado a        
                                    rechazados.      

  CLI-002          `ciudad` nula    Conservar NULL;         30 conservados
                                    no inferir una   
                                    ciudad.          

  CLI-003          Trazabilidad     Conservar campos              Aplicado
                                    de auditoría.    
  ------------------------------------------------------------------------

**Resultado:** `silver.clientes` = 10,000; `silver.clientes_rechazados`
= 12, motivo `REGISTRO_DUPLICADO`.\
**Conciliación:** 10,012 = 10,000 + 12.

## 6. Productos

### Diagnóstico

  Indicador                       Resultado
  ----------------------------- -----------
  Bronze                              1,000
  `costo_unitario` nulo                   5
  Duplicados de `producto_id`             0

### Reglas

  --------------------------------------------------------------------------
  Código           Regla              Tratamiento                  Resultado
  ---------------- ------------------ ---------------- ---------------------
  PRO-001          `costo_unitario`   Enviar a                  5 rechazados
                   nulo               rechazados; no   
                                      inventar un      
                                      costo.           

  PRO-002          `producto_id`      Validar                              0
                   duplicado          unicidad; no se  
                                      detectaron       
                                      casos.           

  PRO-003          Trazabilidad       Conservar campos              Aplicado
                                      de auditoría.    
  --------------------------------------------------------------------------

**Resultado:** `silver.productos` = 995; `silver.productos_rechazados` =
5, motivo `COSTO_UNITARIO_NULO`.\
**Conciliación:** 1,000 = 995 + 5.

**Criterio referencial:** un producto rechazado de Silver por costo nulo
sigue existiendo como identificador en Bronze. No debe confundirse la
existencia de la clave con la aceptación del registro maestro en Silver.

## 7. Almacenes

### Diagnóstico

  Indicador                              Resultado
  ------------------------------------ -----------
  Bronze                                        20
  Campos obligatorios nulos o vacíos             0
  Duplicados de `almacen_id`                     0

### Reglas

  -----------------------------------------------------------------------
  Código                  Regla                   Tratamiento
  ----------------------- ----------------------- -----------------------
  ALM-001                 Normalización de texto  Aplicar `trim()` a
                                                  `nombre_almacen`,
                                                  `ciudad` y `region`.

  ALM-002                 Campos obligatorios     Validar nulos/vacíos;
                                                  no se detectaron.

  ALM-003                 Unicidad                Validar `almacen_id`;
                                                  no se detectaron
                                                  duplicados.

  ALM-004                 Rechazados              No crear tabla de
                                                  rechazados al no
                                                  existir registros
                                                  inválidos detectados.
  -----------------------------------------------------------------------

**Resultado:** `silver.almacenes` = 20; sin tabla de rechazados.

## 8. Inventario

### Diagnóstico

  Indicador                                Resultado
  -------------------------------------- -----------
  Bronze                                      10,000
  Campos obligatorios nulos                        0
  Productos inexistentes                          20
  Almacenes inexistentes                           0
  Grupos duplicados de clave compuesta             0

Clave compuesta: `fecha_corte`, `producto_id`, `almacen_id`.

### Reglas

  --------------------------------------------------------------------------------------
  Código           Regla            Tratamiento                                Resultado
  ---------------- ---------------- ------------------------------ ---------------------
  INV-001          Campos           Rechazar con                                       0
                   obligatorios     `CAMPOS_OBLIGATORIOS_NULOS`.   
                   nulos                                           

  INV-002          `producto_id`    Rechazar con                                      20
                   inexistente en   `PRODUCTO_NO_EXISTE`.          
                   Bronze                                          

  INV-003          `almacen_id`     Validar integridad                                 0
                   inexistente      referencial.                   

  INV-004          Clave compuesta  Validar unicidad.                                  0
                   duplicada                                       

  INV-005          Existencia del   Comparar contra                             Aplicado
                   producto         `bronze.productos`, no         
                                    `silver.productos`.            

  INV-006          Trazabilidad     Conservar campos de auditoría.              Aplicado
  --------------------------------------------------------------------------------------

Se consulta `bronze.productos` porque los productos rechazados en Silver
por `costo_unitario` nulo siguen existiendo en origen.

**Resultado:** `silver.inventario` = 9,980;
`silver.inventario_rechazados` = 20, motivo observado
`PRODUCTO_NO_EXISTE`.\
**Conciliación:** 10,000 = 9,980 + 20.

## 9. Ventas

### Diagnóstico

  Indicador                               Resultado
  ------------------------------------- -----------
  Bronze                                    100,020
  Grupos duplicados de `venta_id`                20
  Registros excedentes por duplicidad            20
  Clientes inexistentes                          15
  Productos inexistentes                         25
  Almacenes inexistentes                          0
  `fecha_venta` nula                             40
  `descuento_pct` nulo                          100

Las incidencias de integridad referencial pueden coincidir en una misma
venta. Por eso, 15 + 25 no representa necesariamente 40 ventas
distintas.

### Reglas acordadas para implementar

  -----------------------------------------------------------------------
  Código                  Regla                   Tratamiento
  ----------------------- ----------------------- -----------------------
  VEN-001                 `venta_id` duplicado    Conservar una
                                                  ocurrencia y enviar
                                                  excedentes a
                                                  rechazados. Los pares
                                                  observados son
                                                  idénticos.

  VEN-002                 `cliente_id`            Rechazar la venta.
                          inexistente             

  VEN-003                 `producto_id`           Rechazar la venta.
                          inexistente             

  VEN-004                 `almacen_id`            Rechazar la venta.
                          inexistente             

  VEN-005                 `descuento_pct` NULL    Normalizar a 0 %.

  VEN-006                 Varias referencias      Rechazar la venta una
                          inválidas en una venta  sola vez y registrar
                                                  todos los motivos
                                                  aplicables.

  VEN-007                 Trazabilidad            Conservar campos de
                                                  auditoría.

  VEN-008                 `fecha_venta` NULL      Rechazar con motivo
                                                  `FECHA_VENTA_NULA`.
                                                  La fecha es necesaria
                                                  para el análisis
                                                  temporal.
  -----------------------------------------------------------------------

### Justificación de `descuento_pct`

Se detectaron 100 ventas con `descuento_pct` nulo. Para este proyecto se
adopta la regla funcional:

**Cuando `descuento_pct` sea NULL, se interpretará como una venta sin
descuento y se normalizará a 0 %.**

La decisión evita excluir ventas potencialmente válidas solo por la
ausencia del porcentaje de descuento. Rechazarlas reduciría
innecesariamente el universo de ventas y podría sesgar los indicadores y
reportes. Además, un porcentaje igual a cero permite realizar los
cálculos de importes sin propagar NULL.

Bronze conserva el valor original, por lo que la normalización de Silver
es trazable.

**Naturaleza de la regla:** es un supuesto de negocio adoptado para el
proyecto académico, no una confirmación del sistema transaccional ni del
gerente de ventas. En un entorno real debe validarse con el responsable
del proceso o con la documentación funcional de la fuente.

### Integridad referencial

La existencia de clientes y productos se distingue de su aceptación en
Silver. La validación debe identificar si la clave realmente no existe
en Bronze, sin confundirla con un registro maestro que existe en Bronze
pero fue rechazado de Silver por otra regla. La estrategia de consumo de
maestros rechazados deberá evaluarse explícitamente al diseñar Gold.

**Estado:** reglas definidas; notebook y conteos finales pendientes de
implementación y validación.

## 10. Gestión de rechazados

1.  No eliminar físicamente registros de Bronze.
2.  Rechazar en Silver los registros que incumplen reglas que impiden su
    uso confiable.
3.  Registrar un motivo estable y comprensible.
4.  Registrar `fecha_proceso`.
5.  Si una fila incumple varias reglas, evitar duplicarla en cuarentena
    y conservar todos los motivos aplicables.
6.  Conservar la trazabilidad en registros válidos.

Motivos ya utilizados: - Clientes: `REGISTRO_DUPLICADO`. - Productos:
`COSTO_UNITARIO_NULO`. - Inventario: `CAMPOS_OBLIGATORIOS_NULOS`,
`PRODUCTO_NO_EXISTE`.

La regla VEN-008 se incorporó al notebook de Ventas después de detectar 40 registros con `fecha_venta` nula durante la ejecución del diagnóstico. Estos registros se rechazan; no se imputan fechas.

## 11. Controles de aceptación

Cada notebook Silver debe validar, según corresponda:

-   Conteo de origen, válidos y rechazados.
-   Conciliación entre origen, válidos y rechazados.
-   Unicidad de claves primarias o compuestas.
-   Integridad referencial.
-   Muestras de registros válidos y rechazados.
-   Conteos de las tablas Delta después de persistirlas.

La conciliación debe considerar registros de origen únicos. Una fila con
varios motivos de rechazo se cuenta una sola vez en el total de
rechazados.

## 12. Estado de implementación

  Dataset      Estado                    Resultado
  ------------ ------------------------- ----------------------------------------------
  Clientes     Implementado y validado   10,000 válidos; 12 rechazados
  Productos    Implementado y validado   995 válidos; 5 rechazados
  Almacenes    Implementado y validado   20 válidos; sin rechazados
  Inventario   Implementado y validado   9,980 válidos; 20 rechazados
  Ventas       En implementación         VEN-008 incorporada en notebook v2; conteos finales pendientes

## Historial de cambios

- **Versión 1.1 — 09/11/2026:** se formaliza VEN-008 para rechazar las 40 ventas con `fecha_venta` nula y se actualiza la versión del notebook de Ventas a v2.

## 13. Mantenimiento

Actualizar este documento cuando cambie una regla, se descubra una
incidencia nueva, cambie un criterio de aceptación, se incorpore un
dataset o un supuesto sea confirmado por el responsable del negocio.

Toda regla nueva debe registrar código, descripción, justificación,
tratamiento, resultado esperado y estado: propuesta, aprobada o
implementada.

------------------------------------------------------------------------

**Nota:** este documento centraliza las reglas funcionales y de calidad.
No sustituye los notebooks ni sus resultados de ejecución; ambos son
evidencias complementarias de la implementación.
