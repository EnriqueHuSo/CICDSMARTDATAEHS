# Insumos del proyecto ETL con Databricks

## Caso
Lakehouse comercial para integrar ventas, clientes, productos e inventario. Los datos son sintéticos y se pueden usar en una evaluación académica.

## Archivos
- `clientes.csv`: clientes y atributos comerciales.
- `productos.csv`: catálogo de productos, categoría, marca, costo y precio de referencia.
- `ventas.csv`: transacciones de venta con fecha, cliente, producto, almacén, cantidad, precio y descuento.
- `inventario.csv`: snapshot de inventario al 2025-12-31 por producto y almacén.
- `almacenes.csv`: catálogo de almacenes y ubicación.

## Volúmenes aproximados
- clientes.csv: 10,012 filas (incluye 12 duplicados intencionales por cliente_id).
- productos.csv: 1,000 filas.
- ventas.csv: 100,020 filas (incluye 20 registros duplicados intencionales por venta_id).
- inventario.csv: 10,000 filas.
- almacenes.csv: 20 filas.

## Reglas de negocio sugeridas
1. `venta_id`, `cliente_id`, `producto_id` y `almacen_id` son identificadores.
2. `cantidad` debe ser mayor que cero.
3. `descuento_pct` debe estar entre 0 y 100. Puedes decidir si los nulos se reemplazan por cero o se envían a cuarentena.
4. `precio_unitario` debe ser mayor que cero.
5. `costo_unitario` y `precio_venta` deben ser válidos y no negativos.
6. La venta debe referenciar un cliente, producto y almacén existentes.
7. `stock_disponible` no debe ser negativo.
8. Para calcular importes:
   - `importe_bruto = cantidad * precio_unitario`
   - `importe_descuento = importe_bruto * descuento_pct / 100`
   - `importe_neto = importe_bruto - importe_descuento`
   - `costo_estimado = cantidad * costo_unitario`
   - `utilidad_bruta_estimada = importe_neto - costo_estimado`
9. Para datos duplicados, definir una estrategia explícita en Silver (por ejemplo, conservar una fila por identificador con una regla de desempate).
10. No descartar silenciosamente registros inválidos: registrar los rechazados en tablas de cuarentena o de errores.

## Problemas de calidad intencionales
- Clientes: ciudades nulas, espacios/mayúsculas inconsistentes y 12 filas duplicadas por `cliente_id`.
- Productos: categorías con espacios/mayúsculas inconsistentes y costos nulos.
- Ventas: descuentos nulos y fuera de rango, formatos de fecha mezclados, cantidades cero/negativas, referencias a clientes/productos inexistentes y 20 filas duplicadas por `venta_id`.
- Inventario: stock negativo y productos inexistentes.
- Estos errores son intencionales para permitir demostrar limpieza, validaciones, métricas de calidad y cuarentena.

## Relaciones sugeridas
- `ventas.cliente_id` -> `clientes.cliente_id`
- `ventas.producto_id` -> `productos.producto_id`
- `ventas.almacen_id` -> `almacenes.almacen_id`
- `inventario.producto_id` -> `productos.producto_id`
- `inventario.almacen_id` -> `almacenes.almacen_id`

## Recomendación de arquitectura
- Bronze: copia de los CSV originales a Delta, agregando metadatos de ingesta.
- Silver: estandarización, deduplicación, validaciones, joins de referencia y cuarentena.
- Golden: `dim_cliente`, `dim_producto`, `dim_almacen`, `dim_fecha`, `fact_ventas` y `fact_inventario`, consumidas por el dashboard de Databricks (AI/BI).

Los datos son sintéticos y no contienen información real de personas ni transacciones.
