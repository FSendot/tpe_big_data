# Cloud Provider Analytics

Vocabulario acordado para analizar el consumo y los costos de un proveedor cloud.

## Lenguaje

**FinOps**:
Equipo que gestiona el gasto cloud y es el destinatario principal del análisis
de aumentos del costo de uso por organización y servicio.

**Costo de uso**:
Importe incremental en USD asociado al consumo registrado en los eventos.
Es la medida principal para analizar aumentos; su equivalencia con el importe
de una factura no está establecida.
_Evitar_: Facturación, ingreso, margen, costo interno del proveedor.

**Aumento relativo**:
Incremento del costo de uso expresado como porcentaje del costo del período
de referencia.
_Evitar_: Aumento absoluto como sinónimo.

**Aumento absoluto**:
Diferencia en USD entre el costo de uso de un período y el del período de
referencia.
_Evitar_: Aumento relativo como sinónimo.

**Costo diario**:
Suma del costo de uso de una organización y servicio durante un día calendario
en UTC.

**Costo horario**:
Suma del costo de uso de una organización y servicio durante una hora
calendario UTC. Puede publicarse y actualizarse antes de que termine esa hora.

**Día completo**:
Día calendario UTC que terminó. Sus resultados pueden corregirse si llegan
eventos adicionales; el término no garantiza que todos los datos hayan llegado.

**Evento tardío**:
Evento que llega después de publicar un resultado del período en el que ocurrió
y que puede requerir corregir ese resultado.

**Sin base comparable**:
Estado de una comparación cuyo día de referencia carece de registros o tiene
costo cero, por lo que no se le asigna un aumento porcentual.

**Costo pendiente de validación**:
Costo cuyo significado o validez no están establecidos. En este caso incluye
los costos negativos, que podrían representar ajustes o errores.
También incluye contribuciones de costo contradictorias sin una revisión
confiable que permita resolverlas.
_Evitar_: Outlier o error confirmado como sinónimos.

**Total parcial**:
Agregado calculado con los valores utilizables de una medida cuando otros
valores del mismo grupo faltan o siguen pendientes de validación. El estado
se evalúa por medida: un consumo parcial no implica por sí solo un costo parcial.

**Pendiente de recálculo**:
Resultado que necesita actualizarse porque se detectaron novedades o conflictos
que su valor publicado todavía no refleja. Recalcularlo puede eliminar esa
espera y mantenerlo parcial si siguen existiendo datos no utilizables.
_Evitar_: Total parcial como sinónimo.

**Revenue mensual**:
Importe de facturación de una organización y mes, neto de créditos e incluidos
los impuestos según la convención acordada, expresado en USD. Puede quedar
sin importe calculable si sus componentes requieren validación.
_Evitar_: Costo de uso o margen como sinónimos.
