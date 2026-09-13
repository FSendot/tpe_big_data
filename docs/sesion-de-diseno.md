# Sesión de diseño

Modalidad: entrevista por rondas con registro de términos y decisiones.
Alcance confirmado por el usuario: trabajar en la primera entrega y usar el
repositorio personal `FSendot/tpe_big_data`.

## Primera ronda pendiente

1. Prioridad analítica y decisión que queremos habilitar para sus usuarios.
2. Integrantes, disponibilidad y recursos para estimar un plan viable.
3. Restricciones o validaciones del entorno de ejecución.

Acceso a datos resuelto: el usuario aportó el paquete del campus
`cloud_provider_challenge_dataset_v1`. Se verificó la presencia de las ocho
fuentes y su lectura básica. Ver [inventario inicial](../data/README.md).

## Decisiones dependientes

- Prioridad analítica + datos observados: preguntas de negocio, métricas y criterios de éxito.
- Recursos + entorno + latencias: patrón arquitectónico y herramientas.
- Perfil de fuentes + consultas: grano, calidad, evolución de esquema y diseño del Data Lake.
- Flujos + zonas: lógica batch equivalente a MapReduce y matriz requisito-componente.
- Decisiones anteriores: riesgos, esfuerzo, diagrama v1 y documento de entrega.

Estas ramas se revisarán con cada respuesta. Las recomendaciones de la
entrevista no se consideran decisiones aceptadas hasta que el usuario las
resuelva. El glosario se creará al acordar términos del dominio; los ADR se
crearán cuando exista una decisión que justifique ese registro.
