# Wikipedia Article Analysis

Trabajo práctico grupal de Sistemas Distribuidos. El sistema procesa artículos de Wikipedia en inglés y francés y genera los resultados de cinco consultas mediante una arquitectura distribuida basada en RabbitMQ.

> El hito de escalabilidad está terminado. Están implementadas la ejecución serial de referencia, la lectura incremental del dataset, el cliente, el Gateway, el protocolo externo, la capa común de middleware y el pipeline distribuido Q1-Q5. Los componentes replicables y las etapas stateful de las cinco consultas se despliegan con tres réplicas o particiones, salvo Gateway y ResultSink, que permanecen como instancias únicas. El pipeline completo fue validado con 50.000 artículos y sus cinco archivos coinciden con la referencia serial. El trabajo actual es el hito de tolerancia a fallos; su alcance, propuesta y plan se documentan debajo antes de modificar código funcional.

## Consultas

El sistema debe resolver las siguientes consultas:

1. **Query 1:** URL y fecha de modificación de artículos escritos en inglés hasta 2020 inclusive.
2. **Query 2:** nombre, URL e imagen en bytes de artículos hasta 2020 inclusive que contengan una sección `biography` o `biographie`, ignorando mayúsculas.
3. **Query 3:** palabras sin duplicados que aparezcan en al menos un artículo en inglés y en al menos uno en francés.
4. **Query 4:** las 20 palabras más utilizadas en los abstracts, excluyendo stopwords. Cada palabra cuenta como máximo una vez por abstract.
5. **Query 5:** URL y cantidad de palabras de los abstracts desde 2021 cuya cantidad de palabras supere el promedio de los abstracts hasta 2020 inclusive. Se utiliza el mismo criterio de conteo que en Query 4.

## Estado actual

| Funcionalidad | Estado |
|---|---|
| Lectura incremental del dataset Parquet | Implementado |
| Ejecución serial de referencia para Q1–Q5 | Implementado |
| Comparación automática entre resultados seriales y distribuidos | Implementado |
| Protocolo TCP entre cliente y Gateway | Implementado |
| Protocolo binario entre cliente y Gateway | Implementado |
| Serialización JSON de los mensajes internos de RabbitMQ | Implementado |
| Streaming de artículos desde el cliente | Implementado |
| Identificador único por ejecución | Implementado |
| Particionado estable Gateway → InputBatcher | Implementado |
| Middleware común sobre RabbitMQ | Implementado |
| Cola temporal de resultados por ejecución | Implementado |
| InputBatcher con batches limitados por bytes | Implementado con tres particiones |
| Validación y normalización de artículos en Preprocessor | Implementado |
| Fan-out y proyecciones mínimas en QueryRouter | Implementado |
| Filtro temporal compartido de Q1 y Q2 | Implementado con tres réplicas coordinadas |
| Filtro de idioma y resultados de Q1 | Implementado con tres réplicas coordinadas |
| Filtro de secciones biográficas de Q2 | Implementado con tres réplicas coordinadas |
| Descarga de imágenes y resultados de Q2 | Implementado con tres réplicas y descarga secuencial por instancia |
| Tokenización compartida en WordExtractor | Implementado con tres réplicas coordinadas |
| Proyección y agregación de palabras compartidas de Q3 | Implementado con tres proyectores y tres particiones de agregación |
| Proyección compartida de Q4/Q5 | Implementado con tres réplicas coordinadas |
| Agregación y resultados de Q4 | Implementado con tres counters y tres reducers distribuidos por ejecución |
| Separación temporal de Q5 en Q5DateRouter | Implementado con tres réplicas coordinadas |
| Estadísticas, promedio y comparación de Q5 | Implementado con tres particiones por etapa y buffer JSONL en volumen Docker |
| ResultSink | Implementado como instancia única con barreras para Q1, Q2, Q3 y Q5; Q4 converge en un reducer lógico |
| Atención concurrente de varios clientes | Implementada con una sesión por thread |
| Graceful shutdown por SIGTERM/SIGINT | Implementado |
| Estrategia de tolerancia a fallos y persistencia | Planificada; implementación pendiente |
| Detección de procesos caídos mediante comunicación | Pendiente |
| Consenso implementado por el grupo | Pendiente |
| Reinicio automático de procesos caídos | Pendiente |
| Recuperación e idempotencia después de reinicios | Pendiente |
| Entrega exactly-once hacia el cliente | Pendiente |
| Chaos Monkey y escenarios de fallos automatizados | Pendiente |
| Comparación de performance contra el hito de escalabilidad | Pendiente |
| Limpieza de recursos de ejecuciones resueltas o fallidas | Parcial; debe generalizarse y probarse |
| Gateway dentro del modelo de fallos | Excluido por indicación de la cátedra; se asume disponible y debe mantenerse mínimo |

## Hito 3: tolerancia a fallos

Esta sección es el tablero interno para el último hito. Distingue requisitos de
la consigna, riesgos comprobados en el código actual, decisiones todavía abiertas
y tareas verificables. Los checks deben marcarse únicamente cuando exista código,
prueba automatizada y evidencia reproducible.

### Alcance obligatorio y evidencia esperada

| Requisito | Evidencia mínima para considerarlo cumplido |
|---|---|
| Tolerar fallas abruptas dentro de un proceso y caídas repentinas de otros nodos | Una ejecución iniciada antes de la caída termina con los cinco resultados correctos después de reiniciar el proceso afectado |
| Reiniciar procesos caídos | Los supervisores reponen automáticamente el proceso sin intervención manual |
| Verificar disponibilidad comunicándose con el nodo | La detección utiliza un protocolo de salud propio; no consulta estado, eventos ni metadatos de Docker |
| Implementar un algoritmo de consenso | Tres supervisores implementan elección de líder y acuerdo por mayoría sin bibliotecas externas |
| Chaos Monkey propio | Una herramienta del repositorio introduce fallas periódicas y configurables durante una ejecución |
| Automatizar los escenarios de fallos | Un comando levanta el entorno, ejecuta cliente y fallas, espera la recuperación, compara resultados y verifica limpieza |
| No usar bibliotecas externas de consenso, tolerancia o recuperación | El protocolo, la persistencia y la recuperación se implementan en el proyecto; cualquier dependencia nueva se consulta antes con la cátedra |
| Limitar Docker-in-Docker o el socket de Docker al reinicio | Docker se usa únicamente para matar procesos en Chaos Monkey y para ejecutar un reinicio ya decidido por consenso |
| Eliminar archivos y liberar recursos | Cada ejecución exitosa o fallida elimina colas temporales, journals, snapshots, outboxes y buffers propios cuando ya no son necesarios |
| Entrega exactly-once al cliente | Una reentrega o reconexión nunca agrega dos veces el mismo resultado lógico al archivo del cliente |
| Documento 4+1 o C4 actualizado | `INFORME.md` y los diagramas reflejan detección, consenso, reinicio, recuperación, limpieza y escenarios de fallos |
| Comparar performance con escalabilidad | Benchmark automatizado con el mismo dataset y topología, varias corridas y métricas comparables con y sin tolerancia habilitada |
| Demo con muestra significativa y fallas periódicas | Guion ensayado que muestra fallas, elección/acuerdo, reinicio, recuperación, resultados correctos y recursos finales limpios |

Los requisitos del hito de escalabilidad siguen vigentes: no se puede degradar el
streaming, la coordinación de barreras, el uso del middleware, la capacidad de
escalar ni la comparación contra la referencia serial.

### Modelo de fallos acordado: Gateway estable y mínimo

La cátedra indicó que para el desarrollo, las pruebas y la demo se puede asumir
que el Gateway no se cae. Por lo tanto:

- Gateway queda fuera del conjunto de procesos supervisados y fuera de los targets
  de Chaos Monkey.
- No se implementarán consenso, checkpointing, journal, inbox/outbox ni reinicio
  automático específicos para una caída del Gateway.
- Gateway no debe almacenar estado de negocio, agregaciones, deduplicación global
  ni decisiones de recuperación.
- Sus responsabilidades se limitan a la frontera: aceptar conexiones, adaptar el
  protocolo TCP al protocolo interno, crear y propagar `execution_id`, aplicar el
  routing inicial, reenviar resultados y coordinar el ACK del cliente.
- La persistencia, deduplicación y recuperación del procesamiento pertenecen al
  pipeline y a ResultSink. La deduplicación del efecto final pertenece al cliente.
- Una nueva responsabilidad solo debe agregarse al Gateway si es indispensable
  para adaptar ambos protocolos. La estabilidad asumida no debe convertirlo en un
  coordinador central ni en un punto donde se acumule lógica del sistema.

Este supuesto elimina del alcance la recuperación ante caída del proceso Gateway,
pero no elimina la necesidad de manejar errores de conexión con RabbitMQ ni de
garantizar exactly-once hacia un cliente conectado.

### Diagnóstico del sistema actual

| Área | Estado actual | Riesgo frente a una caída |
|---|---|---|
| RabbitMQ | Colas y exchanges no durables; mensajes no persistentes; sin volumen de datos | Reiniciar el broker pierde colas, mensajes y trabajo en curso |
| Middleware | Las conexiones se crean al arrancar y no se reconstruyen; heartbeat deshabilitado | La caída o partición del broker termina los workers o puede tardar en detectarse |
| Exchanges de control | Cada réplica usa una cola exclusiva y temporal | Una réplica caída puede perder un final de barrera y bloquear la ejecución al volver |
| InputBatcher | Confirma cada artículo después de guardarlo solamente en memoria | Una caída pierde artículos ya confirmados y el batch parcial |
| Barreras replicadas | Los conjuntos de particiones terminadas viven en memoria | Un reinicio olvida finales ya observados o vuelve a emitir finales |
| Agregadores y reducers | Conteos, palabras, parciales y promedios viven en memoria | Q3, Q4 y Q5 pierden progreso y pueden producir resultados incompletos o duplicados |
| Q5Comparator | Persiste candidatos en JSONL, pero promedio, finales y deduplicación viven en memoria | El archivo sobrevive, pero no alcanza para reconstruir el estado completo |
| Publicación y ACK | Se publica la salida y luego se confirma la entrada, sin transacción entre ambos pasos | Una caída entre ambas operaciones puede duplicar la salida al redeliverarse la entrada |
| Mensajes | No existe `message_id`, `result_id`, secuencia de entrada ni encadenamiento determinista | No se puede reconocer de forma general una reentrega |
| ResultSink | Barreras en memoria y una única instancia | Su caída pierde coordinación; una reentrega puede volver a enviar resultados o finales |
| Gateway | Instancia única, deliberadamente mínima; la cátedra permite asumir que no cae | No se diseña recuperación de su proceso; debe evitarse trasladar estado de negocio o coordinación hacia él |
| Cliente | Trunca los archivos al conectar y no guarda IDs confirmados | Una reconexión puede perder progreso o escribir duplicados |
| Ciclo de vida | Hay limpiezas locales al finalizar correctamente, pero no existe un protocolo global de aborto/cleanup | Una ejecución fallida puede dejar estado, archivos o colas indefinidamente |
| Control de fallos | No hay detectores, supervisores, consenso ni reinicio automático | Cualquier proceso caído queda fuera hasta una intervención manual |

### Arquitectura propuesta para validar con la cátedra

La propuesta separa el **plano de datos** del **plano de control de fallos**. El
primero conserva y recupera el trabajo; el segundo detecta caídas, acuerda una
decisión y repone procesos. Ningún supervisor decide a partir del estado de
Docker. Gateway queda fuera de ambos mecanismos de recuperación por el supuesto
acordado con la cátedra.

#### 1. Supervisores y protocolo de salud

- Desplegar tres `FailureManager` con membresía e identificadores configurados de
  forma estática.
- Cada proceso incluido en el modelo de fallos expone un endpoint de salud TCP
  pequeño y propio. La
  respuesta identifica componente, partición e `incarnation_id`, de modo que una
  respuesta vieja no se confunda con la nueva instancia de un proceso reiniciado.
- El endpoint responde saludable únicamente si el loop funcional y sus threads
  críticos —por ejemplo consumo DATA y control— siguen activos. No alcanza con
  que el proceso o el socket de health permanezcan abiertos.
- Los supervisores realizan pedidos activos `PING`/`PONG` con timeout. La falta de
  respuesta genera una **sospecha**, no un reinicio inmediato.
- Los tres supervisores también se controlan entre sí. Si el coordinador cae, los dos
  restantes pueden elegir otro; una minoría aislada no puede ordenar reinicios.
- El listado de procesos, endpoint de salud e identificador estático necesario
  para reiniciarlo proviene de configuración. No se obtiene con `docker ps`,
  `inspect`, eventos ni healthchecks de Docker.

#### 2. Bully, consenso y decisión de reinicio

- Implementar el algoritmo **Bully**, visto en clase, para elegir un coordinador
  entre los tres FailureManagers. Cada supervisor posee un ID total y estable.
- Si un supervisor deja de recibir mensajes del coordinador, envía `ELECTION` a
  los IDs superiores. Si ninguno responde se anuncia mediante `COORDINATOR`; si
  recibe `OK`, espera que uno de los superiores complete la elección.
- El coordinador mantiene su autoridad mediante mensajes entre supervisores. Cada
  elección recibe un `coordinator_epoch` creciente para rechazar anuncios o
  decisiones de coordinadores anteriores.
- Bully resuelve quién coordina, mientras que el acuerdo sobre el reinicio se
  realiza mediante votación. Ante una sospecha, el coordinador propone
  `RESTART(target_id, incarnation_id, coordinator_epoch, proposal_id)`.
- Cada FailureManager vuelve a comunicarse directamente con el target y vota
  `HEALTHY` o `FAILED`. Con tres supervisores, el reinicio se acepta únicamente
  con al menos dos votos `FAILED` para la misma propuesta.
- La decisión incluye el supervisor que debe ejecutarla. Si ese coordinador cae,
  Bully elige otro y se retoma o repite la propuesta sin depender de Docker para
  conocer el estado del target.
- Las elecciones y decisiones aceptadas se persisten antes de actuar.
  `proposal_id`, `coordinator_epoch` e `incarnation_id` vuelven idempotente la
  orden y evitan reiniciar una instancia nueva por una sospecha vieja.
- Recién después del acuerdo, el coordinador invoca una operación estática de
  reinicio mediante Docker Engine o Docker-in-Docker. Esa integración no consulta
  Docker para decidir disponibilidad ni para descubrir targets.
- Los timeouts usan relojes y eventos; no se utilizan sleeps para sincronizar la
  elección ni la votación.
- El alcance inicial propuesto tolera una caída a la vez y una mayoría disponible
  de supervisores. La cantidad de fallas simultáneas y el tratamiento de
  particiones de red deben confirmarse con el corrector.

#### 3. Transporte durable y reconexión

- Declarar durables las colas y exchanges funcionales, publicar mensajes
  persistentes y montar un volumen persistente para RabbitMQ.
- Reemplazar las colas exclusivas de control por colas durables con nombre estable
  por grupo y partición. Una réplica reiniciada debe recibir los controles que no
  alcanzó a procesar.
- Extender el middleware común para reconstruir conexión, canal, declaraciones y
  consumidor después de una desconexión, con backoff acotado y detención
  interrumpible.
- Rehabilitar detección de conexión caída sin reproducir el problema de publishers
  inactivos: la conexión debe atender eventos/heartbeats o recrearse de forma
  segura desde su thread propietario.
- Persistir los datos de RabbitMQ. Un reinicio del broker debe conservar las colas
  y permitir que los procesos se reconecten sin intervención manual.

#### 4. Identidad, idempotencia y recuperación local

- Agregar identidad determinista al protocolo interno:
  `message_id`, `causation_id`, `execution_id` y, cuando corresponda,
  `article_id`, número de secuencia o partición productora.
- Derivar los IDs de salida de la entrada y de la operación lógica, no de UUIDs
  generados al reintentar. La misma entrada redeliverada debe producir los mismos
  IDs.
- Implementar un `RecoveryStore` propio, sin biblioteca de tolerancia a fallos,
  con journal append-only, registros verificables, `fsync`, snapshots mediante
  reemplazo atómico y recuperación que ignore únicamente una última escritura
  incompleta.
- Guardar por `execution_id` el estado necesario de batches, deduplicación,
  barreras, agregaciones, reducers, promedio y finalización.
- Usar el patrón **inbox + estado + outbox**: registrar atómicamente que una entrada
  fue aplicada y qué salidas debe producir; publicar la outbox; recién entonces
  confirmar la entrada. Si hay una caída después de publicar, la salida puede
  repetirse físicamente, pero conserva su ID y el receptor la aplica una sola vez.
- Compactar journals mediante snapshots para que la deduplicación no requiera
  mantener indefinidamente todos los IDs en memoria.
- Montar almacenamiento persistente separado por componente y partición. No se
  comparte estado mutable entre dueños lógicos distintos.

Los componentes que hoy parecen stateless también poseen estado de coordinación
cuando participan de una barrera. Por eso Preprocessor, QueryRouter,
Until2020Filter, Q1, Q2BiographyFilter, ImageFetcher, WordExtractor, proyectores y
Q5DateRouter necesitan recuperar inbox, controles observados y finales emitidos,
aunque su transformación de DATA sea pura.

#### 5. Entrega exactly-once hacia el cliente

- Gateway conserva el rol mínimo actual: genera `execution_id`, adapta mensajes y
  reenvía resultados. No mantiene journal ni deduplicación durable.
- Cada `RESULT` y `QUERY_END` lleva `result_id` estable. El cliente persiste los IDs
  aplicados y escribe cada registro lógico una sola vez antes de responder ACK.
- Si se pierde el ACK, el servidor puede reenviar el mismo ID; el cliente lo
  reconoce, no vuelve a escribirlo y confirma nuevamente. La garantía observable
  es exactly-once en los archivos, construida sobre transporte at-least-once.
- ResultSink deduplica por `result_id` antes de publicar hacia la cola de respuesta
  y recupera qué resultados y finales ya emitió.
- La reanudación después de una caída del Gateway no forma parte del modelo. Si la
  cátedra exige reanudar clientes desconectados, se diseñará un protocolo separado
  sin trasladar estado de negocio al Gateway.
- Al recibir los cinco finales, el cliente envía una confirmación durable de
  ejecución completa. Solo entonces se habilita la limpieza del canal de salida.

#### 6. Ciclo de vida, fallos terminales y limpieza

- Incorporar mensajes explícitos `EXECUTION_FAILED`, `EXECUTION_COMPLETED` y
  `EXECUTION_CLEANUP`, con causa y `execution_id`.
- Definir un responsable durable del ciclo de vida que recupere ejecuciones activas
  después de reiniciar y emita cleanup exactamente una vez de forma idempotente.
- En éxito: esperar confirmación final del cliente, cerrar la cola de resultados y
  ordenar a cada componente eliminar inbox, outbox, snapshot, journal y buffers de
  esa ejecución.
- En fallo terminal o abandono: notificar al cliente si sigue conectado, detener
  nuevo trabajo, drenar o descartar de forma explícita los mensajes de la ejecución
  y ejecutar la misma limpieza.
- Conservar un tombstone mínimo durante una ventana acotada para que un mensaje
  tardío no recree estado ya eliminado. Luego eliminar también el tombstone.
- Automatizar una inspección final: no deben quedar archivos por ejecución, colas
  temporales, mensajes listos o sin confirmar ni estados activos en memoria.

### Herramienta Chaos Monkey

La herramienta será propia y separada de los supervisores:

- Recibe seed, intervalo, cantidad máxima de fallas y allowlist de targets.
- Elige targets desde configuración estática y registra instante, target, señal y
  seed para reproducir el escenario.
- Inyecta caídas abruptas (`SIGKILL` o stop equivalente) sin ejecutar cleanup del
  proceso víctima.
- Nunca mata Gateway. Tampoco mata cliente, infraestructura de prueba ni una
  mayoría de supervisores salvo en escenarios específicos acordados.
- Puede incluir workers stateless, procesos con barreras, stateful, ResultSink,
  un FailureManager y RabbitMQ en escenarios separados si estos últimos forman
  parte del alcance confirmado.
- Docker puede utilizarse aquí para **introducir** la falla; esta información no se
  comparte como detector de disponibilidad con los supervisores.

El escenario automatizado mínimo deberá:

1. Generar o verificar la referencia serial.
2. Levantar el sistema desde un estado limpio.
3. Iniciar un cliente con una muestra significativa.
4. Iniciar Chaos Monkey con seed conocida y fallas periódicas.
5. Esperar recuperación y finalización con timeout global.
6. Comparar Q1-Q5 contra la referencia, incluyendo duplicados.
7. Verificar que los procesos caídos fueron reiniciados por una decisión de mayoría.
8. Verificar que no quedaron recursos de la ejecución.
9. Guardar logs y resumen del escenario como evidencia.

### Plan de implementación

Cada bloque se trabaja en una rama acordada y se integra después de revisar código,
tests y documentación. No se debe empezar el bloque siguiente si la propiedad del
anterior todavía no puede demostrarse.

#### Bloque 0 - Acordar el modelo de fallos y congelar la línea base

- [ ] Confirmar con el corrector las decisiones abiertas listadas debajo.
- [ ] Etiquetar o registrar el commit exacto que representa el hito de escalabilidad.
- [ ] Automatizar una corrida base sin fallas con 50.000 artículos.
- [ ] Guardar tiempos, throughput, CPU, memoria, red y resultados de al menos tres corridas.
- [ ] Definir qué procesos, cantidad de caídas y fallas simultáneas entran en la defensa.

#### Bloque 1 - Contratos de identidad y pruebas de puntos de caída

- [ ] Diseñar `message_id`, `causation_id`, secuencias y `result_id` para cada tipo de mensaje.
- [ ] Documentar cuándo una entrada se considera aplicada y cuándo puede recibir ACK.
- [ ] Enumerar puntos de caída antes/después de persistir, publicar y confirmar.
- [ ] Agregar tests que reproduzcan redelivery y duplicación sin cambiar todavía la lógica funcional.

#### Bloque 2 - RecoveryStore, inbox y outbox

- [ ] Implementar y probar journal, checksum/versionado, `fsync`, snapshot y recuperación de escritura truncada.
- [ ] Implementar inbox deduplicada y outbox reintentable con IDs deterministas.
- [ ] Probar caídas en cada transición y compactación sin pérdida ni doble aplicación.
- [ ] Definir layout y volúmenes persistentes por componente/partición.

#### Bloque 3 - RabbitMQ durable y middleware recuperable

- [ ] Hacer durables colas/exchanges funcionales y persistentes los mensajes.
- [ ] Convertir el control a colas durables por réplica.
- [ ] Agregar volumen del broker.
- [ ] Implementar reconexión y redeclaración en el middleware común.
- [ ] Probar reinicio de RabbitMQ y pérdida temporal de conexión durante DATA y control.

#### Bloque 4 - Recuperar el pipeline por etapas

- [ ] InputBatcher: batch parcial, secuencias, inbox y outbox.
- [ ] Grupos coordinados: finales observados y finales ya emitidos.
- [ ] Q3WordAggregator: palabras, idiomas, resultados emitidos y barrera.
- [ ] Q4WordCounter y Q4Top20Reducer: conteos, parciales, top y finales.
- [ ] Q5StatsWorker y Q5AverageReducer: suma, cantidad, promedio, parciales y finales.
- [ ] Q5Comparator: candidatos, promedio, finales, resultados emitidos y buffer completo.
- [ ] ResultSink: deduplicación, barreras, resultados/finales y salida durable.
- [ ] Para cada etapa: matar antes y después de persistir/publicar/ACK, reiniciar y comparar salida.

#### Bloque 5 - Supervisión, consenso y reinicio

- [ ] Agregar endpoint de salud e `incarnation_id` a todos los procesos alcanzados.
- [ ] Hacer que la salud agregue el estado del loop principal y threads críticos,
  y probar una falla interna que deje vivo al proceso.
- [ ] Implementar protocolo entre FailureManagers y transporte sin bibliotecas de consenso.
- [ ] Implementar Bully con `ELECTION`, `OK`, `COORDINATOR`, prioridad estable,
  epoch y reemplazo del coordinador caído.
- [ ] Implementar propuesta de reinicio y voto por mayoría después de verificar el target por red.
- [ ] Implementar ejecutor Docker limitado a reinicios estáticos e idempotentes.
- [ ] Probar caída de un supervisor, coordinador, worker sano/reiniciado, sospecha
  falsa y mensajes viejos de otra elección.

#### Bloque 6 - Exactly-once en la frontera externa

- [ ] Incorporar `result_id` determinista a resultados y finales.
- [ ] Recuperar y deduplicar en ResultSink los IDs ya emitidos.
- [ ] Persistir en el cliente los `result_id` aplicados y escribir cada resultado
  de forma recuperable antes de enviar ACK.
- [ ] Mantener Gateway como relay sin RecoveryStore ni estado de negocio nuevo.
- [ ] Probar caída de cliente y ResultSink, duplicados internos y pérdida del ACK
  después de escribir. No inyectar caídas del Gateway.

#### Bloque 7 - Fallo terminal y cleanup distribuido

- [ ] Implementar protocolo de éxito, aborto, timeout y cleanup idempotente.
- [ ] Limpiar estado en memoria, archivos, volúmenes por ejecución y colas temporales.
- [ ] Evitar recreación por mensajes tardíos mediante tombstones acotados.
- [ ] Probar limpieza tanto de consultas resueltas como fallidas.

#### Bloque 8 - Chaos Monkey, benchmark, informe y demo

- [ ] Implementar Chaos Monkey reproducible y escenarios automatizados.
- [ ] Ejecutar matriz de caídas sobre componentes representativos y luego sobre todos los tipos.
- [ ] Comparar automáticamente resultados y recursos al terminar cada escenario.
- [ ] Repetir benchmark sin fallas con tolerancia habilitada y calcular overhead contra el bloque 0.
- [ ] Medir por separado el costo y tiempo de recuperación bajo fallas.
- [ ] Actualizar `INFORME.md` y diagramas de robustez, despliegue, actividades, paquetes, secuencia y DAG.
- [ ] Ensayar la demo completa con fallas periódicas y conservar logs de evidencia.

### Matriz mínima de pruebas de fallos

| Escenario | Invariante a comprobar |
|---|---|
| Caída antes de persistir una entrada | RabbitMQ redelivera y la entrada se aplica una vez |
| Falla de un thread crítico con proceso todavía vivo | El endpoint informa degradación y el proceso se repone |
| Caída después de persistir y antes de publicar | La outbox publica al recuperar |
| Caída después de publicar y antes del ACK | La reentrega conserva ID y no duplica el efecto downstream |
| Caída durante una barrera | Los finales observados sobreviven y se emite un solo final lógico |
| Caída de InputBatcher con batch parcial | Ningún artículo confirmado se pierde ni se duplica |
| Caída de cada stateful de Q3/Q4/Q5 | El estado reconstruido produce la misma salida que la referencia |
| Caída de ResultSink | No se pierde ni duplica un resultado lógico o `QUERY_END` |
| ACK del cliente perdido | El resultado puede reenviarse, pero se escribe una sola vez |
| Caída de RabbitMQ | Colas y mensajes sobreviven; los procesos se reconectan |
| Caída del coordinador | Bully elige otro y las propuestas anteriores no se ejecutan dos veces |
| Minoría de supervisores aislada | No puede ordenar reinicios |
| Ejecución fallida | Cliente informado y recursos eliminados |
| Chaos Monkey periódico | Cinco salidas iguales a referencia, sin duplicados ni recursos huérfanos |

### Comparación de performance

La comparación debe separar el overhead normal de la recuperación:

- **Baseline:** commit final de escalabilidad, sin fallas.
- **Tolerancia habilitada:** misma muestra, topología, host y configuración, sin fallas.
- **Con fallas:** misma configuración, seed y calendario de Chaos Monkey documentados.
- Ejecutar al menos tres corridas por caso y reportar mediana y dispersión, no una
  única medición.
- Medir tiempo total, artículos por segundo, tiempo hasta primer resultado, CPU,
  memoria, red, bytes escritos en journals y tiempo de recuperación.
- Verificar Q1-Q5 en todas las corridas; una ejecución rápida pero incorrecta no
  forma parte de la comparación.

### Decisiones que deben confirmarse con el corrector

1. Si la combinación de Bully para elegir coordinador y votación mayoritaria para
   acordar cada reinicio satisface el algoritmo de consenso exigido.
2. Si el modelo requerido es una caída a la vez o incluye varias caídas
   simultáneas y particiones de red.
3. Si RabbitMQ, cliente y los propios FailureManagers forman parte de los procesos
   que deben caer durante la defensa. Gateway ya está excluido por indicación de
   la cátedra.
4. Si una implementación propia de journal/snapshot con biblioteca estándar es el
   mecanismo esperado, y qué dependencias auxiliares —si alguna— se autorizan.
5. Si se acepta montar el socket de Docker en los FailureManagers siempre que solo
   ejecuten reinicios estáticos y jamás consulten disponibilidad o inventario.
6. Si la garantía exigida hacia el cliente se interpreta como exactly-once del
   efecto observable mediante IDs y deduplicación sobre transporte at-least-once.
7. Qué muestra se considera significativa y cuántas fallas periódicas debe incluir
   la demo.

### Definition of Done del hito

El hito no está terminado hasta que, desde un checkout limpio, un comando
documentado pueda levantar el sistema, ejecutar una muestra significativa con
fallas periódicas, recuperar automáticamente los procesos, obtener los cinco
archivos idénticos a la referencia sin duplicados, demostrar las decisiones de
consenso, verificar la limpieza de todas las ejecuciones y generar la evidencia
de performance. Además, `make check`, la suite unitaria, los escenarios de
recuperación y el informe 4+1/C4 deben estar actualizados y pasar.

### Concurrencia actual de clientes

El Gateway acepta conexiones concurrentes y ejecuta cada sesión en un thread independiente. Cada sesión crea su propio `execution_id`, sus conexiones al middleware y una cola exclusiva `results.<execution_id>`, por lo que el upload o la espera de resultados de un cliente no bloquean la aceptación de otro.

Si un cliente se desconecta durante el upload, Gateway publica `INPUT_END` para que el pipeline cierre la ejecución parcial y luego termina la sesión sin esperar resultados. Al cerrar su conexión se elimina la cola exclusiva. Los resultados tardíos se descartan de forma explícita en ResultSink porque su destino ya no existe. Si Q5 no recibió abstracts históricos, representa el promedio inexistente con `null`, no produce candidatos y emite igualmente su `QUERY_END`.

### Estado actual de Query 2

La referencia serial utiliza `image.content_url` cuando está presente en el registro del dataset. El sistema realiza un HTTP GET directo a esa URL y utiliza el cuerpo de la respuesta como los bytes de la imagen.

El criterio fue confirmado con la cátedra: no es necesario consultar adicionalmente la API REST de Wikimedia. La implementación distribuida utilizará directamente `image.content_url` cuando esté presente.

La descarga contará con timeout y una cantidad acotada de reintentos para errores transitorios. Un HTTP 404 se tratará como un error permanente: se registrará de forma identificable, no se reintentará y ese artículo se omitirá del resultado. Si el artículo no tiene `content_url` o cualquier otro error agota sus reintentos, también se registrará la omisión y la ejecución continuará.

## Flujo implementado actualmente

```text
Dataset Parquet
      |
      v
    Client
      |
      | ARTICLE / INPUT_END mediante TCP
      v
    Gateway
      |
      | RoutedArticle / RoutedInputEnd
      v
RabbitMQ: input_batcher.input.<partition>
      |
      | RoutedArticle / RoutedInputEnd
      v
 InputBatcher
      |
      | ArticleBatch / RoutedInputEnd
      v
RabbitMQ: preprocessor.input
      |
      | ArticleBatch / RoutedInputEnd
      v
 Preprocessor
      |
      | NormalizedArticleBatch / RoutedInputEnd
      v
RabbitMQ: query_router.input
      |
      | NormalizedArticleBatch / RoutedInputEnd
      v
 QueryRouter
      |
      |-------------------------------|
      v                               v
RabbitMQ: until_2020.input     RabbitMQ: word_extractor.input
      |                               |
      | Q1Q2ArticleBatch              | TextArticleBatch
      v                               v
 Until2020Filter                WordExtractor (ver rama textual debajo)
      |
      |-------------------------------|
      v                               v
q1_english_filter.input   q2_biography_filter.input
      |                               |
      | Q1CandidateBatch              | Q2CandidateBatch
      v                               v
      Q1                         Q2BiographyFilter
      |                               |
      | RESULT / QUERY_PARTITION_END  | ImageCandidateBatch
      |                               v
      |                       RabbitMQ: image_fetcher.input
      |                               |
      |                               v
      |                          ImageFetcher
      |                               |
      |       RESULT / QUERY_PARTITION_END
      |_______________________________|
                      |
                      v
          RabbitMQ: result_sink.input
                      |
                      v
              ResultSink

RabbitMQ: results.<execution_id>
      |
      | RESULT / QUERY_END
      v
    Gateway
      |
      v
    Client
      |
      v
query1.jsonl ... query5.jsonl
```

El `InputBatcher` consume los artículos, construye batches de hasta `MAX_BATCH_BYTES` y los publica en `preprocessor.input` antes del mensaje de fin. El `Preprocessor` valida y normaliza esos artículos y publica los batches resultantes en `query_router.input`. El `QueryRouter` proyecta cada batch hacia `until_2020.input` y `word_extractor.input`. `Until2020Filter` consume la primera rama, conserva los artículos hasta 2020 inclusive y publica las proyecciones de Q1 y Q2. Q1 conserva los artículos en inglés y Q2 filtra las secciones biográficas, descarga las imágenes disponibles y codifica sus bytes en Base64. Las cinco consultas publican resultados y finales en `result_sink.input`. La rama textual implementada se detalla a continuación.

La rama textual implementada continúa desde `word_extractor.input`:

```text
WordExtractor ×3
    | TokenizedArticleBatchMessage
    |-------------------------------------|
    v                                     v
q3_word_projector.input         q4_q5_word_projector.input
    |                                     |
Q3WordProjector                    Q4Q5WordProjector
    |                                     |-------------------------------|
    | Q3WordPresenceBatchMessage          | Q4WordBatchMessage            | Q5ArticleWordCountBatchMessage
    v                                     v                               v
q3_word_aggregator.input.<p>      q4_word_counter.input.<p>        q5_date_router.input
    |                                     |                               |
Q3WordAggregator ×3                 Q4WordCounter ×3                 Q5DateRouter ×3
    | RESULT / QUERY_PARTITION_END         |                    |--------------------|
    v                                      v                    v                    v
result_sink.input                  Q4Top20Reducer ×3       Q5StatsWorker ×3    Q5Comparator ×3
                                           |                    |                    ^
                                           v                    v                    |
                                   result_sink.input       Q5AverageReducer ×3 -----|
                                                                                |
                                                                                v
                                                                        result_sink.input
```

Tres réplicas de `Q4Q5WordProjector` consumen la salida tokenizada y publican las
proyecciones específicas de Q4 y Q5. Q4 particiona palabras entre tres counters;
los tops parciales de una ejecución convergen en uno de tres reducers físicos.
Q5DateRouter separa histórico y candidatos; StatsWorker, AverageReducer y
Comparator utilizan tres particiones o réplicas con routing estable. Comparator
conserva en disco los candidatos que llegan antes del promedio y publica los
resultados de Q5 en `result_sink.input`.

## Arquitectura objetivo

El pipeline acordado es:

```text
Client
  -> Gateway
  -> InputBatcher
  -> Preprocessor
  -> QueryRouter
       -> rama Q1/Q2
       -> rama textual Q3/Q4/Q5
  -> ResultSink
  -> Client
```

Las etapas sin estado utilizan colas compartidas con consumidores competidores. Las etapas con estado se particionan mediante una función de hash estable para que los mensajes relacionados lleguen siempre a la misma partición lógica.

### Identidad y aislamiento

Cada ejecución recibe un identificador globalmente único.

Todos los mensajes internos de una ejecución incluyen este identificador. Esto permite:

- Mantener separados los datos de distintos clientes.
- Enviar todos los artículos y el fin de entrada a la misma partición.
- Mantener estado independiente en agregadores y reductores.
- Enrutar cada resultado hacia la sesión correcta.
- Limpiar los recursos temporales al finalizar.

### Gateway

El Gateway:

- Acepta conexiones concurrentes y crea una sesión independiente por cliente.
- Genera un `execution_id`.
- Recibe artículos como stream.
- Adjunta el identificador de ejecución.
- Elige una partición estable de InputBatcher.
- Publica todos los artículos y `INPUT_END` en esa misma partición.
- Recibe resultados progresivamente.
- Los envía al cliente y espera su ACK antes de confirmarlos en RabbitMQ.

El particionado se calcula como:

```text
partition = stable_hash(execution_id) mod INPUT_BATCHER_PARTITIONS
```

No se utiliza `hash()` de Python porque su resultado puede cambiar entre procesos. La implementación utiliza SHA-256 para obtener un valor estable.

### InputBatcher

El InputBatcher construye batches con un límite configurable en bytes:

```text
MAX_BATCH_BYTES
```

Cuando llega un artículo:

1. Calcula su tamaño serializado dentro del batch.
2. Lo agrega si el batch resultante no supera el límite.
3. Si lo supera, publica el batch actual y comienza uno nuevo.
4. Al recibir `INPUT_END`, publica el último batch parcial.
5. Después publica el mensaje de fin en la misma cola de salida que los batches.

InputBatcher se escala mediante particiones: todos los mensajes de una ejecución se asignan a una única cola `input_batcher.input.<partition>`, con un consumidor activo por partición lógica. Compose despliega actualmente las particiones `0`, `1` y `2`, y Gateway utiliza SHA-256 sobre `execution_id` para elegir una de ellas. Así, distintas ejecuciones pueden construir sus batches en paralelo sin repartir el estado incompleto de una misma ejecución entre procesos.

No se deben colocar consumidores competidores sobre una misma partición porque dividirían entre procesos el batch pendiente de una ejecución. Las tres particiones publican en la cola compartida `preprocessor.input`. El final de cada ejecución proviene de una sola partición y el protocolo de control de Preprocessor lo difunde después a sus tres réplicas.

#### Límites y entradas sobredimensionadas

Un artículo que por sí solo supere `MAX_BATCH_BYTES` no se enviará como un batch excepcionalmente grande: será rechazado para preservar el límite configurado. La primera implementación fallará de forma visible ante este caso. Queda pendiente completar un mensaje de error de ejecución que llegue hasta el cliente, junto con el NACK sin requeue del artículo inválido, para evitar tanto una espera indefinida como un ciclo de reentregas.

El protocolo TCP ya rechaza frames individuales mayores que `MAX_MESSAGE_SIZE`, actualmente 64 MiB. Este límite acota un mensaje aislado, pero no evita que un cliente envíe repetidamente mensajes cercanos al máximo. Ese escenario constituye una posible denegación de servicio por agotamiento de memoria, CPU, red o almacenamiento de RabbitMQ.

Durante el hito de escalabilidad se trabaja con el dataset conocido y se prioriza el pipeline completo. Antes de aceptar entradas no confiables se deberán incorporar límites específicos por tipo de mensaje, una cuota total de bytes o artículos por ejecución, timeouts, control de frecuencia, validación temprana del esquema y propagación explícita de errores al cliente. El codec binario externo ya valida los tamaños de los frames y detecta valores truncados, pero estos controles no reemplazan las cuotas por ejecución.

### Preprocessor

El Preprocessor es una etapa sin estado encargada de:

- Validar la estructura de cada artículo.
- Normalizar fecha e idioma.
- Preparar la representación básica del abstract.
- Construir un artículo normalizado para las siguientes etapas.

Recibe `ArticleBatchMessage`, publica `NormalizedArticleBatchMessage` y no aplica reglas específicas de ninguna query.

La implementación actual despliega tres réplicas como consumidores competidores de `preprocessor.input`. La réplica que recibe `INPUT_END` lo difunde mediante `preprocessor.control`; cada instancia pausa ordenadamente su consumidor, termina el batch en vuelo y publica un `PARTITION_END` con su identificador. La pausa es temporal: después de serializar el control con su salida, la réplica continúa atendiendo otras ejecuciones.

### QueryRouter

El QueryRouter realiza el fan-out hacia dos ramas:

```text
Q1/Q2
Q3/Q4/Q5
```

Publica solamente los campos requeridos por cada rama y no realiza filtros propios de las consultas.

`until_2020.input` recibe `Q1Q2ArticleBatchMessage`. Cada artículo contiene `name`, `url`, `language`, `modification_date`, `sections` e `image_url`.

`word_extractor.input` recibe `TextArticleBatchMessage`. Cada artículo contiene `article_id`, `url`, `language`, `modification_date` y `abstract`. Mientras el dataset no provea un identificador independiente, `article_id` utiliza la URL como identidad estable.

La implementación despliega tres QueryRouters como consumidores competidores. La instancia que recibe un `PARTITION_END` de Preprocessor lo difunde mediante `query_router.control`; cada réplica mantiene por `execution_id` el conjunto de Preprocessors finalizados. Después de completar `{0, 1, 2}`, cada QueryRouter publica su propio `PARTITION_END(stage="query_router")` en ambas ramas. Cada rama es consumida por tres réplicas: Until2020Filter en Q1/Q2 y WordExtractor en Q3/Q4/Q5.

El fan-out tiene semántica at-least-once. El mensaje de entrada se confirma solamente después de publicar ambas salidas. Si la primera publicación funciona y la segunda falla, se rechaza la entrada y una reentrega puede duplicar el mensaje en la primera rama. La idempotencia de las etapas posteriores es una tarea explícita del hito actual de tolerancia a fallos.

### Rama Q1/Q2

```text
QueryRouter
    |
Until2020Filter ×3
    |-------------------|
    v                   v
Q1EnglishFilter ×3   Q2BiographyFilter ×3
                        |
                        v
                   ImageFetcher ×3
```

`Until2020Filter` aplica una sola vez la condición `fecha <= 2020`.

Consume `Q1Q2ArticleBatchMessage` desde `until_2020.input`. Por cada batch que contenga artículos hasta 2020 publica:

- `Q1CandidateBatchMessage` en `q1_english_filter.input`, con `url`, `language` y `modification_date`.
- `Q2CandidateBatchMessage` en `q2_biography_filter.input`, con `name`, `url`, `sections` e `image_url`.

Los batches vacíos no se publican. Las tres réplicas compiten por `until_2020.input`. La que recibe un final de QueryRouter lo difunde mediante `until_2020.control`; después de observar los tres IDs de QueryRouter, cada réplica publica su propio `PARTITION_END(stage="until_2020_filter")` hacia Q1 y Q2. Las tres réplicas de Q1 coordinan esos finales mediante `q1.control` y las tres de Q2BiographyFilter hacen lo mismo mediante `q2_biography_filter.control`. Una segunda ejecución real confirmó que las réplicas reanudan el consumo después de la barrera.

- Q1 consume `Q1CandidateBatchMessage`, conserva los artículos cuyo idioma normalizado es `en` y publica un `ResultMessage` por artículo en `result_sink.input`. El registro contiene `url` y `modification_date` en el mismo formato ISO que la referencia serial.
- `Q2BiographyFilter` consume `Q2CandidateBatchMessage`, busca una sección cuyo nombre completo sea `biography` o `biographie` ignorando mayúsculas y publica `ImageCandidateBatchMessage` en `image_fetcher.input`. La proyección contiene `name`, `url` e `image_url`; una URL ausente se conserva para que ImageFetcher registre y omita el candidato de forma explícita.
- `ImageFetcher` utiliza `image.content_url` directamente, descarga con timeout y reintentos acotados para 429, 5xx y errores transitorios de red. Los 404 y demás 4xx se registran y no se reintentan. Si falta la URL o la descarga falla definitivamente, registra la omisión y continúa. Los bytes exitosos se codifican en Base64 y se publican como `ResultMessage(query_id=2)` en `result_sink.input`.

Las tres instancias de Q1 compiten por los batches de candidatos. La que recibe un final de Until2020Filter lo difunde mediante `q1.control`; cuando cada réplica observó los tres IDs de origen, emite `QueryPartitionEndMessage(query_id=1)` con su propio `partition_id`. ResultSink espera las tres particiones y transforma sus cierres en un único `QUERY_END`. Q2BiographyFilter utiliza el mismo patrón y cada una de sus tres réplicas publica un `PARTITION_END(stage="q2_biography_filter")`. Las tres instancias de ImageFetcher difunden esos finales mediante `image_fetcher.control`; después de observar los tres IDs biográficos, cada una emite su propio `QueryPartitionEndMessage(query_id=2)`. ResultSink espera las tres particiones de Q2. Cada instancia descarga secuencialmente, por lo que la cantidad de réplicas establece un límite explícito de tres batches de descarga simultáneos.

### Rama textual Q3/Q4/Q5

```text
QueryRouter
    |
WordExtractor
    |--------------------------|
    v                          v
Q3WordProjector ×3     Q4Q5WordProjector ×3
    |                   |             |
    v                   v             v
Q3WordAggregator ×3 Q4WordCounter ×3 Q5DateRouter ×3
                          |          |         |
                          v          v         v
                   Q4Top20Reducer ×3 Stats ×3 Comparator ×3
```

`WordExtractor` está implementado y reutiliza `common.text.extract_words`:
convierte a minúsculas, tokeniza y descarta tokens exclusivamente numéricos.
No filtra fechas ni idiomas y conserva repeticiones y stopwords. Transforma
`TextArticleBatchMessage` en `TokenizedArticleBatchMessage`, cuyos artículos
contienen `article_id`, `url`, `language`, `modification_date`, `words` y
`has_abstract`. Este último campo permite distinguir un abstract ausente de uno
presente pero vacío.
Los metadatos `execution_id` y `reply_queue` se conservan en el batch.
Publica el mismo batch en las colas independientes de Q3 y Q4/Q5. Las tres
réplicas coordinan los finales de QueryRouter mediante `word_extractor.control`;
cada una emite su propio `PARTITION_END` hacia ambas ramas.

#### Query 3

`Q3WordProjector` conserva solamente artículos `en` y `fr`, deduplica las
palabras dentro de cada abstract y conserva stopwords. Publica batches
`Q3WordPresenceBatchMessage` con `presences` de la forma `{word, language}`,
además de `execution_id` y `reply_queue`. Una presencia puede repetirse entre
artículos o batches.

Tres Q3WordProjectors compiten por los batches y coordinan los finales de WordExtractor. Cada presencia se enruta por `(execution_id, word)` hacia una de tres particiones de Q3WordAggregator. Cada aggregator mantiene, por palabra y ejecución, si fue observada en inglés y en francés; cuando ambas condiciones se cumplen, emite esa palabra una sola vez. Después de recibir los finales de los tres proyectores, cada aggregator publica su `QUERY_PARTITION_END`; ResultSink completa la barrera de Q3.

La afinidad se calcula a partir de:

```text
(execution_id, word)
```


#### Query 4

`Q4Q5WordProjector` está implementado. Omite los artículos cuyo abstract era
`None`, elimina stopwords y repeticiones dentro del mismo abstract y publica un
`Q4WordBatchMessage` con una aparición por palabra y artículo. Cada palabra se
enruta por `(execution_id, word)` hacia `q4_word_counter.input.<partition>`. Las
repeticiones entre artículos se conservan porque
representan apariciones distintas.

Tres Q4WordCounters cuentan subconjuntos disjuntos de palabras. Después de que
cada uno observa los finales de los tres projectors, publica su top 20 local y
un `Q4PartitionEndMessage`. Los parciales de una ejecución se enrutan por
`execution_id` hacia uno de tres Q4Top20Reducers físicos. Ese reducer lógico
espera los finales de los tres counters, fusiona los tops y publica los pares
`{word, count}` seguidos del único `QUERY_END` de Q4.

Enviar solamente veinte candidatos por partición es suficiente
porque una palabra fuera del top local ya tiene veinte palabras de su partición
que la superan o empatan según el desempate alfabético.

#### Query 5

El mismo `Q4Q5WordProjector` publica en `q5_date_router.input` un
`Q5ArticleWordCountBatchMessage` con `article_id`, URL, fecha y `word_count`.
Los artículos sin abstract se omiten; un abstract presente pero vacío se
conserva con conteo cero, de acuerdo con la referencia serial.

Tres réplicas de `Q5DateRouter` consumen
`Q5ArticleWordCountBatchMessage`, no publica batches vacíos y separa:

- Conteos de artículos hasta 2020 en `q5_stats_worker.input`, mediante
  `Q5HistoricalWordCountBatchMessage`.
- `article_id`, URL y conteo de artículos desde 2021 en
  `q5_comparator.input`, mediante `Q5CandidateBatchMessage`.

Los DateRouters coordinan los finales de los tres projectors. Cada réplica emite
su propio `PARTITION_END` a todas las particiones de ambas ramas.

`Q5StatsWorker` acumula por ejecución la suma de los conteos históricos y la
cantidad de abstracts. Al recibir el final publica `Q5PartialStatsMessage` y
`Q5StatsPartitionEndMessage`. Los históricos de una ejecución se enrutan juntos
hacia uno de tres workers; cada worker espera los finales de los tres DateRouters.

Los parciales de una ejecución convergen por `execution_id` en uno de tres
Q5AverageReducers. Ese reducer espera los finales de los tres StatsWorkers,
calcula `total_word_count / abstract_count` y difunde `Q5AverageMessage` a las
tres colas de comparadores.

`Q5Comparator` acepta tanto candidatos antes del promedio como el orden
inverso. Antes de conocerlo persiste cada candidato en un archivo JSONL
append-only separado por `execution_id`; el nombre se deriva con SHA-256 para
que datos de un mensaje no puedan formar una ruta. Los archivos residen en el
volumen Docker `q5_candidate_buffer`. Después del promedio drena el archivo
línea por línea, compara directamente los candidatos posteriores y emite solo
aquellos cuyo conteo sea estrictamente mayor.

Los candidatos se particionan por `(execution_id, article_id)` entre tres
comparadores, cada uno con un subdirectorio de buffer independiente. Q5 finaliza
únicamente después de conocer el promedio, recibir los finales de los tres
DateRouters y procesar el buffer. Entonces cada comparator publica su
`QUERY_PARTITION_END`; ResultSink produce un único `QUERY_END`. RabbitMQ se utiliza como
transporte, no como almacenamiento prolongado de candidatos.

El volumen limita el uso de memoria y conserva los archivos al recrear el
container, pero todavía no implementa recuperación completa: el promedio, la
señal de cierre y la idempotencia frente a publicaciones parciales se incorporan
en el plan del hito actual de tolerancia a fallos.

### ResultSink

El servicio `result-sink` consume `result_sink.input` y reenvía cada `ResultMessage`
inmediatamente a `results.<execution_id>`, sin acumular registros ni esperar al
final de una consulta. También mantiene una barrera genérica por
`(execution_id, query_id)` para los `QueryPartitionEndMessage`: espera los IDs
distintos configurados para esa query y recién entonces publica un único
`QueryEndMessage`. Q4 entrega un `QueryEndMessage` directo porque todos los
parciales de una ejecución convergen en un único reducer lógico. La publicación usa confirmaciones de
RabbitMQ y enrutamiento obligatorio antes de confirmar el mensaje de entrada.
No declara las colas de respuesta: son exclusivas y pertenecen al Gateway.
Si una sesión ya cerró y la cola fue eliminada, RabbitMQ informa que la
publicación obligatoria no tiene destino. ResultSink registra el descarte y
confirma el mensaje de entrada para que una ejecución abandonada no bloquee las
demás.

Esta versión utiliza una sola instancia de ResultSink, por lo que todos los finales
de una ejecución convergen en el mismo estado de barrera. Particionar ResultSink y
persistir la deduplicación quedan pendientes.

Gateway y Client comienzan la recepción de resultados después de finalizar la
subida. Desde ese momento, los resultados se entregan incrementalmente sin
esperar las demás consultas. El cliente termina después de recibir los cinco
finales.

## Mensajes de finalización

Los finales tienen significados diferentes:

- `INPUT_END`: el cliente terminó de enviar artículos.
- `PARTITION_END`: una partición terminó una etapa determinada.
- `QUERY_PARTITION_END`: una partición terminó de producir resultados de una query.
- `QUERY_END`: una consulta completa terminó y no producirá más resultados.
- `QUERY_FAILED`: una consulta o ejecución no puede completarse correctamente.

`INPUT_END` no significa que las consultas hayan terminado.

La primera versión de punta a punta utilizó una sola réplica por etapa. El último batch y el mensaje de fin viajaban por la misma cola, por lo que el consumidor único procesaba primero todos los batches y luego el final.

Antes de habilitar consumidores competidores en una etapa se implementará el siguiente protocolo, basado en el patrón utilizado en el TP de Coordinación:

1. El mensaje de fin se publica después de todos los mensajes de datos en la misma cola compartida.
2. Una sola réplica recibe ese mensaje desde la cola y lo difunde mediante un exchange de control.
3. Cada réplica recibe una copia del control, termina cualquier mensaje que tenga en procesamiento y publica exactamente un `PARTITION_END`.
4. La etapa siguiente mantiene una barrera por `execution_id` y espera un `PARTITION_END` de cada partición o réplica configurada antes de considerar completa la entrada.

El `prefetch_count=1` limita a uno los mensajes en vuelo por réplica cuando se recibe el control. En Preprocessor, el hilo de control coloca el final en una cola local segura y pausa el consumidor; el hilo principal publica luego `PARTITION_END` por la misma conexión que los datos y reanuda el consumo. Esto conserva el orden por réplica y permite procesar varias ejecuciones.

No se debe asumir orden entre una cola de datos y una cola o exchange de control diferentes. Por eso el primer mensaje de fin atraviesa la misma cola que los datos antes de ser difundido. `MessageMiddlewareExchangeRabbitMQ` implementa el exchange `direct`, las colas exclusivas por consumidor y el modo publicador sin cola. El patrón está integrado en los grupos de tres Preprocessors, QueryRouters, Until2020Filters, Q1EnglishFilters, Q2BiographyFilters, ImageFetchers, WordExtractors, Q3WordProjectors, Q4Q5WordProjectors y Q5DateRouters. Las etapas stateful posteriores completan barreras sobre los finales de esas réplicas.

## Comunicación

### Cliente–Gateway

El protocolo externo utiliza sockets TCP.

Cada frame contiene:

```text
4 bytes de longitud en orden de red
cuerpo binario del mensaje
```

El receptor continúa leyendo hasta obtener exactamente la cantidad de bytes indicada por el encabezado. Esto permite manejar fragmentación de TCP correctamente.

El cuerpo utiliza un protocolo binario propio. Su primer byte identifica la clase
concreta del mensaje externo y los campos siguientes se codifican mediante un
codec manual para valores simples y estructuras anidadas. El formato incluye:

- Un byte inicial para identificar la clase concreta del mensaje.
- Un codec propio para `None`, booleanos, enteros, floats, strings, bytes, listas y diccionarios.
- Strings y bloques de bytes precedidos por una longitud de cuatro bytes en orden de red.
- Enteros representados con signo en ocho bytes y en orden de red.
- Claves de diccionarios restringidas a strings.
- Rechazo de códigos desconocidos, mensajes truncados, UTF-8 inválido, tipos no soportados y bytes sobrantes.

El cliente lee el Parquet incrementalmente mediante
`ParquetFile.iter_batches()`, recorre sus registros de a uno y envía cada
`ARTICLE` con este protocolo. El Gateway lo decodifica, agrega `execution_id` y
`reply_queue`, construye el mensaje interno y recién entonces lo serializa como
JSON UTF-8 para publicarlo en RabbitMQ. De esta manera, el cliente no conoce el
middleware ni construye el JSON interno del sistema distribuido.

Los resultados atraviesan el mismo protocolo binario en el camino de regreso. La
imagen de Query 2 continúa representada como Base64 dentro del `ResultMessage`
interno y de `query2.jsonl`, porque RabbitMQ aún transporta esos mensajes como
JSON. El codec externo admite bytes, pero cambiar únicamente ese último tramo no
eliminaría el Base64 usado previamente dentro del pipeline.

Los principales mensajes externos son:

- `ARTICLE`
- `INPUT_END`
- `ACK`
- `RESULT`
- `QUERY_END`
- `ERROR`

### Comunicación interna

RabbitMQ está abstraído detrás de `MessageMiddleware`.

La implementación actual incluye:

- `MessageMiddlewareQueue`
- `MessageMiddlewareQueueRabbitMQ`
- `MessageMiddlewareExchangeRabbitMQ`
- ACK y NACK manuales.
- `prefetch_count=1`.
- Inicio y detención del consumo.
- Cierre explícito de conexiones.
- Heartbeats AMQP deshabilitados en las conexiones bloqueantes actuales.

Los componentes de negocio no utilizan Pika directamente.

Cada módulo de mensajería mantiene conexiones bloqueantes separadas para entrada
y salida. Pika solo procesa heartbeats de una `BlockingConnection` cuando su
loop recibe eventos; una conexión publicadora inactiva puede permanecer sin ese
loop durante toda una agregación. En una prueba con 50.000 artículos RabbitMQ
cerró esas conexiones después de 60 segundos y los agregadores no pudieron
publicar al finalizar. Por eso la configuración actual utiliza `heartbeat=0`.
La detección rápida de conexiones zombis, la reconexión y la idempotencia ante
una publicación ambigua forman parte del hito actual de tolerancia a fallos.

`MessageMiddlewareExchangeRabbitMQ` adapta la implementación del TP de Coordinación. Utiliza un exchange `direct`: cada réplica consumidora declara una cola exclusiva y la vincula con las routing keys del grupo, mientras una instancia publicadora puede omitir la creación de cola. Varias colas vinculadas con la misma clave reciben una copia de cada control. Las conexiones de publicación y consumo permanecen separadas porque `BlockingConnection.start_consuming()` bloquea su thread.

### ACK de resultados

El Gateway no confirma inmediatamente un resultado recibido de RabbitMQ.

El orden es:

```text
RabbitMQ entrega resultado
        |
Gateway lo valida
        |
Gateway lo envía al cliente
        |
Cliente responde ACK
        |
Gateway ejecuta ACK en RabbitMQ
```

Si el resultado es inválido, Gateway ejecuta NACK y detiene el consumo de esa sesión. Si el cliente se desconecta durante la fase de resultados, Gateway deja de enviarlos por TCP pero continúa drenando y confirmando esa cola hasta recibir los cinco finales, tras lo cual libera sus recursos.

## Durabilidad actual

Actualmente las colas se declaran con:

```text
durable = false
```

y los mensajes no se publican como persistentes.

Este es el punto de partida heredado del hito de escalabilidad y debe cambiar en
el hito actual de tolerancia a fallos.

Para que los mensajes sobrevivan a un reinicio de RabbitMQ no alcanza con declarar la cola como durable. También se necesitan mensajes persistentes, almacenamiento persistente de RabbitMQ y una estrategia de recuperación e idempotencia.

La estrategia concreta se encuentra en [Hito 3: tolerancia a fallos](#hito-3-tolerancia-a-fallos):
colas y exchanges durables, mensajes persistentes, volumen del broker,
reconexión, estado recuperable e idempotencia de extremo a extremo.

Las colas `results.<execution_id>` son deliberadamente temporales:

```text
exclusive = true
auto_delete = true
```

Se eliminan cuando termina la conexión asociada con esa ejecución.

## Estructura del repositorio

```text
src/
  client/                  Cliente TCP y escritura de resultados
  common/
    message_protocol/      Mensajes y framing externo
    middleware/            Abstracción de RabbitMQ
    article.py             Modelo de artículo
    partition_router.py    Particionado estable
    query_results.py       Resultados de referencia
    text.py                Normalización textual
    writer.py              Escritura JSONL
  dataset/                 Lectura incremental de Parquet
  gateway/                 Servidor, handler y mensajería del Gateway
  infra/                   Acceso HTTP a Wikimedia
  reference/               Ejecución serial de Q1–Q5

test/                      Tests unitarios y de integración local
datasets/                  Dataset local, no versionado
output/
  reference/               Resultados de la ejecución serial
  distributed/             Resultados del sistema distribuido
```

## Requisitos

- Python.
- `uv`.
- Docker y Docker Compose.
- Acceso a Internet para descargar imágenes de Query 2.
- Dataset Parquet generado a partir de Wikipedia Structured Contents.

## Preparación del entorno

Crear el entorno e instalar las dependencias:

```sh
make setup
```

Este comando:

1. Crea `.venv` si no existe.
2. Instala las dependencias de desarrollo.
3. Descarga las stopwords de NLTK.

Las dependencias principales son:

- `pyarrow`
- `nltk`
- `pika`

## Dataset de muestra

El dataset no se versiona en Git.

La muestra utilizada durante el desarrollo contiene 50.000 artículos:

- 25.000 en inglés.
- 25.000 en francés.

Debe guardarse como:

```text
datasets/wikipedia_sample.parquet
```

Notebook utilizado para generar la muestra:

<https://www.kaggle.com/code/ignaciocarrera1/wikipedia>

## Ejecución serial de referencia

Ejecutar las cinco queries de manera serial:

```sh
make reference
```

Por defecto se utiliza:

```text
datasets/wikipedia_sample.parquet
```

y los resultados se escriben en:

```text
output/reference/
```

Se puede cambiar la entrada y salida:

```sh
make reference \
  DATASET=/ruta/dataset.parquet \
  REFERENCE_OUTPUT=/ruta/resultados
```

La ejecución genera:

```text
query1.jsonl
query2.jsonl
query3.jsonl
query4.jsonl
query5.jsonl
```

## Tests y formato

Ejecutar todos los tests:

```sh
make test
```

Verificar lint y formato:

```sh
make check
```

Aplicar automáticamente las correcciones de Ruff:

```sh
make lint-fix
```

## Comparación de resultados

Cuando el pipeline distribuido esté completo, sus resultados podrán compararse automáticamente con la referencia serial:

```sh
make compare
```

Por defecto se comparan:

```text
output/reference/
output/distributed/
```

La comparación:

- Ignora el orden de las líneas.
- Ignora el orden de las claves JSON.
- Conserva y detecta duplicados.
- Informa registros faltantes y adicionales.
- Devuelve un código de error si existen diferencias.

Se pueden indicar otros directorios:

```sh
make compare \
  REFERENCE_OUTPUT=/ruta/esperados \
  DISTRIBUTED_OUTPUT=/ruta/obtenidos
```

## Ejecución actual con Docker

Levantar RabbitMQ y Gateway:

```sh
make up
```

`make up` construye los containers, los inicia y muestra sus logs.

En otra terminal, ejecutar el cliente:

```sh
docker compose --profile client run --build --rm client
```

El cliente envía todos los artículos, recibe resultados incrementalmente y
termina después de los cinco `QUERY_END`.

### Prueba manual temporal

Obtener el `execution_id` desde los logs del Gateway:

```text
Execution <execution_id>: starting upload
```

Definirlo en la terminal:

```sh
EXECUTION_ID='REEMPLAZAR_POR_EL_ID_DEL_LOG'
```

Publicar un resultado ficticio para Query 3:

```sh
docker compose exec -T rabbitmq rabbitmqadmin -u guest -p guest publish \
  routing_key="results.$EXECUTION_ID" \
  payload="{\"type\":\"RESULT\",\"execution_id\":\"$EXECUTION_ID\",\"query_id\":3,\"record\":{\"word\":\"network\"}}"
```

Marcar las cinco consultas como terminadas:

```sh
for query_id in 1 2 3 4 5; do
  docker compose exec -T rabbitmq rabbitmqadmin -u guest -p guest publish \
    routing_key="results.$EXECUTION_ID" \
    payload="{\"type\":\"QUERY_END\",\"execution_id\":\"$EXECUTION_ID\",\"query_id\":$query_id}"
done
```

El cliente debe:

- Guardar `{"word":"network"}` en `query3.jsonl`.
- Crear archivos vacíos para las otras cuatro consultas.
- Confirmar cada resultado.
- Finalizar después de recibir los cinco `QUERY_END`.

El QueryRouter publica las proyecciones textuales en `word_extractor.input`, consumida por las ramas Q3, Q4 y Q5. `Until2020Filter` alimenta los filtros de Q1 y Q2. ResultSink reenvía los resultados de Q1-Q5 a `results.<execution_id>`.

Detener el entorno:

```sh
make down
```

## Configuración actual

| Variable | Componente | Valor predeterminado | Descripción |
|---|---|---:|---|
| `SERVER_HOST` | Gateway | `0.0.0.0` | Dirección en la que escucha |
| `SERVER_PORT` | Gateway/Client | `5000` | Puerto TCP |
| `GATEWAY_HOST_PORT` | Docker Compose | `5001` | Puerto del host publicado hacia el puerto 5000 del Gateway; evita el puerto 5000 que macOS puede reservar para AirPlay |
| `MOM_HOST` | Todos los componentes conectados a RabbitMQ | `localhost` | Host de RabbitMQ |
| `INPUT_BATCHER_QUEUE_PREFIX` | Gateway/InputBatcher | `input_batcher.input` | Prefijo de las colas de entrada |
| `INPUT_BATCHER_PARTITIONS` | Gateway | `1` en Python; `3` en Compose | Cantidad de particiones de InputBatcher; debe coincidir con las instancias declaradas |
| `INPUT_BATCHER_PARTITION_ID` | InputBatcher | `0` | Partición consumida por la instancia |
| `OUTPUT_QUEUE` | InputBatcher | `preprocessor.input` | Cola de salida hacia Preprocessor |
| `MAX_BATCH_BYTES` | InputBatcher | `1048576` | Tamaño serializado máximo de cada batch |
| `INPUT_QUEUE` | Preprocessor | `preprocessor.input` | Cola de batches crudos |
| `OUTPUT_QUEUE` | Preprocessor | `query_router.input` | Cola de batches normalizados |
| `CONTROL_EXCHANGE` | Preprocessor | `preprocessor.control` | Exchange que difunde el final a todas las réplicas |
| `PARTITION_ID` | Preprocessor | `0` | Identificador de la réplica dentro de la barrera |
| `INPUT_QUEUE` | QueryRouter | `query_router.input` | Cola de batches normalizados |
| `Q1_Q2_OUTPUT_QUEUE` | QueryRouter | `until_2020.input` | Cola de la rama Q1/Q2 |
| `TEXT_OUTPUT_QUEUE` | QueryRouter | `word_extractor.input` | Cola de la rama Q3/Q4/Q5 |
| `CONTROL_EXCHANGE` | QueryRouter | `query_router.control` | Exchange que difunde los finales de Preprocessor al grupo |
| `PARTITION_ID` | QueryRouter | `0` | Identificador de la réplica dentro de la barrera posterior |
| `PREPROCESSOR_PARTITION_COUNT` | QueryRouter | `1` | Cantidad de finales distintos de Preprocessor esperados; Compose configura `3` |
| `INPUT_QUEUE` | Until2020Filter | `until_2020.input` | Cola compartida de Q1 y Q2 |
| `Q1_OUTPUT_QUEUE` | Until2020Filter | `q1_english_filter.input` | Cola de candidatos para Q1 |
| `Q2_OUTPUT_QUEUE` | Until2020Filter | `q2_biography_filter.input` | Cola de candidatos para Q2 |
| `CONTROL_EXCHANGE` | Until2020Filter | `until_2020.control` | Exchange que difunde los finales de QueryRouter a sus réplicas |
| `PARTITION_ID` | Until2020Filter/Q1/Q2BiographyFilter/ImageFetcher | `0` | Identificador de la réplica o partición productora |
| `QUERY_ROUTER_PARTITION_COUNT` | Until2020Filter/WordExtractor | `1` | Cantidad de finales distintos de QueryRouter esperados; Compose configura `3` |
| `INPUT_QUEUE` | Q1 | `q1_english_filter.input` | Cola de candidatos de Q1 |
| `OUTPUT_QUEUE` | Q1 | `result_sink.input` | Cola de resultados y finales de partición de Q1 |
| `UNTIL_2020_PARTITION_COUNT` | Q1/Q2BiographyFilter | `1` | Cantidad de finales de Until2020Filter esperados; Compose configura `3` |
| `INPUT_QUEUE` | Q2BiographyFilter | `q2_biography_filter.input` | Cola de candidatos de Q2 |
| `OUTPUT_QUEUE` | Q2BiographyFilter | `image_fetcher.input` | Cola de candidatos para descarga de imágenes |
| `CONTROL_EXCHANGE` | Q2BiographyFilter | `q2_biography_filter.control` | Exchange que difunde los finales de Until2020Filter a las tres réplicas |
| `INPUT_QUEUE` | ImageFetcher | `image_fetcher.input` | Cola de candidatos con sección biográfica |
| `OUTPUT_QUEUE` | ImageFetcher | `result_sink.input` | Cola de resultados y finales de partición de Q2 |
| `CONTROL_EXCHANGE` | ImageFetcher | `image_fetcher.control` | Exchange que difunde los finales de Q2BiographyFilter a las tres réplicas |
| `BIOGRAPHY_FILTER_PARTITION_COUNT` | ImageFetcher | `1` | Cantidad de finales de Q2BiographyFilter esperados; Compose configura `3` |
| `INPUT_QUEUE` | WordExtractor | `word_extractor.input` | Cola compartida de batches de abstracts |
| `Q3_OUTPUT_QUEUE` | WordExtractor | `q3_word_projector.input` | Artículos tokenizados para Q3 |
| `Q4_Q5_OUTPUT_QUEUE` | WordExtractor | `q4_q5_word_projector.input` | Artículos tokenizados para Q4/Q5 |
| `CONTROL_EXCHANGE` | WordExtractor/Q3WordProjector/Q4Q5WordProjector/Q5DateRouter | Según la etapa | Difusión de finales a las tres réplicas |
| `PARTITION_ID` | Etapas replicadas de la rama textual | `0` | Identificador de la réplica productora |
| `INPUT_QUEUE` | Q3WordProjector | `q3_word_projector.input` | Artículos tokenizados |
| `OUTPUT_QUEUE_PREFIX` | Q3WordProjector | `q3_word_aggregator.input` | Prefijo de tres particiones por `(execution_id, word)` |
| `INPUT_QUEUE` | Q3WordAggregator | `q3_word_aggregator.input.<partition>` | Presencias asignadas a la partición |
| `OUTPUT_QUEUE` | Q3WordAggregator | `result_sink.input` | Resultados y final de partición de Q3 |
| `INPUT_QUEUE` | Q4Q5WordProjector | `q4_q5_word_projector.input` | Artículos tokenizados para Q4 y Q5 |
| `Q4_OUTPUT_QUEUE_PREFIX` | Q4Q5WordProjector | `q4_word_counter.input` | Prefijo de tres particiones por `(execution_id, word)` |
| `Q5_OUTPUT_QUEUE` | Q4Q5WordProjector | `q5_date_router.input` | Artículos y sus conteos para Q5 |
| `INPUT_QUEUE` | Q4WordCounter | `q4_word_counter.input.<partition>` | Apariciones de palabras de Q4 |
| `OUTPUT_QUEUE_PREFIX` | Q4WordCounter | `q4_top20_reducer.input` | Tres reducers físicos elegidos por `execution_id` |
| `PARTITION_ID` | Q4WordCounter | `0` | Identificador de la partición de conteo |
| `INPUT_QUEUE` | Q4Top20Reducer | `q4_top20_reducer.input.<partition>` | Tops de las tres particiones para ejecuciones asignadas |
| `OUTPUT_QUEUE` | Q4Top20Reducer | `result_sink.input` | Resultados y final de Q4 |
| `INPUT_QUEUE` | Q5DateRouter | `q5_date_router.input` | Artículos con conteo para Q5 |
| `HISTORICAL_OUTPUT_QUEUE_PREFIX` | Q5DateRouter | `q5_stats_worker.input` | Workers elegidos por `execution_id` |
| `CANDIDATE_OUTPUT_QUEUE_PREFIX` | Q5DateRouter | `q5_comparator.input` | Comparators elegidos por `(execution_id, article_id)` |
| `INPUT_QUEUE` | Q5StatsWorker | `q5_stats_worker.input.<partition>` | Conteos históricos de Q5 |
| `OUTPUT_QUEUE_PREFIX` | Q5StatsWorker | `q5_average_reducer.input` | Reducers elegidos por `execution_id` |
| `PARTITION_ID` | Q5StatsWorker | `0` | Identificador de la partición de estadísticas |
| `INPUT_QUEUE` | Q5AverageReducer | `q5_average_reducer.input.<partition>` | Parciales históricos de ejecuciones asignadas |
| `OUTPUT_QUEUE_PREFIX` | Q5AverageReducer | `q5_comparator.input` | Difusión del promedio a los tres comparators |
| `PARTITION_COUNT` | Q5AverageReducer | `1` en Python; `3` en Compose | Cantidad de StatsWorkers esperados por ejecución |
| `INPUT_QUEUE` | Q5Comparator | `q5_comparator.input.<partition>` | Candidatos asignados, finales y promedio difundido |
| `OUTPUT_QUEUE` | Q5Comparator | `result_sink.input` | Resultados y final de partición de Q5 |
| `Q5_BUFFER_DIRECTORY` | Q5Comparator | `/tmp/q5-candidates` | Directorio del buffer JSONL; Compose asigna `/var/lib/q5-candidates/<partition>` |
| `INPUT_FILE` | Client | `datasets/wikipedia_sample.parquet` | Dataset de entrada |
| `OUTPUT_DIR` | Client | `output/distributed` | Directorio de resultados |
| `LOG_LEVEL` | Todos los procesos | `INFO` | Nivel de logging |

## Configuración de escalabilidad

Las cantidades de réplicas se expresan actualmente en `docker-compose.yaml` y
los procesos reciben por variables de entorno sus identificadores, cantidades
esperadas, prefijos de colas y exchanges. Como mejora operativa futura se podrá
generar esa topología desde un conjunto más pequeño de variables globales:

```text
MAX_RECORD_BYTES
Q3_PARTITIONS
Q4_COUNTER_PARTITIONS
Q4_REDUCER_PARTITIONS
Q5_STATS_WORKERS
Q5_REDUCER_PARTITIONS
Q5_COMPARATOR_PARTITIONS
RESULT_SINK_PARTITIONS
IMAGE_FETCH_TIMEOUT
IMAGE_FETCH_RETRIES
IMAGE_FETCH_CONCURRENCY
```

Estas variables globales todavía no generan Compose automáticamente; la topología
equivalente de tres instancias sí está implementada de manera explícita.

## Invariantes principales

La implementación distribuida completa deberá respetar:

1. Todos los artículos y `INPUT_END` de una ejecución llegan a la misma partición de InputBatcher.
2. Ningún batch supera `MAX_BATCH_BYTES`.
3. El último batch se publica antes del final de la etapa.
4. Una etapa replicada no se considera terminada hasta completar su barrera.
5. Cada query emite exactamente un `QUERY_END`.
6. La misma clave stateful llega siempre a la misma partición.
7. Q1 y Q2 comparten el filtro `fecha <= 2020`.
8. Q4 y Q5 utilizan el mismo criterio de conteo.
9. RabbitMQ no se usa como almacenamiento prolongado de candidatos de Q5.
10. Un ACK se envía solamente después de completar el efecto del mensaje.
11. Los recursos temporales se eliminan al completar o abortar una ejecución.
12. Un `SIGTERM` no fabrica mensajes de finalización para trabajo incompleto.
13. Gateway queda fuera del modelo de fallos, actúa solo como adaptador y relay, y
    no incorpora estado de negocio ni mecanismos propios de recuperación.

## Orden de implementación

Los dos primeros hitos ya completaron el pipeline funcional y su escalabilidad.
El orden vigente es el del [plan de implementación de tolerancia a fallos](#plan-de-implementación):

1. Confirmar modelo de fallos y propuesta de consenso con el corrector.
2. Congelar y medir la línea base de escalabilidad.
3. Introducir identidad determinista y pruebas de puntos de caída.
4. Construir RecoveryStore, inbox y outbox.
5. Hacer durable y reconectable el transporte.
6. Recuperar el pipeline por etapas.
7. Implementar supervisión, consenso y reinicio.
8. Garantizar exactly-once observable en ResultSink y Client sin cargar estado de
   negocio en Gateway.
9. Completar cleanup, Chaos Monkey, benchmark, informe y demo.

## Próximos pasos

1. Llevar al corrector las decisiones abiertas de consenso, modelo de fallos,
   exactly-once y uso limitado del socket de Docker.
2. Acordar con el grupo la rama y responsables del Bloque 0, evitando cambios
   funcionales superpuestos.
3. Registrar el commit base y automatizar tres mediciones de 50.000 artículos sin
   fallas antes de agregar persistencia.
4. Diseñar los IDs y la matriz de puntos de caída del Bloque 1.

La topología base mantiene tres instancias en los grupos coordinados y tres
particiones de InputBatcher. La comparación de performance del último hito debe
usar exactamente la misma topología, dataset y host para aislar el costo de la
tolerancia a fallos.

## Monitoreo

El monitoreo es opcional y se encuentra agrupado bajo el perfil `monitoring`.
Este perfil inicia cAdvisor, Prometheus y Grafana. RabbitMQ expone sus métricas
mediante el plugin nativo `rabbitmq_prometheus`.

Para iniciar el sistema junto con el monitoreo:

```sh
docker compose --profile monitoring up -d --build
```

El arranque habitual mediante `make up` no inicia los componentes de monitoreo.

- Grafana: <http://localhost:3000/>, usuario `admin`, contraseña
  `admin`.
- Prometheus: <http://localhost:9090>. En `/targets` deben aparecer `prometheus`,
  `cadvisor`, `rabbitmq` y `rabbitmq-queues` con estado `UP`.

El datasource y los dashboards **System Overview** y **Pipeline Details** se
aprovisionan automáticamente en Grafana.

**System Overview** resume el estado de los scrapes, los recursos de los
containers y la actividad global de RabbitMQ. **Pipeline Details** separa CPU,
memoria, disco, red, mensajes y consumidores por componente y por rama Q1-Q5.
Sus filtros permiten seleccionar el proyecto de Compose, el servicio, el vhost y
la cola. Esta segunda vista facilita identificar el cuello de botella de cada
query durante las pruebas de escalabilidad.

### Observaciones previstas para la demo

La demo no se limitará a mostrar los resultados finales. También se utilizarán
el panel de administración de RabbitMQ, las estadísticas de Docker y el dashboard
de Grafana para relacionar el comportamiento observable con las decisiones de la
arquitectura.

El guion de demostración de tolerancia deberá incluir, una vez implementadas las
capacidades correspondientes:

- Ejecutar dos clientes concurrentes y comprobar que sus ejecuciones, colas de
  respuesta y archivos de salida permanecen aislados.
- Repetir la prueba con tres clientes para observar cómo cambia la carga del
  sistema.
- Interrumpir uno de los clientes y verificar qué recursos temporales permanecen,
  qué ocurre con su cola de resultados y cómo se recuperan o eliminan los datos
  persistidos en volúmenes de Docker.
- Inspeccionar en RabbitMQ los mensajes listos, los mensajes sin confirmar, los
  consumidores, las tasas de publicación, ACK, NACK y reentrega.
- Identificar qué componentes pueden emitir un NACK, ante qué errores y si el
  mensaje se reencola o se descarta.
- Comparar el consumo de CPU, memoria y red de cada componente y de las distintas
  ramas de queries durante una ejecución.
- Relacionar los cambios de carga con el tamaño de las colas para detectar el
  componente que actúa como cuello de botella.
- Mostrar los PING/PONG de salud sin consultar Docker, una elección Bully de
  coordinador y el
  acuerdo mayoritario de una propuesta de reinicio.
- Ejecutar Chaos Monkey con una seed conocida y mostrar la caída abrupta, el
  reinicio y la recuperación de progreso.
- Perder un ACK de resultado y comprobar que la reentrega no duplica líneas. No se
  inyectan caídas del Gateway porque está excluido del modelo de fallos acordado.
- Finalizar comparando Q1-Q5 y verificando que no queden journals, buffers, colas
  temporales ni estado activo de la ejecución.

La concurrencia de clientes y la desconexión durante el upload ya están
implementadas. La reanudación de una misma ejecución y la recuperación ante
caídas internas todavía están pendientes.

cAdvisor accede al daemon de Docker mediante el socket
`/var/run/docker.sock`, montado dentro del contenedor, y al runtime containerd
mediante `/rootfs/run/containerd/containerd.sock`. También requiere acceso al host,
modo privilegiado y `/dev/kmsg`. Si Docker utiliza otro directorio de datos, se
debe definir `DOCKER_DATA_ROOT` con el valor obtenido mediante:

```sh
docker info --format '{{.DockerRootDir}}'
```

Las instalaciones rootless o con una ubicación no convencional del socket pueden
requerir adaptar estos montajes.

Para diagnosticar problemas:

```sh
docker compose ps
docker compose logs --tail=100 cadvisor prometheus grafana rabbitmq
curl -fsS http://localhost:9090/-/ready
curl -fsS http://localhost:3000/api/health
```

Para detener todo el entorno, incluido el monitoreo:

```sh
docker compose --profile monitoring down
```

Referencias:

- [cAdvisor Docker exporter](https://prometheus.io/docs/guides/cadvisor/)
- [RabbitMQ Exporter](https://www.rabbitmq.com/docs/3.13/prometheus)
- [Grafana provisioning](https://grafana.com/docs/grafana/latest/administration/provisioning/)

## Cierre por señales

Los procesos del pipeline atienden `SIGTERM` y `SIGINT`: dejan de consumir,
terminan el callback activo y cierran sus conexiones. Las publicaciones usan
confirmaciones del broker en los canales de la capa común. Esto confirma recepción
en RabbitMQ, no procesamiento downstream ni persistencia. InputBatcher confirma
artículos al incorporarlos al buffer; no espera a publicar cada batch. Los mensajes entregados
pero aún no procesados se reencolan al cancelar el consumidor o cerrar la conexión.
InputBatcher publica sus batches parciales sin generar finales.
Los consumidores coordinados detienen y esperan su thread de control.

Gateway deja de aceptar clientes, interrumpe sus sockets y detiene los consumidores
de resultados; espera los threads de sesión para que cada uno cierre sus conexiones
y elimine su cola exclusiva. No fabrica `INPUT_END` para uploads interrumpidos
por el cierre del servidor. Cada `ClientSession` agrupa socket, thread y referencia
al middleware, sincroniza el stop con el cierre y permite esperar su finalización.
El listener revisa la señal de cierre como máximo cada segundo mientras está inactivo.

Client atiende las señales mediante `SystemExit(0)`: al salir de los contextos de
`run()` y `receive_results()`, cierra el socket y hace flush y cierre de los archivos
abiertos. Los archivos pueden quedar incompletos; el código de salida actual no
distingue esta interrupción de una ejecución exitosa.

Compose permite configurar `STOP_GRACE_PERIOD` (por defecto `2m`) para el drenaje.
Para usar ese plazo, ejecutar `docker compose down`.

Si se agota el plazo, Docker envía `SIGKILL`: no se ejecutan las siguientes líneas
ni los bloques de limpieza. Si falla la publicación de un batch ya confirmado en
entrada, su contenido puede perderse al terminar el proceso. Las pruebas de cierre
no sustituyen la validación del despliegue completo bajo carga.
