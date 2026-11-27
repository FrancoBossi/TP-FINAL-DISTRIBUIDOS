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
| Estrategia de tolerancia a fallos y persistencia | Persistencia, inbox/outbox y recuperación propagadas y validadas con una caída simultánea real de todo el pipeline |
| Detección de procesos caídos mediante comunicación | Implementada con `PING`/`PONG` directo desde el coordinador |
| Consenso implementado por el grupo | Implementado con Bully para elegir el coordinador de los FailureManagers |
| Reinicio automático de procesos caídos | Implementado mediante FailureManagers y validado en la matriz exhaustiva |
| Recuperación e idempotencia después de reinicios | Implementada y validada punta a punta con detección y reinicio automáticos |
| Entrega exactly-once hacia el cliente | Implementada y validada con reinicios reales de ResultSink; ACK perdido y redelivery cubiertos unitariamente |
| Chaos Monkey y escenarios de fallos automatizados | Implementados; matriz exhaustiva de 20 escenarios ejecutada con reinicio automático, comparación Q1-Q5 y auditoría de cleanup |
| Comparación de performance contra el hito de escalabilidad | Pendiente |
| Limpieza de recursos de ejecuciones resueltas o fallidas | Implementada y auditada en desconexión de cliente y en las 60 corridas de la matriz exhaustiva |
| Gateway dentro del modelo de fallos | Excluido por indicación de la cátedra; se asume disponible y debe mantenerse mínimo |

## Hito 3: tolerancia a fallos

Esta sección es el tablero interno para el último hito. Distingue requisitos de
la consigna, riesgos comprobados en el código actual, decisiones confirmadas con
el corrector y tareas verificables. Los checks deben marcarse únicamente cuando exista código,
prueba automatizada y evidencia reproducible.

### Alcance obligatorio y evidencia esperada

| Requisito | Evidencia mínima para considerarlo cumplido |
|---|---|
| Tolerar fallas abruptas dentro de un proceso y caídas repentinas de otros nodos | Una ejecución iniciada antes de la caída termina con los cinco resultados correctos después de reiniciar el proceso afectado |
| Reiniciar procesos caídos | Los supervisores reponen automáticamente el proceso sin intervención manual |
| Verificar disponibilidad comunicándose con el nodo | La detección utiliza un protocolo de salud propio; no consulta estado, eventos ni metadatos de Docker |
| Implementar un algoritmo de consenso | Los FailureManagers implementan Bully para consensuar quién es el coordinador, sin bibliotecas externas |
| Chaos Monkey propio | Una herramienta del repositorio introduce fallas periódicas y configurables durante una ejecución |
| Automatizar los escenarios de fallos | Un comando levanta el entorno, ejecuta cliente y fallas, espera la recuperación, compara resultados y verifica limpieza |
| No usar bibliotecas externas de consenso, tolerancia o recuperación | El protocolo, la persistencia y la recuperación se implementan en el proyecto; cualquier dependencia nueva se consulta antes con la cátedra |
| Limitar Docker-in-Docker o el socket de Docker al reinicio | Docker se usa únicamente para matar procesos en Chaos Monkey y para ejecutar el reinicio ordenado por el coordinador elegido mediante Bully |
| Eliminar archivos y liberar recursos | Cada ejecución exitosa o fallida elimina colas temporales, journals, snapshots, outboxes y buffers propios cuando ya no son necesarios |
| Entrega exactly-once al cliente | Una reentrega o reconexión nunca agrega dos veces el mismo resultado lógico al archivo del cliente |
| Documento 4+1 o C4 actualizado | `INFORME.md` y los diagramas reflejan detección, consenso, reinicio, recuperación, limpieza y escenarios de fallos |
| Comparar performance con escalabilidad | Benchmark automatizado con el mismo dataset y topología, varias corridas y métricas comparables con y sin tolerancia habilitada |
| Demo con muestra significativa y fallas periódicas | Guion ensayado que muestra fallas, elección Bully, reinicio, recuperación, resultados correctos y recursos finales limpios |

Los requisitos del hito de escalabilidad siguen vigentes: no se puede degradar el
streaming, la coordinación de barreras, el uso del middleware, la capacidad de
escalar ni la comparación contra la referencia serial.

### Modelo de fallos confirmado con el corrector

Para el desarrollo, las pruebas y la demo se acuerdan los siguientes supuestos:

- Gateway y RabbitMQ no se caen y quedan fuera de Chaos Monkey.
- Puede haber una o varias caídas simultáneas de procesos del pipeline; la lógica
  de detección y reinicio no debe depender de que las caídas sean secuenciales.
- No se contemplan particiones de red. Todos los nodos pueden comunicarse con los
  FailureManagers.
- Los FailureManagers sí pueden caer, pero el inyector de fallas debe garantizar
  que al menos uno permanezca vivo para reiniciar a los demás.
- El cliente no caerá durante la demo completa del dataset. Debe existir un
  escenario separado que interrumpa un cliente y demuestre la liberación de todos
  los recursos de su ejecución.

Gateway se mantiene estable y mínimo. Por lo tanto:

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

Este supuesto elimina del alcance la recuperación ante caída del proceso Gateway.
RabbitMQ también queda fuera del modelo de caídas, pero se mantiene el manejo
ordenado de errores de publicación y consumo. La entrega exactly-once hacia un
cliente conectado sigue siendo obligatoria.

### Diagnóstico del sistema actual

| Área | Estado actual | Riesgo frente a una caída |
|---|---|---|
| RabbitMQ | La cátedra permite asumir que no cae | No se requiere recuperar el broker ni persistir sus datos; sí conservar mensajes cuando cae un consumidor |
| Middleware | Las conexiones se crean al arrancar y las colas DATA compartidas sobreviven a la caída de un consumidor | Un worker reiniciado debe volver a declarar su topología y continuar consumiendo el trabajo pendiente |
| Exchanges de control | Cada réplica usa una cola estable, no exclusiva ni autoeliminable | El control queda en RabbitMQ mientras la réplica se reinicia y la barrera interna se recupera desde su volumen |
| InputBatcher | Confirma conjuntamente inbox, batch parcial y outbox en disco | Recupera entradas aplicadas y salidas pendientes sin perder el batch parcial |
| Barreras replicadas | Finales observados, final emitido e inbox/outbox se confirman juntos | Falta comprobar la recuperación de cada grupo matando containers reales |
| Agregadores y reducers | Q3, Q4 y Q5 persisten sus acumuladores, barreras e inbox/outbox | Falta la corrida integral con caídas y comparación contra la referencia |
| Q5Comparator | Candidatos en JSONL; promedio, finales, completitud e inbox/outbox en snapshot atómico | La consistencia entre ambos archivos debe ejercitarse con caídas reales en todas las ventanas |
| Publicación y ACK | El estado y la outbox se persisten antes de publicar; cada salida se retira luego del confirm | Puede haber publicación física repetida, pero mantiene identidad estable y downstream la deduplica |
| Mensajes | DATA, CONTROL, resultados y finales poseen IDs deterministas; las etapas durables usan inbox/outbox | Preprocessor permanece stateless y se apoya en la deduplicación de la etapa siguiente |
| ResultSink | Persiste inbox, barreras y outbox de resultados/finales | Falta demostrar su reinicio dentro de una ejecución punta a punta |
| Gateway | Instancia única, deliberadamente mínima; la cátedra permite asumir que no cae | No se diseña recuperación de su proceso; debe evitarse trasladar estado de negocio o coordinación hacia él |
| Cliente | Durante la sesión escribe y sincroniza resultado e ID antes del ACK; una redelivery se confirma sin reescribir | La reanudación de un cliente caído queda fuera del modelo; se debe probar la liberación de sus recursos |
| Ciclo de vida | Hay limpiezas locales al finalizar correctamente, pero no existe un protocolo global de aborto/cleanup | Una ejecución fallida puede dejar estado, archivos o colas indefinidamente |
| Control de fallos | No hay detectores, supervisores, consenso ni reinicio automático | Cualquier proceso caído queda fuera hasta una intervención manual |

### Arquitectura confirmada como punto de partida

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
  respuesta permite al coordinador considerar caído al nodo y reiniciarlo; Docker
  no participa de esa detección.
- Los supervisores también se controlan entre sí. Si el coordinador cae, los
  FailureManagers que sigan vivos ejecutan Bully para elegir otro. Incluso uno
  solo puede asumir como coordinador y recuperar a los demás.
- El listado de procesos, endpoint de salud e identificador estático necesario
  para reiniciarlo proviene de configuración. No se obtiene con `docker ps`,
  `inspect`, eventos ni healthchecks de Docker.
- Al asumir, un nuevo coordinador verifica por comunicación todos los targets de
  la configuración; así puede detectar varias caídas simultáneas sin depender del
  estado en memoria del coordinador anterior.

#### 2. Bully y decisión de reinicio

- Implementar el algoritmo **Bully**, visto en clase, para elegir un coordinador
  entre los tres FailureManagers. Cada supervisor posee un ID total y estable.
- Si un supervisor deja de recibir mensajes del coordinador, envía `ELECTION` a
  los IDs superiores. Si ninguno responde se anuncia mediante `COORDINATOR`; si
  recibe `OK`, espera que uno de los superiores complete la elección.
- El coordinador mantiene su autoridad mediante mensajes entre supervisores. Cada
  elección recibe un `coordinator_epoch` creciente para rechazar anuncios o
  decisiones de coordinadores anteriores.
- La elección de coordinador mediante Bully es el algoritmo de consenso exigido:
  los FailureManagers acuerdan quién es el líder. El corrector indicó que no hace
  falta una votación adicional para decidir si un nodo funciona.
- El coordinador detecta la caída comunicándose directamente con el target. Si no
  obtiene una respuesta válida dentro de la política de timeout y reintentos,
  invoca una operación estática de
  reinicio mediante Docker Engine o Docker-in-Docker. Esa integración no consulta
  Docker para decidir disponibilidad ni para descubrir targets.
- La orden se identifica con `target_id`, `incarnation_id` y
  `coordinator_epoch` para ignorar acciones viejas y no reiniciar una instancia
  nueva por una detección anterior.
- Si el coordinador cae antes o después de ejecutar la acción, Bully elige otro;
  el nuevo coordinador vuelve a verificar el target por red antes de decidir si
  todavía necesita reiniciarlo.
- Los timeouts usan relojes y eventos; no se utilizan sleeps para sincronizar la
  elección ni la detección.
- No se contemplan particiones de red. El algoritmo debe tolerar caídas simultáneas
  mientras al menos un FailureManager permanezca vivo.

#### 3. Colas que sobreviven al reinicio de consumidores

- RabbitMQ no se cae en el modelo acordado, por lo que no se requiere volumen del
  broker, recuperación de sus datos ni mensajes persistentes para sobrevivir a un
  reinicio del broker.
- Las colas DATA compartidas o particionadas deben permanecer declaradas cuando
  desaparece un consumidor. Los mensajes sin ACK se reencolan y quedan disponibles
  para la instancia reiniciada.
- Se reemplazaron las colas exclusivas y autoeliminables de control por colas con
  nombre estable por grupo y partición. Una réplica reiniciada puede recibir los
  controles que llegaron mientras estaba caída.
- Al arrancar después de una caída, cada proceso vuelve a declarar su topología,
  abre conexiones nuevas y continúa consumiendo. Los reintentos de conexión de
  startup deben ser acotados, configurables e interrumpibles durante el shutdown.
- No se agregan mecanismos para detectar o recuperar una caída de RabbitMQ porque
  está explícitamente fuera del alcance.

#### 4. Identidad, idempotencia y recuperación local

- Agregar identidad determinista al protocolo interno:
  `message_id`, `execution_id` y, cuando corresponda, `result_id` o `article_id`.
  Las secuencias, particiones productoras y relaciones causales se mantienen como
  material local para derivar IDs y auditar la ejecución, no como campos globales.
- Derivar los IDs de salida de la entrada y de la operación lógica, no de UUIDs
  generados al reintentar. La misma entrada redeliverada debe producir los mismos
  IDs.
- Implementar persistencia propia a disco con la biblioteca estándar de Python;
  no se propone una biblioteca externa. Utilizar archivos JSON/JSONL, `fsync` y
  reemplazo atómico cuando corresponda.
- Guardar por `execution_id` el estado necesario de batches, deduplicación,
  barreras, agregaciones, reducers, promedio y finalización.
- Usar el patrón **inbox + estado + outbox**: registrar atómicamente que una entrada
  fue aplicada y qué salidas debe producir; publicar la outbox; recién entonces
  confirmar la entrada. Si hay una caída después de publicar, la salida puede
  repetirse físicamente, pero conserva su ID y el receptor la aplica una sola vez.
- Mantener checkpoints o message logs solamente donde el componente tenga estado
  que deba reconstruirse. La primera solución debe priorizar archivos simples y
  verificables antes que una infraestructura genérica innecesaria.
- Montar almacenamiento persistente separado por componente y partición. No se
  comparte estado mutable entre dueños lógicos distintos.

##### Contrato de identidad de mensajes

La identidad lógica no es el `delivery_tag` de RabbitMQ ni un UUID creado al
publicar. Esos valores cambian en una reentrega. El envelope de todos los mensajes
internos de DATA y CONTROL se mantiene deliberadamente mínimo:

| Campo | Contrato |
|---|---|
| `execution_id` | Ejecución a la que pertenece el mensaje; continúa siendo generado una sola vez por Gateway |
| `message_id` | Identidad estable de la ocurrencia lógica; una reentrega física conserva exactamente el mismo valor |

`producer_id`, las secuencias y el `message_id` de la entrada causal se utilizan
localmente como material para calcular el ID, pero no viajan como campos de todos
los mensajes. El consumidor downstream solo necesita `message_id` para deduplicar.
Esta decisión evita acoplar el protocolo a información de tracing que no participa
de la corrección. Cuando haga falta diagnóstico, la relación causal se registra en
logs y journals del productor.

Los IDs usarán una versión explícita y SHA-256 sobre JSON canónico UTF-8, con
claves ordenadas y separadores fijos. El prefijo distingue el dominio (`m1` para
mensajes y `r1` para resultados). No se hashea
el mensaje serializado completo porque agregar metadatos o cambiar el orden de
campos no debe modificar su identidad.

El helper común para generar y derivar `m1` y `r1` está implementado en
`src/common/message_identity.py`. Gateway identifica el ingreso, InputBatcher deriva
la identidad de cada batch y el resto del DAG propaga IDs deterministas por
transformaciones, fan-outs, splits, agregaciones y barreras. `ResultMessage` y
`QueryEndMessage` llevan además `result_id`, que ya cruza el protocolo binario hasta
el cliente. Todavía no existen inbox/outbox ni deduplicación durable.

Reglas de derivación:

1. Gateway, que está excluido de caídas, numera los artículos de cada conexión
   desde cero. Un `RoutedArticleMessage` se identifica con
   `(execution_id, "gateway", "article", input_sequence)` y el final con
   `(execution_id, "gateway", "input_end", total_articles)`. Esto distingue dos
   ocurrencias aunque tengan el mismo payload y no agrega estado durable al Gateway.
2. InputBatcher deriva cada `ArticleBatchMessage` de la partición que lo produce y
   de la lista ordenada de `message_id` de los artículos que contiene. La lista es
   estado local: solo el hash resultante viaja en el DTO. Cuando se implemente la
   recuperación, el batch parcial deberá persistir sus artículos y esa lista para
   reconstruir exactamente el mismo ID después de reiniciar.
3. Una transformación uno-a-uno deriva la salida de
   `(input_message_id, producer_id, output_kind, destination, output_index)`. Para un
   fan-out o split, destinos e índices se asignan de manera determinista; una
   reejecución genera los mismos IDs aunque publique nuevamente.
4. Los controles de fin se identifican por su significado, por ejemplo
   `(execution_id, stage, partition_id, "end")`; no dependen del último mensaje
   que casualmente completó la barrera.
5. Parciales, promedios y resultados de reducers usan su clave lógica estable:
   ejecución, etapa, partición y tipo de agregado. No dependen del orden de
   recepción ni del número de intento.
6. `ResultMessage` agrega `result_id`: Q1, Q2 y Q5 usan la URL del artículo; Q3 y
   Q4 usan la palabra; `QueryEndMessage` usa `(execution_id, query_id, "end")`.
   El registro completo no forma parte de la identidad, pero una deduplicación
   que encuentre el mismo ID con otro contenido debe fallar visiblemente.

Aplicación por familia actual:

| Familia | Tipos actuales | Fuente de identidad |
|---|---|---|
| Ingreso | `RoutedArticleMessage`, `RoutedInputEndMessage` | `input_sequence` asignada por Gateway |
| Batching | `ArticleBatchMessage` | partición de InputBatcher + lista ordenada de IDs de entrada |
| Transformación pura | `NormalizedArticleBatchMessage`, `Q1Q2ArticleBatchMessage`, `Q1CandidateBatchMessage`, `Q2CandidateBatchMessage`, `ImageCandidateBatchMessage`, `TextArticleBatchMessage`, `TokenizedArticleBatchMessage` | ID de entrada + tipo de salida + destino + índice |
| Split particionado | `Q3WordPresenceBatchMessage`, `Q4WordBatchMessage`, `Q5ArticleWordCountBatchMessage`, `Q5HistoricalWordCountBatchMessage`, `Q5CandidateBatchMessage` | ID de entrada + partición destino + índice del sub-batch |
| Agregación | `Q4PartialTopBatchMessage`, `Q5PartialStatsMessage`, `Q5AverageMessage` | ejecución + productor/partición + operación lógica |
| Barreras | `PartitionEndMessage`, `Q4PartitionEndMessage`, `Q5StatsPartitionEndMessage`, `QueryPartitionEndMessage` | ejecución + etapa/query + partición + `end` |
| Frontera de salida | `ResultMessage`, `QueryEndMessage` | `result_id` lógico estable y `message_id` derivado de este |

`ArticleMessage`, `InputEndMessage`, `AckMessage` y `ErrorMessage` pertenecen al
protocolo externo. Los ACK de resultados deberán indicar el `result_id` confirmado;
los ACK de carga pueden continuar siendo posicionales porque el intercambio con
Gateway es estrictamente secuencial y Gateway no cae en el modelo acordado.

##### Cuándo se aplica y cuándo se confirma una entrada

Un consumidor considera aplicada una entrada únicamente cuando un registro
durable y sincronizado a disco contiene, como una sola transición recuperable:

- el `message_id` en la inbox;
- la mutación de estado producida por esa entrada;
- todas las salidas lógicas pendientes en la outbox con sus IDs definitivos.

El ACK de RabbitMQ se envía solamente después de publicar toda la outbox
correspondiente y recibir confirmación del broker. Si la inbox ya contiene el ID,
el consumidor no vuelve a mutar estado: completa publicaciones pendientes y luego
confirma la reentrega. Que una publicación confirmada se repita físicamente es
válido; el receptor la descarta por `message_id`.

| Punto de caída | Estado recuperado | Conducta requerida |
|---|---|---|
| Antes del `fsync` de inbox/estado/outbox | La entrada no está aplicada | RabbitMQ la reentrega y se procesa normalmente |
| Después del `fsync` y antes de publicar | Entrada aplicada y outbox pendiente | Reinicio publica la outbox sin reaplicar el estado |
| En medio de un fan-out | Algunas salidas pudieron publicarse | Reinicio publica todas con los mismos IDs; downstream deduplica |
| Después de confirmar publicaciones y antes del ACK de entrada | Estado y salidas están completos | La entrada se reentrega, inbox evita reaplicarla y se vuelve a confirmar |
| Después del checkpoint de InputBatcher y antes de retirar el journal anterior | El journal viejo contiene información redundante | La generación confirmada hace que recuperación lo ignore y elimine |
| Al aplicar una salida duplicada | Inbox ya contiene su `message_id` | No modifica estado ni vuelve a producir efectos lógicos |
| Después de escribir un resultado y antes del ACK del cliente | `result_id` está durable en Client | El reenvío no agrega otra línea y el cliente repite el ACK |

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

Desde la raíz del repositorio, usar `uv run python -m scripts.chaos --help`
para ver los subcomandos. También se puede ejecutar con `.venv/bin/python -m
scripts.chaos`. Los subcomandos `sweep` y `matrix` usan el benchmark y
requieren las dependencias de `make setup`.

| Subcomando | Uso |
|---|---|
| `inject` | Inyectar caídas en un proyecto Compose activo. |
| `monitor` | Mostrar el historial de inyecciones o seguirlo en vivo. |
| `sweep` | Comparar intervalos de inyección y reportar el menor que superó las verificaciones. |
| `matrix` | Listar o ejecutar escenarios por tipo de servicio. |

Cada subcomando tiene su propio `--help`. Los comandos anteriores siguen
disponibles para los scripts existentes.

La herramienta será propia y separada de los supervisores:

- Recibe seed, intervalo, cantidad máxima de fallas y allowlist de targets.
- Elige targets desde configuración estática y registra instante, target, señal y
  seed para reproducir el escenario.
- Inyecta caídas abruptas (`SIGKILL` o stop equivalente) sin ejecutar cleanup del
  proceso víctima.
- Nunca mata Gateway ni RabbitMQ. El cliente tampoco se mata durante la demo del
  dataset completo; su desconexión se prueba en un escenario de cleanup separado.
- Puede incluir workers stateless, procesos con barreras, stateful, ResultSink y
  FailureManagers.
- Antes de elegir un FailureManager verifica en su propio estado que, después de
  la caída, quede al menos uno vivo. Esta condición es obligatoria: de lo contrario
  no habría un proceso capaz de reiniciar a los demás.
- Puede matar varios targets en el mismo ciclo para demostrar que las caídas
  simultáneas no alteran el algoritmo.
- El intervalo inicial recomendado por el corrector es de cinco segundos. Después
  se reduce progresivamente para encontrar y documentar la frecuencia máxima que
  el sistema tolera sin perder corrección.
- Docker puede utilizarse aquí para **introducir** la falla; esta información no se
  comparte como detector de disponibilidad con los supervisores.

Con el proyecto de Compose ya levantado y el cliente en ejecución, el inyector
puede ejecutarse desde otra terminal:

```sh
uv run python -m scripts.chaos inject \
  --target preprocessor-0 --target q3-word-aggregator-0 \
  --seed 42 --interval 5 --max-failures 8 --log chaos.jsonl
```

Si Compose se levantó con `-p`, pasar ese mismo nombre con `--project-name`.
Si se omite, Docker Compose resuelve el nombre del proyecto con su configuración
habitual. El log debe ser un archivo nuevo. El primer ciclo se programa después del intervalo indicado.
`--batch-size` permite incluir varios servicios distintos en un ciclo.
`--max-failures` limita los intentos programados: si un servicio ya está caído,
se registra el salto sin cambiar el calendario. Las réplicas supervisoras deben
usar nombres de servicio `failure-manager-*` para que el inyector preserve al
menos una según el estado observado antes de cada ciclo. Usar un solo inyector
por proyecto. El inyector registra las caídas y la supervisión se ocupa de
reiniciar los procesos.

Para observar Chaos Monkey en otra terminal:

```sh
uv run python -m scripts.chaos monitor --follow chaos.jsonl
```

Pasar el `chaos.jsonl` de la ejecución para monitorear un benchmark. Sin
`--follow`, el comando muestra el historial y sale. El monitor muestra el SCHEDULE, los intentos, las
confirmaciones de Compose y el motivo de cada skip. Una confirmación indica
que Compose ejecutó el stop.

El escenario automatizado mínimo deberá:

1. Generar o verificar la referencia serial.
2. Levantar el sistema desde un estado limpio.
3. Iniciar un cliente con una muestra significativa.
4. Iniciar Chaos Monkey con seed conocida y fallas periódicas.
5. Esperar recuperación y finalización con timeout global.
6. Comparar Q1-Q5 contra la referencia, incluyendo duplicados.
7. Verificar que Bully eligió un coordinador y que este detectó y reinició los
   procesos caídos mediante comunicación.
8. Verificar que no quedaron recursos de la ejecución.
9. Guardar logs y resumen del escenario como evidencia.

Para incorporar estas caídas a una corrida de benchmark:

```sh
.venv/bin/python scripts/run_benchmark.py \
  --dataset datasets/wikipedia_sample.parquet --expected-output output/reference \
  --project-name wikipedia-chaos --timeout-seconds 1800 \
  --chaos-target preprocessor-0 --chaos-target q3-word-aggregator-0 \
  --chaos-seed 42 --chaos-interval 5 --chaos-max-failures 8
```

Si Compose falla al levantar el entorno, consultar `startup.log` y `compose.log`
en el directorio de la corrida. Cada corrida con fallas
guarda `chaos.jsonl`, `chaos.log`, `client.log` y `compose.log`. Para pasar
debe confirmar al menos una inyección y coincidir con la referencia para
Q1–Q5. Además, exige que desaparezca la cola temporal, que todas las colas
queden sin mensajes `ready` ni `unacked`, que no subsistan estado o buffers de
la ejecución y que cada réplica alcanzada conserve su tombstone. El runner
detiene el inyector al terminar el cliente o vencer el plazo. Después de guardar
la evidencia elimina containers y volúmenes; la siguiente repetición empieza sin
estado durable heredado de una ejecución anterior.

### Plan de implementación

Cada bloque se trabaja en una rama acordada y se integra después de revisar código,
tests y documentación. No se debe empezar el bloque siguiente si la propiedad del
anterior todavía no puede demostrarse.

#### Bloque 0 - Registrar decisiones y congelar la línea base

- [x] Confirmar con el corrector el modelo de fallos, Bully, uso de Docker,
  exactly-once y responsabilidades del Gateway.
- [x] Registrar `4463695` como el commit exacto que representa el hito de escalabilidad.
- [x] Automatizar la línea base y validarla con una corrida smoke de 50.000
  artículos contra la referencia serial.
- [x] Guardar tiempos, throughput, CPU, memoria, red y resultados de tres corridas
  con `datasets/wikipedia_benchmark_12.parquet` (12 shards por idioma). Este
  tamaño mantiene una carga representativa dentro del máximo práctico de 30 minutos;
  los 90 shards por idioma demoraban varias horas por la consulta HTTP de Q2.
- [x] Definir targets: procesos del pipeline y FailureManagers; excluir Gateway,
  RabbitMQ y cliente durante la demo completa.

Evidencia de la línea base del 15 de noviembre de 2026:

- commit funcional base: `44636953cf1f903e3fd63a60a5c7bdeab6428418`;
- el runner registró `dirty: true` porque el propio tooling del benchmark y su
  documentación todavía no estaban versionados; no había cambios funcionales del pipeline;
- dataset: 612.475 artículos, SHA-256
  `47a250453b588c2c39354f92657af3ec522f09a61e17561c37c58c7aaee0ac70`;
- duraciones: 553,43 s, 549,45 s y 552,18 s; media 551,69 s
  (9 min 11,69 s) y desvío estándar 2,03 s;
- throughput medio: 1.110,19 artículos/s;
- CPU agregada media por corrida: 141,33%, 147,94% y 155,65%; picos
  agregados entre 389,57% y 404,71%;
- memoria agregada pico: entre 2,42 GiB y 2,46 GiB;
- I/O final agregado de las interfaces de contenedores: aproximadamente 25,79 GB
  recibidos y 23,44 GB transmitidos por corrida, incluyendo tráfico interno;
- Q1-Q5 produjeron fingerprints idénticos en las tres corridas, con 24.764,
  637, 137.673, 20 y 235.306 registros respectivamente;
- artefactos locales: `benchmarks/results/20261115-202022-44636953cf1f/`
  (ignorados por Git por su tamaño).

#### Bloque 1 - Contratos de identidad y pruebas de puntos de caída

- [x] Diseñar `message_id`, las reglas locales de secuencia/derivación y
  `result_id` para cada tipo de mensaje.
- [x] Propagar `message_id` por todo el DAG e incorporar `result_id` en resultados,
  finales y protocolo externo.
- [x] Documentar cuándo una entrada se considera aplicada y cuándo puede recibir ACK.
- [x] Enumerar puntos de caída antes/después de persistir, publicar y confirmar.
- [x] Agregar tests que reproduzcan redelivery y duplicación sin cambiar todavía la lógica funcional.

Las pruebas de caracterización cubren tres ventanas representativas antes de
introducir inbox/outbox: un fan-out cuya segunda publicación falla, una entrada
reentregada a un contador con estado y un resultado reentregado a ResultSink. En
los tres casos la identidad lógica se conserva; también queda demostrado que hoy
la publicación o el efecto todavía pueden repetirse.

#### Bloque 2 - Persistencia local, inbox y outbox

- [x] Definir archivos simples por componente, partición y `execution_id` usando
  únicamente biblioteca estándar.
- [x] Implementar y probar escritura temporal, `fsync`, reemplazo atómico y
  recuperación de una última escritura incompleta.
- [x] Implementar inbox deduplicada y outbox reintentable con IDs deterministas.
- [x] Probar caídas en cada transición y compactación sin pérdida ni doble aplicación.
- [x] Definir layout y volúmenes persistentes por componente/partición.

El sustrato común está implementado en `src/common/persistence.py`. El layout es:

```text
<state_root>/<component>/partition-<partition_id>/executions/<execution_id>/state.json
```

El `state.json` contendrá conjuntamente inbox, estado propio y outbox para que una
transición no quede repartida entre archivos con commits independientes. La
escritura usa `.state.json.tmp` en el mismo directorio, `flush`, `fsync` del archivo,
`os.replace` y `fsync` del directorio. Al recuperar, un temporal huérfano se
descarta como transición no confirmada y se conserva el último `state.json`; un
archivo confirmado corrupto produce un error visible.

Sobre ese archivo ya existe un modelo común de transición durable. Para una entrada
nueva, `DurableExecutionStore` incorpora su `message_id` a la inbox, reemplaza el
estado del componente y agrega las salidas a la outbox en un único commit. Una
reentrega presente en inbox no vuelve a modificar nada. Las salidas se identifican
por `(destination, message_id)`, permanecen pendientes después de reiniciar y se
eliminan individualmente solo después del publisher confirm. El mismo mensaje puede
estar pendiente para varios destinos, como ocurre al propagar un control a todas
las particiones.

Todos los componentes que conservan estado o participan de una barrera montan un
volumen nombrado e independiente en `/state`; no comparten estado mutable entre
particiones. InputBatcher fue la primera integración. Cada artículo confirma conjuntamente su
inbox y el snapshot del batch parcial; cuando una transición completa un batch o
recibe `INPUT_END`, las salidas quedan en outbox antes de publicarse. Al reiniciar,
la partición reconstruye el batch y publica lo pendiente sin reaplicar la entrada.
Después del ACK se compacta la inbox conservando los IDs del batch parcial y el
último ID confirmado, necesario ante la posible pérdida física de ese ACK. Compose
monta un volumen independiente en `/state` para cada una de las tres particiones.

Las pruebas de fallos de esta primera integración cubren cinco ventanas concretas:
antes del commit durable se revierte el estado en memoria y la reentrega lo aplica
una vez; después del commit y antes de publicar, el reinicio drena la outbox; si la
caída ocurre a mitad del fan-out entre el batch y `INPUT_END`, solo se recupera la
salida que no recibió confirmación; si las salidas fueron confirmadas pero se pierde
el ACK de consumo, la reentrega no vuelve a publicarlas; y un fallo al compactar
después del ACK conserva información redundante, pero no pierde ni reaplica estado.

#### Bloque 3 - Colas de control recuperables

- [x] Mantener las colas DATA disponibles cuando cae su consumidor.
- [x] Convertir el control a colas nombradas por réplica, no exclusivas ni
  autoeliminables.
- [ ] Verificar que un proceso reiniciado redeclare la topología y consuma DATA y
  controles pendientes.
- [x] No agregar volumen ni recuperación de RabbitMQ: el broker no cae en el modelo.

Cada consumidor coordinado declara `<control_exchange>.partition-<partition_id>`.
La identidad proviene del `PARTITION_ID` ya estable de Compose, por lo que un
reinicio vuelve a enlazar la misma cola al exchange `direct`. Las colas continúan
con `durable=false`: sobreviven a la caída del consumidor, no a la del broker,
que está excluida del modelo. Una prueba con RabbitMQ real cerró el primer
consumidor, publicó un control sin consumidores activos y comprobó que una nueva
conexión recuperó el mensaje al redeclarar el mismo nombre. Aún queda probar el
reinicio de un componente completo junto con sus mensajes DATA y su estado local.

`CoordinatedConsumer` tampoco confirma un control al copiarlo a su cola local. El
thread de RabbitMQ espera a que el thread principal termine
`process_control_message` y recién entonces envía el ACK desde la conexión que le
pertenece. Una excepción o un shutdown anterior a la aplicación produce NACK con
reencolado. Todavía puede existir una reentrega si el efecto termina pero el ACK se
pierde; la inbox/outbox de cada controller hace idempotente esa ventana.

#### Bloque 4 - Recuperar el pipeline por etapas

- [x] InputBatcher: batch parcial, IDs ordenados de origen, inbox y outbox.
- [x] Grupos coordinados: finales observados y finales ya emitidos.
  - [x] QueryRouter: barrera de Preprocessor y fan-out final durable.
  - [x] Until2020Filter, WordExtractor, Q1, Q2 y proyectores restantes.
- [x] Q3WordAggregator: palabras, idiomas, resultados emitidos y barrera.
- [x] Q4WordCounter y Q4Top20Reducer: conteos, parciales, top y finales.
- [x] Q5StatsWorker y Q5AverageReducer: suma, cantidad, promedio, parciales y finales.
- [x] Q5Comparator: candidatos, promedio, finales, resultados emitidos y buffer completo.
- [x] ResultSink: deduplicación, barreras, resultados/finales y salida durable.
- [x] Cubrir las ventanas antes/después de persistir, publicar y confirmar mediante
  inyección unitaria; reiniciar todos los tipos de etapa durante una ejecución real
  y comparar Q1-Q5.

Preprocessor no incorpora un journal por batch: su normalización es stateless y
determinista, por lo que una entrega reintentada puede reprocesarse. Persistir cada
batch agregaría un `fsync` sin impedir que RabbitMQ entregue el reintento a otra
réplica. La identidad estable de la salida permite que la siguiente etapa la
deduplique. Sí se conserva el control sin ACK hasta que el callback termina.

QueryRouter fue el primer grupo coordinado integrado con persistencia. Cada partición
guarda los IDs de Preprocessor ya observados, `reply_queue`, el estado de finalización
y las dos salidas de cierre en una transición atómica. Sus tres réplicas montan
volúmenes separados. Al recuperar una barrera incompleta continúa desde los finales
confirmados; si cayó a mitad del fan-out, publica solamente el destino todavía
pendiente. Una reentrega posterior a ambos publisher confirms no vuelve a emitir el
final.

El mismo `DurableBarrier` se propagó a Until2020Filter, WordExtractor,
Q1EnglishFilter, Q2BiographyFilter, ImageFetcher, Q3WordProjector,
Q4Q5WordProjector y Q5DateRouter. Cada réplica conserva los finales de origen y el
fan-out pendiente; al reiniciar drena únicamente las copias que no habían recibido
publisher confirm.

Las etapas stateful usan el mismo commit `inbox + state + outbox`: Q3 conserva la
presencia por idioma y la marca `emitted`; Q4 conserva conteos, tops parciales y el
top global; Q5 conserva suma/cantidad, promedio, candidatos, finales y resultados.
El fan-out del promedio se registra por destino, de modo que una caída entre dos
comparadores no obliga a repetir los destinos confirmados. ResultSink persiste sus
barreras y toda salida hacia `results.<execution_id>` antes de publicarla.

El cliente sincroniza cada línea y registra su `result_id` antes de responder el
ACK. Dentro de la sesión, una redelivery con el mismo ID se confirma sin volver a
escribir. No se promete reanudar un cliente caído: ese caso está fuera del modelo
de recuperación y se valida por liberación de recursos.

Las pruebas unitarias de recuperación cubren una barrera incompleta, caída antes de
publicar, fan-out parcialmente confirmado y redelivery al cliente. La validación
integral utilizó una muestra equilibrada de 300 artículos —150 de cada extremo del
dataset de 50.000— y detuvo inicialmente InputBatcher para acumular toda la entrada.
Después de iniciar el procesamiento se enviaron `SIGKILL` simultáneos a todas las
réplicas del pipeline y a ResultSink, manteniendo Gateway y RabbitMQ activos. Los
containers se iniciaron manualmente para aislar la recuperación de datos del futuro
supervisor. El cliente recibió los cinco cierres y 175 resultados: 7 de Q1, 0 de
Q2, 86 de Q3, 20 de Q4 y 62 de Q5. Los cinco archivos coincidieron con la referencia,
RabbitMQ quedó sin mensajes ready/unacked y todos los procesos volvieron a estar
activos. Con esta evidencia queda cerrado el bloque 4; la detección y el reinicio
automáticos pertenecen al bloque 5.

Un intento previo con los 50.000 artículos mostró que reescribir el snapshot
completo y ejecutar `fsync` por cada entrada de InputBatcher drenaba demasiado
lentamente para un smoke de fallos. InputBatcher ahora agrega cada artículo que no
cierra un batch a `pending-<generation>.jsonl` y lo sincroniza antes del ACK. Sólo
al cerrar un batch crea un checkpoint atómico con la nueva generación, el siguiente
batch parcial y la outbox; luego elimina el journal anterior. Así el volumen escrito
por artículo pasa de crecer con el batch parcial a ser proporcional al artículo.
El benchmark final debe medir la mejora y el costo que todavía conserva el `fsync`
por entrada.

#### Bloque 5 - Supervisión, consenso y reinicio

- [x] Agregar endpoint de salud e `incarnation_id` a todos los procesos alcanzados.
- [x] Hacer que la salud agregue el estado del loop principal y threads críticos,
  y probar una falla interna que deje vivo al proceso.
- [x] Implementar protocolo entre FailureManagers y transporte sin bibliotecas de consenso.
- [x] Implementar Bully con `ELECTION`, `OK`, `COORDINATOR`, prioridad estable,
  epoch y reemplazo del coordinador caído.
- [x] Implementar detección directa `PING`/`PONG` desde el coordinador y política
  configurable de timeout/reintentos, sin votación.
- [x] Implementar ejecutor Docker limitado a reinicios estáticos e idempotentes.
- [x] Probar caída de un supervisor, coordinador, varios workers simultáneos,
  sospecha falsa y mensajes viejos de otra elección.
- [x] Probar que Chaos Monkey nunca deja cero FailureManagers vivos.

La validación Docker del 26/11/2026 levantó las 56 unidades de Compose y dejó
que Bully eligiera a `failure-manager-2` como coordinador del epoch 1. Luego se
enviaron `SIGKILL` simultáneos a `failure-manager-2`, `failure-manager-1` y
`q1-0`. El único supervisor restante, `failure-manager-0`, detectó la pérdida,
ganó el epoch 2 y reinició los tres containers mediante la operación estática de
Docker. Al regresar, el nodo 2 ganó el epoch 3 y el nodo 0 abandonó el liderazgo.
La prueba terminó con los 56 servicios activos y sin errores de supervisión.

#### Bloque 6 - Exactly-once en la frontera externa

- [x] Incorporar `result_id` determinista a resultados y finales.
- [x] Recuperar y deduplicar en ResultSink los mensajes ya aplicados y sus salidas.
- [x] Persistir en el cliente los `result_id` aplicados y escribir cada resultado
  de forma recuperable antes de enviar ACK.
- [x] Mantener Gateway como relay sin RecoveryStore ni estado de negocio nuevo.
- [x] Probar con procesos reales la caída de cliente y ResultSink. La pérdida del
  ACK después de escribir y la redelivery duplicada también están cubiertas
  unitariamente. No se inyectan caídas del Gateway.

#### Bloque 7 - Fallo terminal y cleanup distribuido

- [x] Implementar protocolo de éxito, aborto, timeout y cleanup idempotente.
- [x] Limpiar estado en memoria, archivos, volúmenes por ejecución y colas temporales.
- [x] Evitar recreación por mensajes tardíos mediante tombstones acotados.
- [x] Probar limpieza tanto de consultas resueltas como fallidas.
- [x] Interrumpir un cliente en un escenario separado y verificar que se liberen
  todos sus recursos aunque no forme parte de la demo completa.

##### Cierre y distribución

1. Antes de publicar artículos, Gateway envía `EXECUTION_STARTED` con el
   `execution_id` y el vencimiento. RabbitMQ confirma la publicación y
   ResultSink guarda el registro activo en disco antes de confirmar el consumo.
   Gateway publica `EXECUTION_COMPLETED` después de recibir el ACK del cliente
   para cada uno de los cinco `QUERY_END`. Si detecta una desconexión, un error
   o un vencimiento, publica `EXECUTION_FAILED` con el `execution_id` y la
   causa. Al cerrar la sesión se libera la cola exclusiva
   `results.<execution_id>`.
2. ResultSink recibe el evento terminal en su cola de entrada. Antes de borrar
   su propio estado, registra en disco los destinos pendientes de
   `EXECUTION_CLEANUP`. Usa el mismo ID lógico de limpieza para todos los
   destinos y quita cada publicación de la outbox después de que RabbitMQ la
   confirma. Si ResultSink se reinicia, recupera y publica las órdenes
   pendientes antes de volver a consumir. También recupera las ejecuciones
   activas. Si pasan cinco segundos desde el vencimiento sin un cierre
   procesado, encola un `EXECUTION_FAILED` de respaldo en su propia cola.
   Así, un evento terminal que ya estaba en cola se procesa primero.
   El registro activo se elimina después de guardar la orden de limpieza y
   cerrar el estado local.
3. La orden llega a la partición de InputBatcher que recibió la entrada, a las
   colas de control de las etapas coordinadas y a las colas de entrada
   dedicadas de los agregadores, reducers y comparadores. Compose configura
   esos destinos en `CLEANUP_QUEUES`. Cada réplica confirma su orden después de
   aplicarla. La confirmación de publicación de RabbitMQ acredita la entrega
   a la cola, mientras que el ACK del consumidor acredita el borrado local.

Gateway usa `EXECUTION_TIMEOUT_SECONDS` (3600 por defecto) como plazo total
desde que empieza a atender la sesión. El plazo cubre la carga, la espera de
resultados y los ACK del cliente. Durante la espera, Gateway también detecta
el cierre del socket antes del primer resultado. Preprocessor no tiene estado
durable por ejecución y puede terminar trabajo que ya estaba en tránsito. Las
etapas posteriores descartan sus salidas una vez aplicada la orden de limpieza.

##### Limpieza, recuperación y mensajes tardíos

Cada réplica guarda primero un *tombstone* durable. Luego elimina el estado en
memoria y el directorio de la ejecución dentro de su volumen, incluidos inbox,
outbox y snapshot. Q5 también elimina el archivo de candidatos. Si la réplica
cae a mitad del borrado, al arrancar termina la limpieza antes de recuperar
estado activo o publicar desde sus outboxes. Una orden repetida es idempotente.
Los mensajes tardíos de una ejecución cerrada se confirman y descartan.

Las ejecuciones nuevas incluyen en su ID un límite para trabajo tardío,
calculado como el plazo de Gateway más una hora. Una réplica puede retirar la
marca cuando hayan pasado tanto ese límite como una hora desde el borrado
local. La revisión corre al arrancar y durante el consumo. Antes de retirar
una marca, la réplica persiste un límite de rechazo que también se aplica al
recuperar outboxes. Los IDs anteriores, sin fecha en el identificador,
conservan sus marcas hasta que se pueda verificar el drenaje.

El escenario reproducible de aceptación se ejecuta con:

```bash
make cleanup-smoke
```

El runner levanta una topología Compose aislada, publica artículos y espera a
que InputBatcher persista estado. Luego corta el socket sin enviar `INPUT_END`.
La prueba sólo pasa si todas las réplicas alcanzadas eliminan el directorio de
la ejecución y guardan su tombstone, desaparecen la cola exclusiva
`results.<execution_id>` y el buffer de Q5, y RabbitMQ queda sin mensajes
`ready` ni `unacked`. Finalmente elimina containers y volúmenes del escenario.
La corrida del 24/11/2026 pasó sobre las 46 réplicas alcanzadas por el cleanup.

#### Bloque 8 - Chaos Monkey, benchmark, informe y demo

**Implementado y verificado**

- [x] Implementar Chaos Monkey reproducible y el monitor del log de inyecciones.
- [x] Automatizar la matriz por tipo de servicio y el barrido de intervalos,
  conservando la evidencia de cada intento.
- [x] Automatizar la comparación de Q1–Q5 y la auditoría de cleanup antes de
  bajar Compose. Verificar ambas con caídas reales de InputBatcher y
  ResultSink y reinicio manual.
- [x] Ejecutar la matriz exhaustiva con reinicio automático: veinte casos,
  tres corridas por caso, resultados iguales a la referencia y cleanup completo.
- [ ] Repetir benchmark sin fallas con tolerancia habilitada y calcular overhead contra el bloque 0.
- [ ] Medir por separado el costo y tiempo de recuperación bajo fallas.
- [ ] Actualizar `INFORME.md` y diagramas de robustez, despliegue, actividades, paquetes, secuencia y DAG.
- [ ] Ensayar la demo completa con fallas periódicas y conservar logs de evidencia.

**Pendiente**

- [x] Extender la matriz representativa a todos los tipos de proceso.
- [ ] Completar los casos dirigidos que requieran una falla en un punto exacto.
- [x] Ejecutar el barrido desde cinco segundos y registrar el menor intervalo
  probado que completa varias corridas correctas: 1 segundo en el barrido mixto
  sobre 50.000 artículos.
- [ ] Medir el overhead a escala con al menos tres corridas válidas por
  versión, sin fallas y con la misma configuración. Recuperar el Parquet
  original de 612.475 artículos o volver a medir ambas versiones sobre la
  reconstrucción, cuyo SHA-256 difiere del histórico.

##### Validación representativa del 26/11/2026

La primera ejecución de la matriz descubrió una carrera del entorno de prueba.
`docker compose up --wait` sólo esperaba el healthcheck de RabbitMQ; los demás
containers se consideraban disponibles apenas estaban `running`. El cliente podía
publicar los finales antes de que todas las réplicas hubieran declarado y enlazado
sus colas de control. Los datos quedaban en las colas normales, pero un control
publicado a un exchange sin su binding todavía creado podía perderse y dejar Q4 o
Q5 esperando indefinidamente.

El runner ahora consulta el endpoint de salud interno de cada proceso supervisado
y exige `OK` antes de iniciar cliente y Chaos Monkey. Ese estado se alcanza después
de construir las conexiones y bindings de RabbitMQ; en la prueba aislada fueron 54
componentes listos en 3,67 segundos. Con esta espera, ResultSink soportó dos
`SIGKILL`, fue repuesto automáticamente y entregó Q1=7, Q2=0, Q3=86, Q4=20 y
Q5=62, iguales a la referencia y sin recursos pendientes.

La matriz representativa cubrió `preprocessor`, `input-batcher`, `query-router`,
`q3-word-aggregator`, `q4-top20-reducer`, `q5-comparator`, `result-sink`, una caída
simultánea de Preprocessor y QueryRouter, y un FailureManager. Fueron 27 corridas:
todas terminaron, coincidieron con la referencia y pasaron la auditoría de cleanup.
Cada corrida confirmó al menos una caída real; 23 confirmaron dos, mientras que los
tres casos simultáneos confirmaron un ciclo con ambos workers y una corrida corta
de Q3 finalizó después del primer ciclo. Por eso la matriz exige una caída por
corrida, mientras el barrido de frecuencia conserva el requisito más estricto de
al menos dos ciclos.

##### Barrido de frecuencia del 26/11/2026

El barrido usó `wikipedia_sample.parquet` (50.000 artículos, SHA-256
`6e403dcd4e26b8551518cd9422dc661220505dbefb809f433fa033eda46552fb`) y la
referencia de 22.650 resultados ya validada. El calendario mezcló
`input-batcher-0`, `query-router-0`, `result-sink` y `failure-manager-2`, con una
caída por ciclo y seed 42. Así se ejercitaron estado parcial, barreras, entrega
externa y elecciones Bully sin usar el dataset de performance de 612.475 artículos
para nueve corridas largas.

| Intervalo | Corridas | Caídas confirmadas | Mediana | Desvío muestral | Primer resultado mediano |
|---:|---:|---:|---:|---:|---:|
| 5 s | 3/3 | 6, 6, 7 | 696,60 s | 63,43 s | 18,40 s |
| 2,5 s | 3/3 | 5, 5, 5 | 730,49 s | 15,60 s | 23,08 s |
| 1 s | 3/3 | 3, 3, 3 | 732,81 s | 22,05 s | 20,62 s |

Las nueve ejecuciones coincidieron con Q1–Q5 de referencia y pasaron la auditoría
de cleanup. El menor intervalo **probado** y aprobado fue un segundo; no se afirma
que sea el límite absoluto porque no se ensayaron intervalos menores. A menor
intervalo hubo menos caídas efectivamente confirmadas: varios intentos encontraron
el target todavía detenido o reiniciándose y fueron omitidos. Cada corrida conservó
al menos tres ciclos confirmados, por encima del mínimo de dos exigido por el
barrido. La diferencia de medianas no es estrictamente monótona y debe leerse con
su dispersión; este experimento caracteriza recuperación, no reemplaza el benchmark
de overhead sin fallas.

##### Matriz exhaustiva del 26/11/2026

Después de validar los casos representativos se ejecutó `--scope all` sobre el
dataset equilibrado de 300 artículos. La matriz cubrió individualmente
ImageFetcher, InputBatcher, Preprocessor, Q1, Q2BiographyFilter,
Q3WordAggregator, Q3WordProjector, Q4Q5WordProjector, Q4Top20Reducer,
Q4WordCounter, Q5AverageReducer, Q5Comparator, Q5DateRouter, Q5StatsWorker,
QueryRouter, ResultSink, Until2020Filter y WordExtractor. Agregó además una caída
simultánea de Preprocessor y QueryRouter y la caída de un FailureManager.

Los 20 escenarios pasaron sus tres corridas: 60/60 ejecuciones coincidieron con
los cinco archivos de referencia y aprobaron cleanup, sin casos pendientes. Se
confirmaron 117 ciclos de caída. Los 18 casos funcionales individuales y el de
FailureManager confirmaron dos ciclos por corrida; el caso simultáneo confirmó en
cada repetición un ciclo que derribó ambos workers a la vez. Esta matriz completa
la cobertura por tipo. Las ventanas exactas entre persistencia, publicación y ACK
continúan cubiertas por pruebas dirigidas y unitarias, no por temporización
probabilística del Chaos Monkey.

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
| Caída del coordinador | Bully elige otro y las propuestas anteriores no se ejecutan dos veces |
| Caída de uno o dos FailureManagers | Al menos uno permanece vivo, asume como coordinador y reinicia a los demás |
| Caídas simultáneas de workers | El coordinador detecta y reinicia cada target sin bloquear las demás recuperaciones |
| Desconexión de cliente | La ejecución libera colas, archivos y estado temporal aunque el cliente no complete |
| Ejecución fallida | Cliente informado y recursos eliminados |
| Chaos Monkey cada cinco segundos | Cinco salidas iguales a referencia, sin duplicados ni recursos huérfanos |

Para listar los escenarios de caída de procesos sin levantar Compose:

```sh
uv run python -m scripts.chaos matrix --scope representative --list
uv run python -m scripts.chaos matrix --scope all --list
```

Para ejecutarlos, reemplazar `--list` por `--dataset`, `--expected-output`,
`--output` y `--timeout-seconds` con sus valores. La matriz ejecuta tres
ejecuciones por caso a intervalos de cinco segundos por defecto y guarda
`matrix.json`, los logs y los resultados, incluidos los casos fallidos o
pendientes. `--scope all` elige una réplica por tipo de servicio. Los casos
en puntos específicos de persistencia, barrera o ACK requieren pruebas
dirigidas adicionales.

### Comparación de performance

Con tres o más ejecuciones válidas por caso, calcular la variación de las medianas:

```sh
.venv/bin/python -m scripts.compare_benchmarks \
  ruta/baseline/summary.json ruta/sin-fallas/summary.json \
  --faults ruta/con-fallas/summary.json
```

`--faults` es opcional. La ejecución sin fallas debe incluir verificación contra la
referencia serial mediante `--expected-output`. El comparador exige el mismo
SHA-256 del dataset y contrasta Q1–Q5 de cada ejecución exitosa. El JSON de salida
incluye ejecuciones fallidas y separa la variación con tolerancia habilitada de la
variación bajo fallas.

La comparación debe separar el overhead normal de la recuperación:

- **Baseline:** commit final de escalabilidad, sin fallas.
- **Tolerancia habilitada:** misma muestra, topología, host y configuración, sin fallas.
- **Con fallas:** misma configuración, seed y calendario de Chaos Monkey
  documentados; comenzar con intervalos de cinco segundos.
- Ejecutar al menos tres corridas por caso y reportar mediana y dispersión, no una
  única medición.
- El runner registra `first_result_seconds` desde el inicio del cliente hasta
  que persiste el primer resultado, incluido su ID. Queda vacío si no hay
  resultados o si la imagen del cliente no emite esa medición. `summary.json`
  agrega mediana, desvío estándar muestral y cantidad de mediciones de las
  ejecuciones válidas para duración, throughput, tiempo al primer resultado y
  bytes de red recibidos y transmitidos. El total suma los contadores de los
  contenedores e incluye tráfico interno. Cuando una muestra detecta que un
  contador caído, el runner acumula el tramo nuevo. Un reinicio entre muestras
  puede subestimar el total. Para ejecuciones sin fallas, los resúmenes anteriores
  aportan sus contadores finales.
- El runner suma CPU y memoria de los contenedores en cada muestra completa
  de Docker. `stack_metrics` guarda CPU media y máxima, memoria máxima y el
  número de muestras usadas. El comparador contrasta las medianas de estas
  métricas entre ejecuciones. CPU puede superar el 100 % al usar más de un
  núcleo. Para resúmenes anteriores, el comparador calcula estas métricas si
  los archivos originales `run-*/metrics.jsonl` siguen disponibles.
- `input_batcher_journal_bytes` suma el tamaño observado de cada journal
  incremental de InputBatcher al retirarlo durante un checkpoint, recuperación
  o cleanup. Usa el mayor tamaño por ejecución, partición y generación para
  deduplicar reintentos. Requiere logs de InputBatcher a nivel INFO. Mide bytes
  lógicos del journal y puede omitir un archivo si la ejecución termina antes de
  que se registre su tamaño. El campo queda vacío cuando no hay muestras.
- Medir tiempo total, artículos por segundo, tiempo hasta primer resultado, CPU,
  memoria, red, bytes escritos en journals y tiempo de recuperación.
- Reducir progresivamente el intervalo entre fallas y documentar el menor valor
  que completa correctamente varias corridas. El objetivo es medir el límite, no
  ocultar una ejecución fallida.
- Verificar Q1-Q5 en todas las corridas; una ejecución rápida pero incorrecta no
  forma parte de la comparación.

Para barrer intervalos cuando esté disponible el reinicio automático:

```sh
uv run python -m scripts.chaos sweep \
  --dataset datasets/wikipedia_benchmark_12.parquet \
  --expected-output output/reference-benchmark \
  --output benchmarks/results/interval-sweep \
  --timeout-seconds 1800 --target preprocessor-0 \
  --interval 5 --interval 2.5 --interval 1
```

Cada intervalo conserva los logs y los
artefactos del benchmark. `sweep.json` registra todos los intentos y el menor
intervalo probado que pasó. Para pasar, las tres ejecuciones deben coincidir
con la referencia, verificar el cleanup y confirmar inyecciones en al menos
dos ciclos distintos.

### Decisiones confirmadas con el corrector

| Tema | Decisión |
|---|---|
| Consenso | Bully es suficiente: el valor consensuado es quién será el coordinador. No agregar votación para decidir si un nodo funciona |
| Simultaneidad | Puede haber caídas simultáneas; el algoritmo no debe depender de que sean secuenciales |
| Red | No contemplar particiones; asumir comunicación entre los nodos y los FailureManagers |
| Gateway | No cae y debe mantenerse mínimo |
| RabbitMQ | No cae y queda fuera de Chaos Monkey |
| Cliente | No cae durante la demo completa; probar aparte que una desconexión libera recursos |
| FailureManagers | Pueden caer, pero el inyector debe dejar siempre al menos uno vivo |
| Persistencia | Escritura directa a disco es suficiente. No se propone una biblioteca externa; se usará la biblioteca estándar |
| Docker | Puede ejecutar reinicios, pero nunca detectar disponibilidad ni tomar decisiones |
| Exactly-once | IDs y deduplicación del efecto observable sobre transporte at-least-once son aceptables |
| Gateway actual | Sus responsabilidades actuales son suficientemente mínimas |
| Frecuencia de fallas | Comenzar con una cada cinco segundos y reducir el intervalo para medir el límite del sistema |

### Definition of Done del hito

El hito no está terminado hasta que, desde un checkout limpio, un comando
documentado pueda levantar el sistema, ejecutar una muestra significativa con
fallas periódicas, recuperar automáticamente los procesos, obtener los cinco
archivos idénticos a la referencia sin duplicados, demostrar las decisiones de
elección de coordinador, verificar la limpieza de todas las ejecuciones y generar la evidencia
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

No se debe asumir orden entre una cola de datos y una cola o exchange de control diferentes. Por eso el primer mensaje de fin atraviesa la misma cola que los datos antes de ser difundido. `MessageMiddlewareExchangeRabbitMQ` implementa el exchange `direct`, una cola estable por consumidor y el modo publicador sin cola. El patrón está integrado en los grupos de tres Preprocessors, QueryRouters, Until2020Filters, Q1EnglishFilters, Q2BiographyFilters, ImageFetchers, WordExtractors, Q3WordProjectors, Q4Q5WordProjectors y Q5DateRouters. Las etapas stateful posteriores completan barreras sobre los finales de esas réplicas.

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

`MessageMiddlewareExchangeRabbitMQ` adapta la implementación del TP de Coordinación. Utiliza un exchange `direct`: cada réplica consumidora declara una cola nombrada `<exchange>.partition-<id>`, no exclusiva ni autoeliminable, y la vincula con las routing keys del grupo; una instancia publicadora puede omitir la creación de cola. Varias colas vinculadas con la misma clave reciben una copia de cada control. Las conexiones de publicación y consumo permanecen separadas porque `BlockingConnection.start_consuming()` bloquea su thread.

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

RabbitMQ queda fuera del modelo de caídas confirmado con el corrector. Por eso no
es necesario hacer durable el broker, publicar mensajes persistentes ni agregarle
un volumen para este hito. La recuperación se concentra en las caídas de los
consumidores y en el estado local de cada proceso.

Las colas DATA actuales no son exclusivas ni autoeliminables: permanecen en el
broker estable cuando un consumidor cae y RabbitMQ reencola sus mensajes sin ACK.
Las colas de control también son ahora nombradas por réplica, no exclusivas ni
autoeliminables, para conservar los controles mientras la réplica se reinicia.

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

1. Registrar el modelo de fallos y las decisiones ya confirmadas con el corrector.
2. Congelar y medir la línea base de escalabilidad.
3. Introducir identidad determinista y pruebas de puntos de caída.
4. Implementar persistencia local simple, inbox y outbox.
5. Hacer que las colas de control sobrevivan al reinicio de sus consumidores.
6. Recuperar el pipeline por etapas.
7. Implementar supervisión, consenso y reinicio.
8. Garantizar exactly-once observable en ResultSink y Client sin cargar estado de
   negocio en Gateway.
9. Completar cleanup, Chaos Monkey, benchmark, informe y demo.

## Próximos pasos

1. Medir en el benchmark final el costo del journal incremental y del `fsync` por
   artículo frente al baseline previo a tolerancia.
2. Completar los casos dirigidos de ventanas exactas que no dependen de la
   temporización del Chaos Monkey.
3. Actualizar los diagramas y ensayar el guion completo de la demo.

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
  coordinador y el reinicio decidido por ese coordinador.
- Ejecutar Chaos Monkey con una seed conocida y mostrar la caída abrupta, el
  reinicio y la recuperación de progreso.
- Perder un ACK de resultado y comprobar que la reentrega no duplica líneas. No se
  inyectan caídas del Gateway porque está excluido del modelo de fallos acordado.
- Finalizar comparando Q1-Q5 y verificando que no queden journals, buffers, colas
  temporales ni estado activo de la ejecución.

La concurrencia de clientes y la desconexión durante el upload ya están
implementadas. Debe automatizarse la verificación de cleanup del cliente
desconectado y completar la recuperación ante caídas internas.

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
