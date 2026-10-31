# Wikipedia Article Analysis

Trabajo práctico grupal de Sistemas Distribuidos. El sistema procesa artículos de Wikipedia en inglés y francés y genera los resultados de cinco consultas mediante una arquitectura distribuida basada en RabbitMQ.

> El proyecto se encuentra en desarrollo. Actualmente están implementadas la ejecución serial de referencia, la lectura del dataset, el cliente, el Gateway, el protocolo externo, la capa común de middleware, el InputBatcher, el Preprocessor, el QueryRouter, las ramas funcionales de Q1, Q2, Q3 y Q4, ResultSink, la proyección compartida de Q4/Q5 y Q5DateRouter. El cálculo del promedio y la comparación de Q5 todavía están pendientes.

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
| InputBatcher con batches limitados por bytes | Implementado |
| Validación y normalización de artículos en Preprocessor | Implementado |
| Fan-out y proyecciones mínimas en QueryRouter | Implementado |
| Filtro temporal compartido de Q1 y Q2 | Implementado con una réplica |
| Filtro de idioma y resultados de Q1 | Implementado con una réplica |
| Filtro de secciones biográficas de Q2 | Implementado con una réplica |
| Descarga de imágenes y resultados de Q2 | Implementado secuencialmente con una réplica |
| Tokenización compartida en WordExtractor | Implementado con una réplica |
| Proyección y agregación de palabras compartidas de Q3 | Implementado con una partición |
| Proyección compartida de Q4/Q5 | Implementado con una réplica |
| Agregación y resultados de Q4 | Implementado con contador y reducer |
| Separación temporal de Q5 en Q5DateRouter | Implementado con una réplica |
| Estadísticas, promedio y comparación de Q5 | Pendiente |
| ResultSink | Implementado para ramas con una sola instancia |
| Atención concurrente de varios clientes | Pendiente |
| Graceful shutdown completo para procesos concurrentes | Pendiente |
| Estrategia de tolerancia a fallos y persistencia | Pendiente |

### Limitación actual de clientes

El Gateway actualmente atiende una única conexión por vez. Una ejecución debe finalizar antes de que el servidor comience a atender al siguiente cliente.

La arquitectura objetivo requiere ejecuciones aisladas y concurrentes entre varios clientes. Para alcanzarla, el servidor deberá ejecutar cada sesión en un thread o proceso independiente, mantener separados sus recursos y realizar el cierre ordenado de sockets y tareas activas.

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
      | RESULT / QUERY_END provisional| ImageCandidateBatch
      |                               v
      |                       RabbitMQ: image_fetcher.input
      |                               |
      |                               v
      |                          ImageFetcher
      |                               |
      |       RESULT / QUERY_END provisional
      |_______________________________|
                      |
                      v
          RabbitMQ: result_sink.input
                      |
                      v
              ResultSink

      ... etapas posteriores pendientes ...

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

El `InputBatcher` consume los artículos, construye batches de hasta `MAX_BATCH_BYTES` y los publica en `preprocessor.input` antes del mensaje de fin. El `Preprocessor` valida y normaliza esos artículos y publica los batches resultantes en `query_router.input`. El `QueryRouter` proyecta cada batch hacia `until_2020.input` y `word_extractor.input`. `Until2020Filter` consume la primera rama, conserva los artículos hasta 2020 inclusive y publica las proyecciones de Q1 y Q2. Q1 conserva los artículos en inglés y Q2 filtra las secciones biográficas, descarga las imágenes disponibles y codifica sus bytes en Base64. Q1, Q2, Q3 y Q4 publican resultados y finales en `result_sink.input`. La rama textual implementada se detalla a continuación. Como las etapas posteriores de Q5 todavía no están implementadas, el cliente no puede completar la ejecución de las cinco consultas sin intervención manual.

La rama textual implementada continúa desde `word_extractor.input`:

```text
WordExtractor
    | TokenizedArticleBatchMessage
    |-------------------------------------|
    v                                     v
q3_word_projector.input         q4_q5_word_projector.input
    |                                     |
Q3WordProjector                    Q4Q5WordProjector
    |                                     |-------------------------------|
    | Q3WordPresenceBatchMessage          | Q4WordBatchMessage            | Q5ArticleWordCountBatchMessage
    v                                     v                               v
q3_word_aggregator.input.0        q4_word_counter.input.0          q5_date_router.input
    |                                     |                               |
Q3WordAggregator                    Q4WordCounter                    Q5DateRouter
    | RESULT / QUERY_END provisional
    v
result_sink.input -> ResultSink
```

`Q4Q5WordProjector` consume la salida tokenizada y publica las proyecciones
específicas de Q4 y Q5. La rama Q4 continúa por un único `Q4WordCounter` y
`Q4Top20Reducer` hasta `result_sink.input`. Q5DateRouter separa histórico y
candidatos; ambas salidas se acumulan hasta implementar sus consumidores.

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

- Acepta una ejecución del cliente.
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

La primera implementación utiliza una única réplica de la etapa siguiente. De esta manera, los batches y el mensaje de fin se consumen en el mismo orden en que fueron publicados. No se habilitarán múltiples réplicas consumidoras hasta implementar el protocolo de control y las barreras descritos en la sección de mensajes de finalización.

InputBatcher se escala mediante particiones: todos los mensajes de una ejecución se asignan a una única cola `input_batcher.input.<partition>`, con un consumidor activo por partición lógica. No se deben colocar consumidores competidores sobre una misma partición porque dividirían entre procesos el batch pendiente de una ejecución.

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

Recibe `ArticleBatchMessage`, publica `NormalizedArticleBatchMessage` y reenvía el mensaje de fin después de los batches anteriores. No aplica reglas específicas de ninguna query.

La implementación actual utiliza una sola réplica. La normalización de cada batch es independiente y permite escalar horizontalmente la etapa, pero antes de habilitar consumidores competidores se debe implementar una barrera que impida publicar el fin mientras otra réplica todavía procesa un batch.

### QueryRouter

El QueryRouter realiza el fan-out hacia dos ramas:

```text
Q1/Q2
Q3/Q4/Q5
```

Publica solamente los campos requeridos por cada rama y no realiza filtros propios de las consultas.

`until_2020.input` recibe `Q1Q2ArticleBatchMessage`. Cada artículo contiene `name`, `url`, `language`, `modification_date`, `sections` e `image_url`.

`word_extractor.input` recibe `TextArticleBatchMessage`. Cada artículo contiene `article_id`, `url`, `language`, `modification_date` y `abstract`. Mientras el dataset no provea un identificador independiente, `article_id` utiliza la URL como identidad estable.

El mensaje de fin se publica en ambas salidas después de los batches anteriores. La implementación actual utiliza una sola réplica hasta incorporar el protocolo de control y las barreras.

El fan-out tiene semántica at-least-once. El mensaje de entrada se confirma solamente después de publicar ambas salidas. Si la primera publicación funciona y la segunda falla, se rechaza la entrada y una reentrega puede duplicar el mensaje en la primera rama. La idempotencia de las etapas posteriores queda pendiente para el hito de tolerancia a fallos.

### Rama Q1/Q2

```text
QueryRouter
    |
Until2020Filter
    |-------------------|
    v                   v
Q1EnglishFilter   Q2BiographyFilter
                        |
                        v
                   ImageFetcher
```

`Until2020Filter` aplica una sola vez la condición `fecha <= 2020`.

Consume `Q1Q2ArticleBatchMessage` desde `until_2020.input`. Por cada batch que contenga artículos hasta 2020 publica:

- `Q1CandidateBatchMessage` en `q1_english_filter.input`, con `url`, `language` y `modification_date`.
- `Q2CandidateBatchMessage` en `q2_biography_filter.input`, con `name`, `url`, `sections` e `image_url`.

Los batches vacíos no se publican. El `RoutedInputEndMessage` se reenvía siempre a ambas ramas para que puedan finalizar incluso cuando una ejecución no tenga candidatos. Actualmente se ejecuta una sola réplica: antes de escalarla se debe reemplazar el fin simple por el protocolo de control y barrera, ya que una réplica podría reenviar el final mientras otra todavía procesa un batch anterior.

- Q1 consume `Q1CandidateBatchMessage`, conserva los artículos cuyo idioma normalizado es `en` y publica un `ResultMessage` por artículo en `result_sink.input`. El registro contiene `url` y `modification_date` en el mismo formato ISO que la referencia serial.
- `Q2BiographyFilter` consume `Q2CandidateBatchMessage`, busca una sección cuyo nombre completo sea `biography` o `biographie` ignorando mayúsculas y publica `ImageCandidateBatchMessage` en `image_fetcher.input`. La proyección contiene `name`, `url` e `image_url`; una URL ausente se conserva para que ImageFetcher registre y omita el candidato de forma explícita.
- `ImageFetcher` utiliza `image.content_url` directamente, descarga con timeout y reintentos acotados para 429, 5xx y errores transitorios de red. Los 404 y demás 4xx se registran y no se reintentan. Si falta la URL o la descarga falla definitivamente, registra la omisión y continúa. Los bytes exitosos se codifican en Base64 y se publican como `ResultMessage(query_id=2)` en `result_sink.input`.

En la primera iteración funcional, una única instancia de Q1 y una única instancia de ImageFetcher transforman `RoutedInputEndMessage` en los `QueryEndMessage` de Q1 y Q2. Estos finales son provisionales. En la iteración de escalabilidad del mismo trabajo, cada réplica emitirá `PARTITION_END` y ResultSink producirá un único `QUERY_END` después de completar cada barrera. ImageFetcher también deberá incorporar concurrencia limitada; actualmente descarga secuencialmente para priorizar el pipeline funcional.

### Rama textual Q3/Q4/Q5

```text
QueryRouter
    |
WordExtractor
    |--------------------------|
    v                          v
Q3WordProjector        Q4Q5WordProjector
    |                   |             |
    v                   v             v
Q3WordAggregator   Q4WordCounter   Q5DateRouter
                          |          |         |
                          v          v         v
                   Q4Top20Reducer  Stats   Comparator
```

`WordExtractor` está implementado y reutiliza `common.text.extract_words`:
convierte a minúsculas, tokeniza y descarta tokens exclusivamente numéricos.
No filtra fechas ni idiomas y conserva repeticiones y stopwords. Transforma
`TextArticleBatchMessage` en `TokenizedArticleBatchMessage`, cuyos artículos
contienen `article_id`, `url`, `language`, `modification_date`, `words` y
`has_abstract`. Este último campo permite distinguir un abstract ausente de uno
presente pero vacío.
Los metadatos `execution_id` y `reply_queue` se conservan en el batch.
Publica el mismo batch en las colas independientes de Q3 y Q4/Q5; reenvía
`RoutedInputEndMessage` a ambas incluso si no hubo artículos.

#### Query 3

`Q3WordProjector` conserva solamente artículos `en` y `fr`, deduplica las
palabras dentro de cada abstract y conserva stopwords. Publica batches
`Q3WordPresenceBatchMessage` con `presences` de la forma `{word, language}`,
además de `execution_id` y `reply_queue`. Una presencia puede repetirse entre
artículos o batches.

`Q3WordAggregator` mantiene, por palabra y ejecución, si fue observada en inglés y en francés. Cuando ambas condiciones se cumplen, emite esa palabra una sola vez. La versión funcional utiliza una única partición y publica un `QUERY_END` provisional; las múltiples particiones y su barrera se incorporarán durante la etapa de escalabilidad.

La afinidad se calcula a partir de:

```text
(execution_id, word)
```


#### Query 4

`Q4Q5WordProjector` está implementado. Omite los artículos cuyo abstract era
`None`, elimina stopwords y repeticiones dentro del mismo abstract y publica un
`Q4WordBatchMessage` con una aparición por palabra y artículo en
`q4_word_counter.input.0`. Las repeticiones entre artículos se conservan porque
representan apariciones distintas.

La versión funcional despliega un solo `Q4WordCounter`. Este cuenta las
apariciones y, al recibir el final, publica su top 20 local y un
`Q4PartitionEndMessage` con `partition_id=0`.

`Q4Top20Reducer` recibe el top parcial y espera la marca de fin de la única
partición configurada antes de publicar los pares `{word, count}` y el único
`QUERY_END` de Q4.

El diseño objetivo particionará el conteo por `(execution_id, word)`. Cada
contador producirá su top 20 local y el reducer esperará los finales de todas
las particiones. Enviar solamente veinte candidatos por partición es suficiente
porque una palabra fuera del top local ya tiene veinte palabras de su partición
que la superan o empatan según el desempate alfabético. El routing, el despliegue
de múltiples counters y la coordinación con múltiples producers quedan
pendientes dentro del hito de escalabilidad.

#### Query 5

El mismo `Q4Q5WordProjector` publica en `q5_date_router.input` un
`Q5ArticleWordCountBatchMessage` con `article_id`, URL, fecha y `word_count`.
Los artículos sin abstract se omiten; un abstract presente pero vacío se
conserva con conteo cero, de acuerdo con la referencia serial.

`Q5DateRouter` está implementado con una réplica. Consume
`Q5ArticleWordCountBatchMessage`, no publica batches vacíos y separa:

- Conteos de artículos hasta 2020 en `q5_stats_worker.input`, mediante
  `Q5HistoricalWordCountBatchMessage`.
- `article_id`, URL y conteo de artículos desde 2021 en
  `q5_comparator.input`, mediante `Q5CandidateBatchMessage`.

El mensaje de fin se reenvía siempre a ambas ramas. La implementación actual
utiliza una sola réplica hasta incorporar el protocolo de control y barreras.

`Q5AverageReducer` calcula el promedio global.

`Q5Comparator` conserva temporalmente los candidatos hasta conocer el promedio y luego emite aquellos cuyo conteo sea estrictamente mayor.

RabbitMQ se utiliza como transporte, no como almacenamiento de largo plazo para candidatos. El estado temporal pertenecerá a la aplicación mediante un `CandidateBuffer`.

### ResultSink

El servicio `result-sink` consume `result_sink.input` y reenvía cada `ResultMessage`
inmediatamente a `results.<execution_id>`, sin acumular registros ni esperar al
final de una consulta. También reenvía los `QueryEndMessage` producidos por las
ramas actuales (una instancia por rama). La publicación usa confirmaciones de
RabbitMQ y enrutamiento obligatorio antes de confirmar el mensaje de entrada.
No declara las colas de respuesta: son exclusivas y pertenecen al Gateway.

Esta versión utiliza una sola instancia de ResultSink. Las barreras para ramas
replicadas y la deduplicación persistente quedan pendientes; los productores
actuales deben emitir exactamente un final por consulta, después de sus resultados.

Gateway y Client comienzan la recepción de resultados después de finalizar la
subida. Desde ese momento, los resultados se entregan incrementalmente sin
esperar las demás consultas. Las etapas posteriores de Q5 siguen pendientes, por lo que el cliente
continúa esperando sus cinco finales para terminar la ejecución completa.

## Mensajes de finalización

Los finales tienen significados diferentes:

- `INPUT_END`: el cliente terminó de enviar artículos.
- `PARTITION_END`: una partición terminó una etapa determinada.
- `QUERY_END`: una consulta completa terminó y no producirá más resultados.
- `QUERY_FAILED`: una consulta o ejecución no puede completarse correctamente.

`INPUT_END` no significa que las consultas hayan terminado.

En la primera ejecución de punta a punta habrá una sola réplica por etapa. El último batch y el mensaje de fin viajarán por la misma cola, por lo que el consumidor único procesará primero todos los batches y luego el final.

Antes de habilitar consumidores competidores en una etapa se implementará el siguiente protocolo, basado en el patrón utilizado en el TP de Coordinación:

1. El mensaje de fin se publica después de todos los mensajes de datos en la misma cola compartida.
2. Una sola réplica recibe ese mensaje desde la cola y lo difunde mediante un exchange de control.
3. Cada réplica recibe una copia del control, termina cualquier mensaje que tenga en procesamiento y publica exactamente un `PARTITION_END`.
4. La etapa siguiente mantiene una barrera por `execution_id` y espera un `PARTITION_END` de cada partición o réplica configurada antes de considerar completa la entrada.

El `prefetch_count=1` limita a uno los mensajes en vuelo por réplica cuando se recibe el control. El estado compartido entre el consumidor de datos y el consumidor de control deberá protegerse con sincronización.

No se debe asumir orden entre una cola de datos y una cola o exchange de control diferentes. Por eso el primer mensaje de fin atraviesa la misma cola que los datos antes de ser difundido. La interfaz abstracta `MessageMiddlewareExchange` ya está definida. Queda pendiente implementar `MessageMiddlewareExchangeRabbitMQ`, identificar las particiones y conocer la cantidad esperada de finales para completar cada barrera.

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
- ACK y NACK manuales.
- `prefetch_count=1`.
- Inicio y detención del consumo.
- Cierre explícito de conexiones.

Los componentes de negocio no utilizan Pika directamente.

La interfaz `MessageMiddlewareExchange` ya está definida, pero todavía no tiene una implementación concreta sobre RabbitMQ. `MessageMiddlewareExchangeRabbitMQ` se basará en la utilizada en el TP de Coordinación y se adaptará al protocolo de este sistema. Cada réplica consumirá el control desde su propia cola exclusiva vinculada al exchange, de modo que una publicación llegue a todas las réplicas del grupo.

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

Si el resultado es inválido o el cliente no confirma la recepción, el Gateway ejecuta NACK y detiene el consumo de esa sesión.

## Durabilidad actual

Actualmente las colas se declaran con:

```text
durable = false
```

y los mensajes no se publican como persistentes.

Esto sigue el alcance actual del hito de escalabilidad, que todavía no supone recuperación ante la caída del servidor.

Para que los mensajes sobrevivan a un reinicio de RabbitMQ no alcanza con declarar la cola como durable. También se necesitan mensajes persistentes, almacenamiento persistente de RabbitMQ y una estrategia de recuperación e idempotencia.

Estas decisiones deberán revisarse en el hito de tolerancia a fallos.

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

Actualmente el cliente enviará todos los artículos y luego quedará esperando resultados, ya que el DAG distribuido todavía no está implementado.

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

El QueryRouter publica las proyecciones textuales en `word_extractor.input`, consumida por las ramas Q3, Q4 y Q5. `Until2020Filter` alimenta los filtros de Q1 y Q2. ResultSink reenvía los resultados de Q1-Q4 a `results.<execution_id>`; las salidas de Q5DateRouter permanecen en `q5_stats_worker.input` y `q5_comparator.input` hasta implementar sus consumidores.

Detener el entorno:

```sh
make down
```

## Configuración actual

| Variable | Componente | Valor predeterminado | Descripción |
|---|---|---:|---|
| `SERVER_HOST` | Gateway | `0.0.0.0` | Dirección en la que escucha |
| `SERVER_PORT` | Gateway/Client | `5000` | Puerto TCP |
| `MOM_HOST` | Todos los componentes conectados a RabbitMQ | `localhost` | Host de RabbitMQ |
| `INPUT_BATCHER_QUEUE_PREFIX` | Gateway/InputBatcher | `input_batcher.input` | Prefijo de las colas de entrada |
| `INPUT_BATCHER_PARTITIONS` | Gateway | `1` | Cantidad de particiones de InputBatcher |
| `INPUT_BATCHER_PARTITION_ID` | InputBatcher | `0` | Partición consumida por la instancia |
| `OUTPUT_QUEUE` | InputBatcher | `preprocessor.input` | Cola de salida hacia Preprocessor |
| `MAX_BATCH_BYTES` | InputBatcher | `1048576` | Tamaño serializado máximo de cada batch |
| `INPUT_QUEUE` | Preprocessor | `preprocessor.input` | Cola de batches crudos |
| `OUTPUT_QUEUE` | Preprocessor | `query_router.input` | Cola de batches normalizados |
| `INPUT_QUEUE` | QueryRouter | `query_router.input` | Cola de batches normalizados |
| `Q1_Q2_OUTPUT_QUEUE` | QueryRouter | `until_2020.input` | Cola de la rama Q1/Q2 |
| `TEXT_OUTPUT_QUEUE` | QueryRouter | `word_extractor.input` | Cola de la rama Q3/Q4/Q5 |
| `INPUT_QUEUE` | Until2020Filter | `until_2020.input` | Cola compartida de Q1 y Q2 |
| `Q1_OUTPUT_QUEUE` | Until2020Filter | `q1_english_filter.input` | Cola de candidatos para Q1 |
| `Q2_OUTPUT_QUEUE` | Until2020Filter | `q2_biography_filter.input` | Cola de candidatos para Q2 |
| `INPUT_QUEUE` | Q1 | `q1_english_filter.input` | Cola de candidatos de Q1 |
| `OUTPUT_QUEUE` | Q1 | `result_sink.input` | Cola de resultados y final provisional de Q1 |
| `INPUT_QUEUE` | Q2BiographyFilter | `q2_biography_filter.input` | Cola de candidatos de Q2 |
| `OUTPUT_QUEUE` | Q2BiographyFilter | `image_fetcher.input` | Cola de candidatos para descarga de imágenes |
| `INPUT_QUEUE` | ImageFetcher | `image_fetcher.input` | Cola de candidatos con sección biográfica |
| `OUTPUT_QUEUE` | ImageFetcher | `result_sink.input` | Cola de resultados y final provisional de Q2 |
| `INPUT_QUEUE` | WordExtractor | `word_extractor.input` | Batches de abstracts |
| `Q3_OUTPUT_QUEUE` | WordExtractor | `q3_word_projector.input` | Artículos tokenizados para Q3 |
| `Q4_Q5_OUTPUT_QUEUE` | WordExtractor | `q4_q5_word_projector.input` | Artículos tokenizados para Q4/Q5 |
| `INPUT_QUEUE` | Q3WordProjector | `q3_word_projector.input` | Artículos tokenizados |
| `OUTPUT_QUEUE_PREFIX` | Q3WordProjector | `q3_word_aggregator.input` | Prefijo de salida; actualmente usa `.0` |
| `INPUT_QUEUE` | Q3WordAggregator | `q3_word_aggregator.input.0` | Presencias de palabras |
| `OUTPUT_QUEUE` | Q3WordAggregator | `result_sink.input` | Resultados y final provisional de Q3 |
| `INPUT_QUEUE` | Q4Q5WordProjector | `q4_q5_word_projector.input` | Artículos tokenizados para Q4 y Q5 |
| `Q4_OUTPUT_QUEUE` | Q4Q5WordProjector | `q4_word_counter.input.0` | Apariciones de palabras para la única partición funcional de Q4 |
| `Q5_OUTPUT_QUEUE` | Q4Q5WordProjector | `q5_date_router.input` | Artículos y sus conteos para Q5 |
| `INPUT_QUEUE` | Q4WordCounter | `q4_word_counter.input.0` | Apariciones de palabras de Q4 |
| `OUTPUT_QUEUE` | Q4WordCounter | `q4_top20_reducer.input` | Top 20 local y final de la partición |
| `PARTITION_ID` | Q4WordCounter | `0` | Identificador de la única partición funcional |
| `INPUT_QUEUE` | Q4Top20Reducer | `q4_top20_reducer.input` | Top local y final del counter |
| `OUTPUT_QUEUE` | Q4Top20Reducer | `result_sink.input` | Resultados y final de Q4 |
| `INPUT_QUEUE` | Q5DateRouter | `q5_date_router.input` | Artículos con conteo para Q5 |
| `HISTORICAL_OUTPUT_QUEUE` | Q5DateRouter | `q5_stats_worker.input` | Conteos de artículos hasta 2020 |
| `CANDIDATE_OUTPUT_QUEUE` | Q5DateRouter | `q5_comparator.input` | Candidatos desde 2021 |
| `INPUT_FILE` | Client | `datasets/wikipedia_sample.parquet` | Dataset de entrada |
| `OUTPUT_DIR` | Client | `output/distributed` | Directorio de resultados |
| `LOG_LEVEL` | Todos los procesos | `INFO` | Nivel de logging |

## Configuración prevista

La arquitectura completa deberá externalizar, entre otras:

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
Q5_BUFFER_DIRECTORY
```

Estas variables todavía no están implementadas.

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

## Orden de implementación

El desarrollo se divide en tres momentos distintos:

1. Completar primero un pipeline funcional de punta a punta con una réplica por componente y finales simples.
2. Dentro de este mismo TP de escalabilidad, incorporar múltiples réplicas, mensajes de control, `PARTITION_END` y barreras que produzcan un único `QUERY_END` por consulta.
3. En la instancia posterior de tolerancia a fallos, incorporar persistencia, recuperación ante caídas e idempotencia frente a reentregas.

## Próximos pasos

1. Incorporar barreras para productores replicados en ResultSink.
2. Implementar Q5StatsWorker, Q5AverageReducer y Q5Comparator a partir de las salidas de Q5DateRouter.
3. Ejecutar y comparar automáticamente Q1-Q5 con la referencia serial.
4. Implementar los protocolos de `PARTITION_END` y barreras.
5. Permitir clientes concurrentes y aislados en el Gateway.
6. Completar graceful shutdown de todos los procesos.
7. Completar la propagación de errores y los límites para entradas no confiables.
8. Incorporar tolerancia a fallos, persistencia e idempotencia en el siguiente hito.

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

El datasource y el dashboard **System Overview** se aprovisionan automáticamente
en Grafana.

El dashboard incluye el estado de los scrapes, CPU, memoria y tráfico de red por
servicio, mensajes pendientes y sin confirmar, consumidores por cola, tasas de
publicación, confirmación y reentrega, y recursos del broker. Sus filtros permiten
seleccionar el proyecto de Compose, el servicio y la cola.

### Observaciones previstas para la demo

La demo no se limitará a mostrar los resultados finales. También se utilizarán
el panel de administración de RabbitMQ, las estadísticas de Docker y el dashboard
de Grafana para relacionar el comportamiento observable con las decisiones de la
arquitectura.

El guion de demostración deberá incluir, una vez implementadas las capacidades
correspondientes:

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

Este guion describe la demo objetivo, no el estado funcional actual. En
particular, la concurrencia de clientes, la recuperación ante la caída de uno de
ellos y el manejo completo de recursos temporales continúan pendientes. Las
pruebas concretas, la carga utilizada y el comportamiento esperado de cada caída
se definirán cuando el pipeline y esas capacidades estén implementados.

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
