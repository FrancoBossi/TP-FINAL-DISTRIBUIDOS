# Wikipedia Article Analysis

Trabajo práctico grupal de Sistemas Distribuidos. El sistema procesa artículos de Wikipedia en inglés y francés y genera los resultados de cinco consultas mediante una arquitectura distribuida basada en RabbitMQ.

> El proyecto se encuentra en desarrollo. Actualmente están implementadas la ejecución serial de referencia, la lectura del dataset, el cliente, el Gateway, el protocolo externo y la capa común de middleware. Los controllers del DAG distribuido todavía están pendientes.

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
| Serialización binaria explícita de los mensajes | Pendiente |
| Streaming de artículos desde el cliente | Implementado |
| Identificador único por ejecución | Implementado |
| Particionado estable Gateway → InputBatcher | Implementado |
| Middleware común sobre RabbitMQ | Implementado |
| Cola temporal de resultados por ejecución | Implementado |
| InputBatcher y procesamiento distribuido del DAG | Pendiente |
| ResultSink distribuido | Pendiente |
| Atención concurrente de varios clientes | Pendiente |
| Graceful shutdown completo para procesos concurrentes | Pendiente |
| Estrategia de tolerancia a fallos y persistencia | Pendiente |

### Limitación actual de clientes

El Gateway actualmente atiende una única conexión por vez. Una ejecución debe finalizar antes de que el servidor comience a atender al siguiente cliente.

La arquitectura objetivo requiere ejecuciones aisladas y concurrentes entre varios clientes. Para alcanzarla, el servidor deberá ejecutar cada sesión en un thread o proceso independiente, mantener separados sus recursos y realizar el cierre ordenado de sockets y tareas activas.

### Estado actual de Query 2

La referencia serial utiliza `image.content_url` cuando está presente en el registro del dataset. El sistema realiza un HTTP GET directo a esa URL y utiliza el cuerpo de la respuesta como los bytes de la imagen.

La descarga cuenta con timeout y una cantidad acotada de reintentos. Si el artículo no tiene `content_url` o la descarga falla definitivamente, ese artículo se omite del resultado de Query 2.

Está pendiente confirmar si la implementación distribuida deberá consultar adicionalmente la API REST de Wikimedia cuando el dataset no incluya una imagen.

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

Todavía no existe un `InputBatcher` que consuma los artículos ni están implementadas las etapas posteriores. Por ese motivo, la prueba distribuida actual utiliza publicación manual de resultados ficticios.

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

El InputBatcher deberá construir batches con un límite configurable en bytes:

```text
MAX_BATCH_BYTES
```

Cuando llega un artículo:

1. Calcula su tamaño serializado dentro del batch.
2. Lo agrega si el batch resultante no supera el límite.
3. Si lo supera, publica el batch actual y comienza uno nuevo.
4. Al recibir `INPUT_END`, publica el último batch parcial.
5. Después inicia la propagación del fin de la etapa.

### Preprocessor

El Preprocessor será una etapa sin estado encargada de:

- Validar la estructura de cada artículo.
- Normalizar fecha e idioma.
- Preparar la representación básica del abstract.
- Construir un artículo normalizado para las siguientes etapas.

No deberá aplicar reglas específicas de una query.

### QueryRouter

El QueryRouter realizará el fan-out hacia dos ramas:

```text
Q1/Q2
Q3/Q4/Q5
```

Publicará solamente los campos requeridos por cada rama y no realizará filtros propios de las consultas.

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

- `Q1EnglishFilter` conserva artículos en inglés y genera los resultados de Query 1.
- `Q2BiographyFilter` busca las secciones `biography` o `biographie`.
- `ImageFetcher` descarga las imágenes con timeout, reintentos acotados y concurrencia limitada.

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

`WordExtractor` solamente normaliza y tokeniza. No aplica fechas, stopwords ni reglas particulares de una consulta.

#### Query 3

`Q3WordProjector` produce palabras únicas por artículo junto con su idioma.

`Q3WordAggregator` mantiene, por palabra y ejecución, si fue observada en inglés y en francés. Cuando ambas condiciones se cumplen, emite esa palabra una sola vez.

La afinidad se calcula a partir de:

```text
(execution_id, word)
```

#### Query 4

`Q4Q5WordProjector` elimina stopwords y repeticiones dentro del mismo abstract.

`Q4WordCounter` cuenta apariciones y produce un top 20 local por partición.

`Q4Top20Reducer` combina los tops parciales y genera el top 20 global. Los reducers serán replicables y cada ejecución se asignará a una réplica mediante `execution_id`.

#### Query 5

`Q5DateRouter` separa:

- Artículos hasta 2020 para calcular suma y cantidad.
- Artículos desde 2021 como candidatos.

`Q5AverageReducer` calcula el promedio global.

`Q5Comparator` conserva temporalmente los candidatos hasta conocer el promedio y luego emite aquellos cuyo conteo sea estrictamente mayor.

RabbitMQ se utiliza como transporte, no como almacenamiento de largo plazo para candidatos. El estado temporal pertenecerá a la aplicación mediante un `CandidateBuffer`.

### ResultSink

El ResultSink recibirá resultados y finales de las distintas ramas.

Estará particionado por `execution_id` para que todos los resultados de una ejecución converjan en una única réplica lógica.

Cada query debe producir exactamente un mensaje final:

```text
QUERY_END
```

El cliente considera terminada la ejecución solamente después de recibir los finales de las cinco consultas.

## Mensajes de finalización

Los finales tienen significados diferentes:

- `INPUT_END`: el cliente terminó de enviar artículos.
- `PARTITION_END`: una partición terminó una etapa determinada.
- `QUERY_END`: una consulta completa terminó y no producirá más resultados.
- `QUERY_FAILED`: una consulta o ejecución no puede completarse correctamente.

`INPUT_END` no significa que las consultas hayan terminado.

Cuando una etapa tiene varias réplicas, el fin debe propagarse a todas y la siguiente etapa debe esperar la barrera completa de `PARTITION_END`. No se debe asumir orden entre colas diferentes sin una barrera explícita.

## Comunicación

### Cliente–Gateway

El protocolo externo utiliza sockets TCP.

Cada frame contiene:

```text
4 bytes de longitud en orden de red
cuerpo serializado del mensaje
```

El receptor continúa leyendo hasta obtener exactamente la cantidad de bytes indicada por el encabezado. Esto permite manejar fragmentación de TCP correctamente.

Actualmente, el cuerpo de cada mensaje se serializa como JSON codificado en UTF-8. Durante el hito de escalabilidad se mantendrá esta representación para priorizar una ejecución completa de las cinco consultas de punta a punta.

Mientras se utilice JSON, las imágenes de Query 2 deberán transportarse como strings Base64 dentro de `ResultMessage`. Esto permite escribir directamente el mismo valor en `query2.jsonl`, cuyo formato también representa la imagen como Base64.

#### Serialización binaria postergada

Una vez que el pipeline distribuido funcione de punta a punta, se evaluará reemplazar JSON por una serialización binaria explícita, sin modificar el framing TCP actual de cuatro bytes de longitud.

El diseño acordado como punto de partida es:

- Un byte inicial para identificar la clase concreta del mensaje.
- Códigos diferentes para mensajes externos e internos, por ejemplo `ARTICLE` y `ROUTED_ARTICLE`.
- Campos superiores codificados en un orden conocido, sin enviar sus nombres.
- Un codec propio para `None`, enteros, strings, bytes, listas, tuplas y diccionarios.
- Strings y bloques de bytes precedidos por una longitud de cuatro bytes en orden de red.
- Enteros representados con signo en ocho bytes y en orden de red.
- Claves de diccionarios restringidas a strings.
- Bytes binarios transportados directamente, sin Base64 dentro de la comunicación.
- Rechazo de códigos desconocidos, mensajes truncados, UTF-8 inválido, tipos no soportados y bytes sobrantes.

La asignación definitiva de códigos se realizará cuando estén estabilizados todos los tipos de mensajes del DAG. No se utilizarán bibliotecas externas que resuelvan automáticamente la serialización.

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

Para etapas que requieran difusión de mensajes de control se agregará `MessageMiddlewareExchange`.

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

Los artículos permanecen en la cola de entrada porque todavía no existe un InputBatcher que los consuma.

Detener el entorno:

```sh
make down
```

## Configuración actual

| Variable | Componente | Valor predeterminado | Descripción |
|---|---|---:|---|
| `SERVER_HOST` | Gateway | `0.0.0.0` | Dirección en la que escucha |
| `SERVER_PORT` | Gateway/Client | `5000` | Puerto TCP |
| `MOM_HOST` | Gateway | `localhost` | Host de RabbitMQ |
| `INPUT_BATCHER_QUEUE_PREFIX` | Gateway | `input_batcher.input` | Prefijo de las colas de entrada |
| `INPUT_BATCHER_PARTITIONS` | Gateway | `1` | Cantidad de particiones de InputBatcher |
| `INPUT_FILE` | Client | `datasets/wikipedia_sample.parquet` | Dataset de entrada |
| `OUTPUT_DIR` | Client | `output/distributed` | Directorio de resultados |
| `LOG_LEVEL` | Client/Gateway | `INFO` | Nivel de logging |

## Configuración prevista

La arquitectura completa deberá externalizar, entre otras:

```text
MAX_BATCH_BYTES
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

## Próximos pasos

1. Implementar InputBatcher con batches limitados por bytes.
2. Implementar Preprocessor y QueryRouter.
3. Implementar la rama compartida de Q1 y Q2.
4. Implementar el pipeline textual de Q3, Q4 y Q5.
5. Implementar los protocolos de `PARTITION_END` y barreras.
6. Implementar ResultSink.
7. Permitir clientes concurrentes y aislados en el Gateway.
8. Completar graceful shutdown de todos los procesos.
9. Resolver la política definitiva de imágenes faltantes para Query 2.
10. Ejecutar y comparar automáticamente todo el pipeline con la referencia serial.
11. Reemplazar la serialización JSON por el protocolo binario explícito documentado.
12. Incorporar tolerancia a fallos, persistencia e idempotencia en el siguiente hito.
