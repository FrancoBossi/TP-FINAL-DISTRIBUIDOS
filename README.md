# TP-FINAL-DISTRIBUIDOS

## Entrada y resultados

El gateway asigna cada ejecución a una cola durable de `InputBatcher` y publica
allí todos sus artículos y su `INPUT_END`. Las colas siguen la convención
`input_batcher.input.<partition>`. El prefijo y la cantidad de particiones se
configuran mediante `INPUT_BATCHER_QUEUE_PREFIX` e
`INPUT_BATCHER_PARTITIONS`, respectivamente.

Después de la carga, el cliente espera resultados de las cinco consultas y los
guarda en `OUTPUT_DIR`. Cada consulta debe emitir un `QUERY_END`, incluso si
no produjo resultados. Las consultas vacías generan archivos JSONL vacíos.

## Prueba manual (temporal)

> **Note:** Es temporal hasta que esté la implementación completa

Correr el gateway y el cliente:

```sh
make up

# Una vez que haya iniciado el gateway, se puede correr muchas veces:
docker compose --profile client run --build --rm client
```

```sh
RUN_ID='REEMPLAZAR_POR_EL_ID_DEL_LOG'

# Publicar un resultado de ejemplo de la consulta 3.
docker compose exec -T rabbitmq rabbitmqadmin -u guest -p guest publish \
  routing_key="results.$RUN_ID" \
  payload="{\"type\":\"RESULT\",\"run_id\":\"$RUN_ID\",\"query_id\":3,\"record\":{\"word\":\"network\"}}"

# Marcar las cinco consultas como terminadas.
for query_id in 1 2 3 4 5; do
  docker compose exec -T rabbitmq rabbitmqadmin -u guest -p guest publish \
    routing_key="results.$RUN_ID" \
    payload="{\"type\":\"QUERY_END\",\"run_id\":\"$RUN_ID\",\"query_id\":$query_id}"
done
```

El cliente debe guardar `{"word":"network"}` en `query3.jsonl`, crear los otros
cuatro archivos vacíos y finalizar. Estos resultados son ficticios: no validan
las consultas sobre el dataset. Los artículos enviados siguen pendientes en la
cola de entrada si todavía no hay un `InputBatcher` consumiéndolos.
