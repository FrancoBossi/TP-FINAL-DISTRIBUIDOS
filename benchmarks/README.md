# Benchmarks reproducibles

Este directorio documenta las mediciones de referencia para comparar el sistema
actual con la versión tolerante a fallos. Los artefactos generados se guardan en
`benchmarks/results/` y no se versionan.

## Prueba corta

La prueba corta reconstruye, si hace falta, el dataset histórico de 50.000
artículos usando el primer shard en inglés y el primero en francés que ya están
en la caché local. Luego ejecuta una corrida y compara sus cinco resultados con
`output/reference` sin depender del orden de las líneas:

```bash
make benchmark-smoke
```

## Baseline del dataset de la demo

La medición principal usa 12 shards por idioma y realiza tres corridas
independientes. Ese tamaño se eligió después de comprobar que los 90 shards por
idioma demoraban varias horas por corrida debido a las descargas externas de Q2;
el objetivo práctico es mantener cada medición por debajo de 30 minutos:

```bash
make benchmark-baseline RUNS=3
```

Si el dataset no existe, el target lo reconstruye desde los shards guardados en
`datasets/kaggle_shards/`. El archivo predeterminado es
`datasets/wikipedia_benchmark_12.parquet`.

La primera línea base completa tardó aproximadamente 9 minutos y 12 segundos por
corrida. Este tiempo deja margen para repetir las mediciones durante el desarrollo
y para ejecutar escenarios con recuperación y Chaos sin reducir la muestra de
612.475 artículos.

Cada corrida arranca una topología limpia de Docker Compose, mide el tiempo de
punta a punta del cliente y toma muestras de CPU y memoria de los contenedores.
El runner conserva:

- commit, rama, estado del repositorio y metadatos/hash del dataset;
- configuración, duración y código de salida de cada corrida;
- logs del cliente y de Docker Compose;
- muestras y resumen de CPU/memoria por contenedor;
- los cinco archivos de salida y fingerprints independientes del orden;
- comparación entre corridas para detectar resultados inestables.

La consulta 2 usa una API HTTP externa. Por eso una diferencia real en sus
resultados puede reflejar cambios o fallas del servicio externo; el runner la
informa y no la oculta.

## Comparación con tolerancia a fallos

Una vez implementado el hito se repite exactamente el comando principal con la
tolerancia habilitada pero sin inyectar fallas. Esa segunda serie mide el costo
de la solución. Las corridas con Chaos se registran por separado porque evalúan
recuperación y corrección, no el baseline de rendimiento.

Opciones útiles del runner:

```bash
.venv/bin/python scripts/run_benchmark.py --help
```
