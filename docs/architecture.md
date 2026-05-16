# Documento de Arquitectura 4+1 — Sistema Distribuido de Detección de Anomalías en Transacciones

---

## 1. Vista Lógica

La vista lógica describe los componentes funcionales clave, sus responsabilidades y sus relaciones, independientemente de las concernientes al despliegue.

### 1.1 Modelo de Dominio

El sistema procesa **registros de transacciones** entre cuentas bancarias. Cada transacción contiene:

| Campo | Descripción |
|---|---|
| `source_account` | Identificador de la cuenta de origen |
| `destination_account` | Identificador de la cuenta de destino |
| `amount` | Monto de la transacción en la moneda original |
| `currency` | Código de la moneda original |
| `amount_usd` | Monto convertido a USD |
| `bank` | Nombre del banco de la cuenta de origen |
| `payment_method` | Método de pago (ej.: Wire, ACH) |
| `date` | Fecha de la transacción |

### 1.2 Componentes Funcionales

```
┌─────────────────────────────────────────────────────────────────────┐
│              Sistema de Análisis de Transacciones                    │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  ┌─────────────┐    ┌──────────────────┐    ┌─────────────────────┐  │
│  │   Cliente    │    │     Gateway      │    │  Message Handler    │  │
│  │ (lectura/env)│───>│  (entrada/salida)│───>│  (serializar/deser) │  │
│  └─────────────┘    └──────────────────┘    └─────────────────────┘  │
│                                                                       │
│  ┌──────────────────┐                                                 │
│  │ Conversor USD    │  Pre-procesamiento compartido: moneda → USD     │
│  └──────────────────┘                                                 │
│                                                                       │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌────────┐│
│  │Filtro Q1 │  │Filtro Q2 │  │Filtro Q3 │  │Filtro Q4 │  │Filtro  ││
│  │USD < 50  │  │Máx/Banco │  │Anomalía  │  │Scatter-  │  │Q5      ││
│  │          │  │          │  │Detección │  │Gather    │  │Wire/ACH││
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘  └────────┘│
│       │              │              │              │           │      │
│       │         ┌────┴────┐   ┌────┴────┐         │      ┌──┴──┐   │
│       │         │Agg Q2   │   │Agg Q3.1 │    ┌────┴────┐ │Agg  │   │
│       │         │merge max│   │merge avg│    │Q4 Merge │ │Q5   │   │
│       │         └────┬────┘   │         │    │+ Detect │ │count │   │
│       │              │        └────┬────┘    └────┬────┘ └──┬──┘   │
│       │              │        ┌────┴────┐         │         │      │
│       │              │        │Agg Q3.2 │         │         │      │
│       │              │        │merge    │    ┌────┴────┐    │      │
│       │              │        │candidatos│   │Q4 Scat- │    │      │
│       │              │        └────┬────┘    │Gather   │    │      │
│       │              │             │         │Detect   │    │      │
│       │              │        ┌────┴────┐    └────┬────┘    │      │
│       │              │        │Q3 Comp  │         │         │      │
│       │              │        │< avg/100│         │         │      │
│       │              │        └────┬────┘         │         │      │
│       │              │             │              │         │      │
│  ┌────┴──────────────┴─────────────┴──────────────┴─────────┴──┐   │
│  │                    Join (sincronización 5 vías)               │   │
│  └──────────────────────────────┬──────────────────────────────┘   │
│                                  │                                   │
└──────────────────────────────────┼───────────────────────────────────┘
                                   │
                         Resultados al Cliente
```

### 1.3 Responsabilidades por Consulta

| Consulta | Filtro(s) | Entrada | Salida | Con Estado |
|---|---|---|---|---|
| Q1 | Filtro Q1 | Todas las transacciones | Registros donde `amount_usd < 50` | Sin estado |
| Q2 | Filtro Q2 + Agg Q2 | Todas las transacciones | `(banco, cuenta_origen, monto_máximo)` por banco | Con estado |
| Q3 | Filtro Q3.1 + Agg Q3.1 + Filtro Q3.2 + Agg Q3.2 + Filtro Q3.3 | Todas las transacciones | `(cuenta_origen, monto)` donde `monto < promedio/100` | Con estado (dos fases) |
| Q4 | Filtro Q4.1 + Q4 Merger + Q4 ScatterGather | Transacciones en [09-01, 09-05] | Cuentas que cumplen patrón scatter-gather | Con estado (grafo) |
| Q5 | Filtro Q5 + Agg Q5 | Transacciones en [09-01, 09-05] | `cantidad` de transacciones Wire/ACH con `USD < 1` | Con estado (contador) |

### 1.4 Detalle de Consulta: Q3 (Detección de Anomalías)

Q3 requiere un cálculo en dos fases:

1. **Fase 1** (Filtro Q3.1): Acumular suma y conteo de montos USD agrupados por `payment_method` para transacciones en `[2022-09-01, 2022-09-05]`. Al recibir EOF, computar el promedio por método de pago.
2. **Fase 2** (Filtro Q3.2): Almacenar en búfer todas las transacciones en `[2022-09-06, 2022-09-15]` (candidatas).
3. **Comparación** (Filtro Q3.3): Recibir promedios fusionados de Agg Q3.1 y candidatas fusionadas de Agg Q3.2. Para cada candidata, verificar si su `amount_usd < promedio(payment_method) / 100`. Emitir los registros coincidentes.

Ambas fases consumen del mismo exchange upstream en paralelo, pero sus resultados convergen en el Filtro Q3.3.

### 1.5 Detalle de Consulta: Q4 (Detección Scatter-Gather)

Q4 detecta cuentas que siguen el patrón scatter-gather con una única cuenta separadora:

1. **Constructor de Grafo** (Filtro Q4.1): Para cada transacción en `[2022-09-01, 2022-09-05]`, construir una lista de adyacencia `{cuenta_origen: conjunto(cuentas_destino)}`. Rastrear solo cuentas con `≥ 5` destinos distintos (candidatas a scatter).
2. **Fusionador de Grafos** (Filtro Q4.2): Fusionar las listas de adyacencia parciales de todas las instancias de Q4.1 en un grafo completo.
3. **Detector de Patrón** (Filtro Q4.3): Sobre el grafo completo, identificar cuentas `A` tales que:
   - `A` transfirió a ≥ 5 cuentas distintas `D1...Dn` (scatter)
   - Existe una única cuenta `M` tal que todas `D1...Dn` transfirieron a `M` (gather a través de un intermediario)
   - Emitir las cuentas que coincidan con este patrón.

---

## 2. Vista de Procesos

La vista de procesos describe el comportamiento en tiempo de ejecución del sistema, incluyendo concurrencia, sincronización y el ciclo de vida del procesamiento de datos.

### 2.1 Ciclo de Vida de un Proceso

Cada servicio sigue el mismo patrón de ciclo de vida:

```
Inicio → Conectar a RabbitMQ → Registrar manejador SIGTERM → Comenzar a consumir → Procesar mensajes → EOF/Cierre → Cerrar conexiones → Salir
```

### 2.2 Flujo de Ingesta de Datos

1. El **Cliente** abre una conexión TCP hacia el **Gateway**.
2. El **Gateway** asigna un `client_id` único mediante un contador atómico y crea un `MessageHandler` para ese cliente.
3. Por cada registro de transacción, el **Gateway** lo serializa como `[client_id, origen, destino, monto, moneda, banco, método_pago, fecha]` y publica en el exchange `transactions` con `rk="transactions"`.
4. El **Conversor USD** consume de `rk="transactions"`, agrega `amount_usd` (calculado a partir de `amount` y `currency`), y republica en el exchange `usd_transactions` con `rk="usd_transactions"`.
5. Todos los filtros downstream consumen de `rk="usd_transactions"` mediante round-robin de RabbitMQ dentro de sus respectivas colas.

### 2.3 Propagación de EOF y Sincronización

El sistema utiliza **mensajes EOF** para señalar el fin del flujo de datos de un cliente. La propagación de EOF sigue el patrón establecido en el trabajo de coordinación:

#### 2.3.1 Filtros Sin Estado (Q1)

- Solo una instancia recibe el EOF (efecto colateral del round-robin).
- Esa instancia **fanout-difunde** el EOF a todas las demás instancias mediante un exchange `fanout`.
- Cada instancia envía su propio EOF al **Join**. No se necesitan resultados parciales (Q1 reenvía registros a medida que llegan).

#### 2.3.2 Filtros Con Estado con Agregación (Q2, Q5)

- El EOF llega a una instancia vía round-robin.
- Esa instancia **fanout-difunde** el EOF. Todas las instancias envían (flush) su estado parcial (máximo parcial por banco para Q2, conteos parciales para Q5) al **Agregador** correspondiente.
- Cada instancia también envía un EOF al Agregador.
- El **Agregador** cuenta los EOF. Cuando ha recibido `FILTER_N_AMOUNT` EOFs, fusiona los resultados parciales y envía el resultado final al **Join**.

#### 2.3.3 Filtro de Dos Fases (Q3)

- **Filtro Q3.1** (promedios): Al recibir EOF, fanout-difunde, todas las instancias envían sumas/conteos parciales a **Agg Q3.1**. Agg Q3.1 cuenta EOFs, computa los promedios finales, los envía a **Filtro Q3.3**.
- **Filtro Q3.2** (candidatas): Al recibir EOF, fanout-difunde, todas las instancias envían las candidatas almacenadas en búfer a **Agg Q3.2**. Agg Q3.2 cuenta EOFs, fusiona las listas de candidatas, las envía a **Filtro Q3.3**.
- **Filtro Q3.3**: Espera ambas entradas (promedios fusionados y candidatas fusionadas). Cuando ambas llegan, realiza la comparación `monto < promedio/100` y envía los resultados al **Join**.

#### 2.3.4 Filtro Basado en Grafo (Q4)

- **Filtro Q4.1**: Al recibir EOF, fanout-difunde, todas las instancias envían sus listas de adyacencia parciales. Cada instancia publica su grafo parcial al exchange `graph_merge` con `rk="graph_merge"`.
- **Filtro Q4.2** (Fusionador de Grafos): Se vincula a `rk="graph_merge"`. Cuenta `FILTER_4_1_AMOUNT` EOFs. Fusiona todas las listas de adyacencia parciales en un grafo completo. Envía el grafo completo al **Filtro Q4.3**.
- **Filtro Q4.3** (Detector de Patrón): Recibe el grafo completo, ejecuta la detección scatter-gather, envía las cuentas coincidentes al **Join**.

#### 2.3.5 Sincronización del Join

El **Join** consume de 5 colas de resultados (una por consulta) mediante `MultiQueueConsumer`. Realiza seguimiento por `client_id` de qué consultas han entregado resultados. Cuando los 5 resultados de consulta han llegado para un `client_id`, ensambla la respuesta final y la publica en `results_queue`.

### 2.4 Manejo de SIGTERM

Cada servicio registra un manejador de `SIGTERM` que:

1. Establece un flag `_closed`.
2. Llama a `stop_consuming()` en su(s) conexión(es) de middleware.
3. El middleware utiliza `add_callback_threadsafe` para interrumpir de forma segura el bucle de `start_consuming()` desde el hilo del manejador de señales.
4. El servicio sale de su bucle principal, cierra todas las conexiones en un bloque `finally`, y termina de forma graceful.

El Gateway adicionalmente:
- Cierra su socket de servidor TCP.
- Cierra todos los sockets de clientes activos.
- Señaliza el pool de procesos que maneja respuestas.

---

## 3. Vista de Desarrollo

La vista de desarrollo describe la organización del software, la estructura de módulos y las decisiones tecnológicas.

### 3.1 Stack Tecnológico

| Componente | Tecnología |
|---|---|
| Lenguaje | Python 3.12 |
| Middleware de Mensajes | RabbitMQ (vía `pika`) |
| Contenerización | Docker / Docker Compose |
| Serialización Interna | JSON (`message_protocol.internal`) |
| Protocolo Externo | Binario sobre TCP (`message_protocol.external`) |
| Build | Makefile |

### 3.2 Estructura de Directorios

```
src/
├── common/
│   ├── __init__.py
│   ├── middleware/
│   │   ├── __init__.py
│   │   ├── middleware.py              # Interfaces abstractas
│   │   └── middleware_rabbitmq.py     # Implementación RabbitMQ
│   ├── message_protocol/
│   │   ├── __init__.py
│   │   ├── internal.py               # Serialización JSON para RabbitMQ
│   │   ├── external.py               # Protocolo binario para TCP
│   │   └── external_serializer.py    # Helpers binarios de bajo nivel
│   └── transaction/
│       ├── __init__.py
│       └── transaction.py            # Clase de registro de transacción
│
├── gateway/
│   ├── Dockerfile
│   ├── main.py
│   └── message_handler/
│       ├── __init__.py
│       └── message_handler.py        # client_id + serializar/deserializar
│
├── usd_converter/
│   ├── Dockerfile
│   └── main.py
│
├── filter_q1/                        # Q1: USD < 50
│   ├── Dockerfile
│   └── main.py
│
├── filter_q2/                        # Q2: Máximo por banco
│   ├── Dockerfile
│   └── main.py
│
├── agg_q2/                           # Q2: Agregador
│   ├── Dockerfile
│   └── main.py
│
├── filter_q3_avg/                    # Q3 Fase 1: Promedio por método de pago
│   ├── Dockerfile
│   └── main.py
│
├── agg_q3_avg/                       # Q3 Agregador: fusionar promedios
│   ├── Dockerfile
│   └── main.py
│
├── filter_q3_candidates/             # Q3 Fase 2: Almacenar candidatas en búfer
│   ├── Dockerfile
│   └── main.py
│
├── agg_q3_candidates/                # Q3 Agregador: fusionar candidatas
│   ├── Dockerfile
│   └── main.py
│
├── filter_q3_comparison/             # Q3: comparación monto < promedio/100
│   ├── Dockerfile
│   └── main.py
│
├── filter_q4_graph/                  # Q4 Fase 1: Construir grafo parcial
│   ├── Dockerfile
│   └── main.py
│
├── filter_q4_merger/                 # Q4 Fase 2: Fusionar grafos
│   ├── Dockerfile
│   └── main.py
│
├── filter_q4_scatter_gather/         # Q4 Fase 3: Detectar patrón
│   ├── Dockerfile
│   └── main.py
│
├── filter_q5/                        # Q5: Conteo Wire/ACH
│   ├── Dockerfile
│   └── main.py
│
├── agg_q5/                           # Q5: Agregador
│   ├── Dockerfile
│   └── main.py
│
├── join/
│   ├── Dockerfile
│   └── main.py
│
├── client/
│   ├── Dockerfile
│   └── main.py
│
└── rabbitmq/
    └── Dockerfile
```

### 3.3 Abstracción del Middleware Común

La capa de middleware proporciona las siguientes abstracciones (siguiendo el patrón del trabajo de coordinación):

| Clase | Propósito |
|---|---|
| `MessageMiddleware` (ABC) | Interfaz base: `start_consuming`, `stop_consuming`, `send`, `close` |
| `MessageMiddlewareQueue` (ABC) | Especialización de cola de `MessageMiddleware` |
| `MessageMiddlewareExchange` (ABC) | Especialización de exchange de `MessageMiddleware` |
| `MessageMiddlewareQueueRabbitMQ` | Implementación de cola RabbitMQ con reintentos, envío thread-safe, soporte SIGTERM |
| `MessageMiddlewareExchangeRabbitMQ` | Implementación de exchange direct de RabbitMQ |
| `FanoutExchange` | Exchange fanout de RabbitMQ para difusión (sincronización EOF entre instancias de filtros) |
| `MultiQueueConsumer` | Consumidor de conexión única para múltiples colas (usado por Join y filtros que consumen de múltiples fuentes) |

### 3.4 Protocolo de Mensajes

**Interno (RabbitMQ):** Listas codificadas en JSON. Ejemplos:
- Datos: `[client_id, origen, destino, monto, moneda, amount_usd, banco, método_pago, fecha]`
- EOF: `[client_id]`
- Resultado: `[client_id, payload_resultado]` (el payload varía según la consulta)

**Externo (TCP):** Protocolo binario con mensajes prefijados por tipo, campos delimitados por longitud, y handshake basado en ACK. Mismo patrón que el trabajo de coordinación.

### 3.5 Plantilla de Servicio

Cada servicio de filtro/agregador sigue el mismo patrón de código:

```python
class FilterN:
    def __init__(self):
        # Inicializar conexiones de middleware
        # Inicializar diccionarios de estado por cliente

    def _process_data(self, client_id, ...):
        # Acumular estado

    def _process_eof(self, client_id):
        # Enviar (flush) estado, enviar a downstream, difundir EOF si es necesario

    def process_message(self, message, ack, nack):
        # Deserializar, despachar a _process_data o _process_eof

    def _handle_sigterm(self, signum, frame):
        self._closed = True
        self.middleware.stop_consuming()

    def start(self):
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        self.middleware.start_consuming(self.process_message)

    def close(self):
        # Cerrar todas las conexiones de middleware

def main():
    service = FilterN()
    try:
        service.start()
    except MessageMiddlewareDisconnectedError:
        logging.info("Middleware desconectado")
    finally:
        service.close()
```

---

## 4. Vista Física

La vista física describe la topología de despliegue, mapeando componentes de software a hardware/contenedores.

### 4.1 Topología de Contenedores

```
┌─────────────────────────────────────────────────────────────────────┐
│                       Red Docker                                     │
│                                                                       │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐                               │
│  │client_0 │  │client_1 │  │client_N │  (N clientes, cada uno un      │
│  └────┬────┘  └────┬────┘  └────┬────┘   contenedor)                  │
│       │             │             │         TCP                        │
│  ┌────┴─────────────┴─────────────┴────┐                              │
│  │            Gateway                    │                              │
│  └──────────────────┬───────────────────┘                              │
│                      │ RabbitMQ                                        │
│  ┌───────────────────┴───────────────────────┐                        │
│  │            Broker RabbitMQ                 │                        │
│  │  Exchanges:                                │                        │
│  │    - transactions (direct)                 │                        │
│  │    - usd_transactions (direct)             │                        │
│  │    - graph_merge (direct)                  │                        │
│  │  Colas:                                    │                        │
│  │    - usd_converter_queue                   │                        │
│  │    - filter_q1_queue                       │                        │
│  │    - filter_q2_queue                       │                        │
│  │    - filter_q3_avg_queue                   │                        │
│  │    - filter_q3_candidates_queue            │                        │
│  │    - filter_q4_graph_queue                 │                        │
│  │    - filter_q5_queue                       │                        │
│  │    - agg_q2_queue, agg_q3_avg_queue, ...   │                        │
│  │    - filter_q3_comparison_queue            │                        │
│  │    - filter_q4_merger_queue                │                        │
│  │    - filter_q4_scatter_gather_queue        │                        │
│  │    - join_queue                            │                        │
│  │    - results_queue                         │                        │
│  └───────────────────────────────────────────┘                        │
│                                                                       │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐              │
│  │usd_conv_0    │   │usd_conv_1    │   │usd_conv_N    │  (escalable) │
│  └──────────────┘   └──────────────┘   └──────────────┘              │
│                                                                       │
│  ┌──────┐ ┌──────┐ ┌──────┐   ┌──────┐ ┌──────┐ ┌──────┐           │
│  │f_q1_0│ │f_q1_1│ │f_q1_N│   │f_q2_0│ │f_q2_1│ │f_q2_N│ (cada    │
│  └──────┘ └──────┘ └──────┘   └──────┘ └──────┘ └──────┘  escalable)│
│                                                                       │
│  ┌──────┐ ┌──────┐   ┌──────┐ ┌──────┐   ┌──────┐ ┌──────┐         │
│  │f_q3a0│ │f_q3aN│   │f_q3b0│ │f_q3bN│   │f_q4_0│ │f_q4_N│         │
│  └──────┘ └──────┘   └──────┘ └──────┘   └──────┘ └──────┘         │
│                                                                       │
│  ┌──────┐ ┌──────┐   ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐           │
│  │f_q5_0│ │f_q5_N│   │agg_q2│ │aggq3a│ │aggq3b│ │agg_q5│           │
│  └──────┘ └──────┘   └──────┘ └──────┘ └──────┘ └──────┘           │
│                                                                       │
│  ┌──────┐ ┌──────┐ ┌──────┐   ┌──────┐                              │
│  │f_q3c │ │f_q4m │ │f_q4sg│   │ join │  (instancia única c/u)      │
│  └──────┘ └──────┘ └──────┘   └──────┘                              │
│                                                                       │
└─────────────────────────────────────────────────────────────────────┘
```

### 4.2 Resumen de Servicios Docker Compose

| Servicio | Escalable | Variables de Entorno |
|---|---|---|
| `rabbitmq` | No (1) | `RABBITMQ_LOG_LEVELS` |
| `gateway` | No (1) | `SERVER_HOST`, `SERVER_PORT`, `MOM_HOST`, `INPUT_QUEUE`, `OUTPUT_QUEUE` |
| `usd_converter_N` | Sí | `ID`, `MOM_HOST`, `INPUT_QUEUE`, `OUTPUT_EXCHANGE`, `OUTPUT_ROUTING_KEY`, `USD_CONVERTER_AMOUNT` |
| `filter_q1_N` | Sí | `ID`, `MOM_HOST`, `INPUT_QUEUE`, `OUTPUT_QUEUE`, `FILTER_Q1_AMOUNT` |
| `filter_q2_N` | Sí | `ID`, `MOM_HOST`, `INPUT_QUEUE`, `OUTPUT_QUEUE`, `AGG_Q2_QUEUE`, `FILTER_Q2_AMOUNT`, `AGG_Q2_AMOUNT` |
| `agg_q2` | Escalable | `ID`, `MOM_HOST`, `INPUT_QUEUE`, `OUTPUT_QUEUE`, `FILTER_Q2_AMOUNT` |
| `filter_q3_avg_N` | Sí | `ID`, `MOM_HOST`, `INPUT_QUEUE`, `OUTPUT_QUEUE`, `FILTER_Q3_AVG_AMOUNT` |
| `agg_q3_avg` | Escalable | `ID`, `MOM_HOST`, `INPUT_QUEUE`, `OUTPUT_QUEUE`, `FILTER_Q3_AVG_AMOUNT` |
| `filter_q3_candidates_N` | Sí | `ID`, `MOM_HOST`, `INPUT_QUEUE`, `OUTPUT_QUEUE`, `FILTER_Q3_CAND_AMOUNT` |
| `agg_q3_candidates` | Escalable | `ID`, `MOM_HOST`, `INPUT_QUEUE`, `OUTPUT_QUEUE`, `FILTER_Q3_CAND_AMOUNT` |
| `filter_q3_comparison` | No (1) | `MOM_HOST`, `AVG_INPUT_QUEUE`, `CAND_INPUT_QUEUE`, `OUTPUT_QUEUE` |
| `filter_q4_graph_N` | Sí | `ID`, `MOM_HOST`, `INPUT_QUEUE`, `OUTPUT_EXCHANGE`, `OUTPUT_ROUTING_KEY`, `FILTER_Q4_GRAPH_AMOUNT` |
| `filter_q4_merger` | No (1) | `MOM_HOST`, `INPUT_QUEUE`, `OUTPUT_QUEUE`, `FILTER_Q4_GRAPH_AMOUNT` |
| `filter_q4_scatter_gather` | No (1) | `MOM_HOST`, `INPUT_QUEUE`, `OUTPUT_QUEUE` |
| `filter_q5_N` | Sí | `ID`, `MOM_HOST`, `INPUT_QUEUE`, `OUTPUT_QUEUE`, `AGG_Q5_QUEUE`, `FILTER_Q5_AMOUNT` |
| `agg_q5` | Escalable | `ID`, `MOM_HOST`, `INPUT_QUEUE`, `OUTPUT_QUEUE`, `FILTER_Q5_AMOUNT` |
| `join` | No (1) | `MOM_HOST`, `Q1_QUEUE`, `Q2_QUEUE`, `Q3_QUEUE`, `Q4_QUEUE`, `Q5_QUEUE`, `OUTPUT_QUEUE` |
| `client_N` | No | `INPUT_FILE`, `OUTPUT_FILE`, `SERVER_HOST`, `SERVER_PORT` |

### 4.3 Estrategia de Escalabilidad

- **Filtros de cómputo intensivo** (Q1, Q2, Q3.1, Q3.2, Q4.1, Q5): Escalan horizontalmente agregando más instancias de contenedor. RabbitMQ distribuye la carga mediante round-robin automáticamente.
- **Agregadores** (Agg Q2, Agg Q3.1, Agg Q3.2, Agg Q5): Típicamente de instancia única dado que fusionan resultados de un número acotado de instancias de filtros. Pueden escalarse particionando por `client_id % N`.
- **Join**: Instancia única. Recibe exactamente un resultado por consulta por cliente, por lo que la carga es proporcional a la cantidad de clientes, no al volumen de datos.
- **Conversor USD**: Escala horizontalmente como los filtros. Round-robin en `usd_converter_queue`.

---

## 5. Escenarios (El "+1")

Esta vista describe los casos de uso clave y cómo se trazan a través de las otras cuatro vistas.

### 5.1 Escenario: Un Único Cliente Procesa un Dataset

| Paso | Componente | Acción |
|---|---|---|
| 1 | Cliente | Lee archivo CSV, se conecta al Gateway vía TCP |
| 2 | Gateway | Asigna `client_id=1`, crea `MessageHandler(1)` |
| 3 | Cliente | Envía cada registro de transacción |
| 4 | Gateway | Serializa como `[1, datos_tx]`, publica en exchange `rk="transactions"` |
| 5 | Conversor USD | Consume, agrega `amount_usd`, publica en `rk="usd_transactions"` |
| 6 | Filtros Q1–Q5 | Cada uno consume de `rk="usd_transactions"` vía round-robin |
| 7 | Filtro Q1 | Reenvía registros donde `USD < 50` directamente al Join |
| 8 | Filtro Q2 | Acumula máximo por banco; al recibir EOF, envía a Agg Q2 |
| 9 | Agg Q2 | Fusiona máximos parciales, envía máximo final por banco al Join |
| 10 | Filtros Q3.1/Q3.2 | Acumulan promedios y almacenan candidatas; al EOF, envían a agregadores |
| 11 | Agg Q3.1/Q3.2 | Fusionan parciales, envían al Filtro Q3.3 |
| 12 | Filtro Q3.3 | Compara candidatas contra `promedio/100`, envía coincidencias al Join |
| 13 | Filtro Q4.1 | Construye grafo parcial; al EOF, publica en `rk="graph_merge"` |
| 14 | Filtro Q4.2 | Fusiona grafos, envía grafo completo a Q4.3 |
| 15 | Filtro Q4.3 | Detecta scatter-gather, envía cuentas coincidentes al Join |
| 16 | Filtro Q5 | Cuenta transacciones coincidentes; al EOF, envía a Agg Q5 |
| 17 | Agg Q5 | Suma conteos parciales, envía total al Join |
| 18 | Join | Recibe los 5 resultados para `client_id=1`, publica en `results_queue` |
| 19 | Gateway | Consume resultado, envía al Cliente vía TCP |
| 20 | Cliente | Escribe resultados en CSV de salida |

### 5.2 Escenario: Múltiples Clientes Concurrentes

| Paso | Componente | Acción |
|---|---|---|
| 1 | Cliente A, B | Se conectan simultáneamente, reciben `client_id=1` y `client_id=2` |
| 2 | Gateway | Usa pool de procesos para manejar ambos clientes en paralelo |
| 3 | Todos los filtros | Mantienen estado por cliente: `state[1]` y `state[2]` son independientes |
| 4 | Cliente A finaliza | EOF para `client_id=1` se propaga por el pipeline |
| 5 | Filtros | Envían solo `state[1]`; `state[2]` sigue acumulando |
| 6 | Join | Recibe resultados para `client_id=1`, envía al Gateway |
| 7 | Gateway | Asocia resultado al socket del Cliente A, envía respuesta |
| 8 | Cliente B finaliza | Mismo flujo para `client_id=2` |

**Invariante clave**: `client_id` se transporta en cada mensaje interno, asegurando aislamiento completo entre clientes concurrentes a lo largo de todo el pipeline.

### 5.3 Escenario: Escalamiento Horizontal Bajo Carga

| Paso | Componente | Acción |
|---|---|---|
| 1 | Operador | Agrega `filter_q2_3` al docker-compose, configura `FILTER_Q2_AMOUNT=4` |
| 2 | Todas las instancias Q2 | Consumen de `filter_q2_queue` — RabbitMQ redistribuye round-robin entre 4 consumidores |
| 3 | Llega EOF | Solo una instancia Q2 lo recibe |
| 4 | Exchange fanout | Difunde EOF a las 4 instancias (incluida la nueva) |
| 5 | Todas las instancias Q2 | Envían máximo parcial por banco a Agg Q2 |
| 6 | Agg Q2 | Ahora espera `FILTER_Q2_AMOUNT=4` EOFs en vez de 3 |
| 7 | Resultado | Salida correcta con mayor paralelismo |

### 5.4 Escenario: Cierre Graceful (SIGTERM)

| Paso | Componente | Acción |
|---|---|---|
| 1 | Docker/Orquestador | Envía SIGTERM a `filter_q2_1` |
| 2 | Filtro Q2.1 | El manejador de SIGTERM llama a `stop_consuming()` en el middleware |
| 3 | Middleware | Interrumpe de forma segura `start_consuming()` vía `add_callback_threadsafe` |
| 4 | Filtro Q2.1 | Sale del bucle principal, entra al bloque `finally` |
| 5 | Filtro Q2.1 | Cierra todas las conexiones de middleware, termina |
| 6 | RabbitMQ | Los mensajes no ackeados se reencolan; las instancias Q2 restantes continúan procesando |

### 5.5 Escenario: Fallo del Conversor USD

| Paso | Componente | Acción |
|---|---|---|
| 1 | Conversor USD | Se crashea o pierde la conexión con RabbitMQ |
| 2 | RabbitMQ | Los mensajes en `usd_converter_queue` permanecen durables y persistidos |
| 3 | Conversores restantes | Continúan consumiendo; sin pérdida de datos |
| 4 | Si todos los conversores caen | Los mensajes se acumulan en `usd_converter_queue` hasta que un conversor se reinicie |
| 5 | Filtros downstream | Inactivos (sin mensajes nuevos), pero no se crashean; esperan nuevos datos o EOF |

---

## Apéndice: Resumen de Topología RabbitMQ

### Exchanges

| Exchange | Tipo | Propósito |
|---|---|---|
| `transactions` | direct | Gateway → Conversor USD (rk=`transactions`) |
| `usd_transactions` | direct | Conversor USD → todos los filtros (rk=`usd_transactions`) |
| `graph_merge` | direct | Instancias de grafo Q4 → fusionador Q4 (rk=`graph_merge`) |
| `filter_q2_bcast` | fanout | Difusión de EOF entre instancias del filtro Q2 |
| `filter_q3_avg_bcast` | fanout | Difusión de EOF entre instancias del filtro Q3.1 |
| `filter_q3_cand_bcast` | fanout | Difusión de EOF entre instancias del filtro Q3.2 |
| `filter_q4_graph_bcast` | fanout | Difusión de EOF entre instancias del filtro Q4.1 |
| `filter_q5_bcast` | fanout | Difusión de EOF entre instancias del filtro Q5 |

### Colas

| Cola | Consumidor | Productor |
|---|---|---|
| `usd_converter_queue` | Conversor USD | Gateway (vía exchange `transactions`) |
| `filter_q1_queue` | Instancias Filtro Q1 | Conversor USD (vía exchange `usd_transactions`) |
| `filter_q2_queue` | Instancias Filtro Q2 | Conversor USD (vía exchange `usd_transactions`) |
| `filter_q3_avg_queue` | Instancias Filtro Q3.1 | Conversor USD (vía exchange `usd_transactions`) |
| `filter_q3_cand_queue` | Instancias Filtro Q3.2 | Conversor USD (vía exchange `usd_transactions`) |
| `filter_q4_graph_queue` | Instancias Filtro Q4.1 | Conversor USD (vía exchange `usd_transactions`) |
| `filter_q5_queue` | Instancias Filtro Q5 | Conversor USD (vía exchange `usd_transactions`) |
| `agg_q2_queue` | Agg Q2 | Instancias Filtro Q2 |
| `agg_q3_avg_queue` | Agg Q3.1 | Instancias Filtro Q3.1 |
| `agg_q3_cand_queue` | Agg Q3.2 | Instancias Filtro Q3.2 |
| `filter_q3_comparison_queue` | Filtro Q3.3 | Agg Q3.1, Agg Q3.2 |
| `filter_q4_merger_queue` | Filtro Q4.2 | Instancias Filtro Q4.1 (vía exchange `graph_merge`) |
| `filter_q4_sg_queue` | Filtro Q4.3 | Filtro Q4.2 |
| `join_queue` | Join | Filtro Q1, Agg Q2, Filtro Q3.3, Filtro Q4.3, Agg Q5 |
| `results_queue` | Gateway | Join |
