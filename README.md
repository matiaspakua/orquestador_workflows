# Orquestador de Workflows

## Descripción
Sistema de orquestación de workflows que permite la ejecución de flujos de trabajo basados en eventos. Utiliza una arquitectura basada en microservicios con Kafka para mensajería y PostgreSQL para almacenamiento de datos.

## Arquitectura
El sistema está compuesto por los siguientes componentes:
- **Web Application**: Interfaz de usuario para gestionar workflows
- **Workflow Engine**: Motor de ejecución de workflows
- **Kafka**: Sistema de mensajería para comunicación entre componentes
- **PostgreSQL**: Base de datos para almacenamiento de datos y resultados

## Requisitos
- Docker
- Docker Compose

## Configuración
1. Clonar el repositorio:
```bash
git clone https://github.com/yourusername/orquestador_workflows.git
cd orquestador_workflows
```

2. Configurar variables de entorno:
```bash
cp .env.example .env
```
Editar el archivo .env con los valores apropiados.

## Ejecución
1. Iniciar los servicios:
```bash
docker-compose up -d
```

2. Verificar el estado de los servicios:
```bash
docker-compose ps
```

## Servicios Disponibles
| Servicio | URL | Descripción |
|----------|-----|-------------|
| Kafka UI | http://localhost:8080 | Interfaz de monitoreo de Kafka |
| Web UI | http://localhost:5000 | Interfaz de usuario principal |
| pgAdmin | http://localhost:8081 | Administración de PostgreSQL |

## Estructura del Proyecto
```
orquestador_workflows/
├── consumer/           # Consumidor de eventos
├── producer/          # Productor de eventos
├── ui/               # Interfaz web
├── diagrams/         # Diagramas de arquitectura
├── scripts/          # Scripts de inicialización
└── docker-compose.yml # Configuración de servicios
```

## Monitoreo
- **Kafka UI**: Monitoreo de topics y mensajes
- **pgAdmin**: Administración de base de datos
- **Logs**: Disponibles a través de `docker-compose logs`

## Desarrollo
Para ejecutar en modo desarrollo:
```bash
docker-compose -f docker-compose.yml -f docker-compose.dev.yml up -d
```

## Pruebas
```bash
docker-compose -f docker-compose.test.yml up --build
```

## TODO

1. Agregar Grafana para telemetria de los componentes (no logs, sino telemetria funcional)
2. Agregar Prometheus para logs y monitoreo (logs técnicos)
3. Definir como funciona el orquestador y los workflows
4. Hacer una pruebas con el producer y el consumer usando el orquestador.
5. Implementar una UI para ver el progreso de ejecución del orquestador.