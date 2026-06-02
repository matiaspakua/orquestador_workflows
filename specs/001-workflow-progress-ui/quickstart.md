# Quickstart: Workflow Progress UI

## Access the Dashboard

```
http://localhost:5000/workflows
```

The dashboard lists all workflow executions with status, start time, and duration. Click any execution to view its step-by-step progress.

If the dashboard does not load, ensure all containers are running:

```bash
docker compose ps
docker compose up -d
```

## Run Tests

### Unit and Integration Tests (pytest)

```bash
# From repository root — run Flask test client tests
python -m pytest ui/tests/ -v

# Run with coverage
python -m pytest ui/tests/ --cov=ui
```

### End-to-End Tests (Selenium)

```bash
# Requires web-ui and postgres containers running
python -m pytest ui/tests/e2e/ -v
```

## How to Add a New Workflow View

1. Add a route in `ui/app.py`:
   ```python
   @app.route('/workflows/<id>/<new_view>')
   def workflow_new_view(id):
       execution = get_workflow_execution(id)
       return render_template('workflow_new_view.html', execution=execution)
   ```

2. Create the template `ui/templates/workflow_new_view.html` extending the base layout:
   ```jinja2
   {% extends "base.html" %}
   {% block content %}
   <!-- Custom view content -->
   {% endblock %}
   ```

3. Add a link from the workflow list or detail page.

4. Write tests in `ui/tests/` following the existing pattern.

## Troubleshooting

### "Orquestador desconectado" state

**Cause**: The orchestrator backend is not running or unreachable.

**Symptom**: The dashboard shows a "Desconectado" banner at the top. Status indicators show stale timestamps. Auto-refresh switches to polling mode.

**Fix**:
```bash
# Verify orchestrator container status
docker compose logs orchestrator

# Restart if needed
docker compose restart orchestrator
```

### Event stream unavailable

**Cause**: Kafka or the orchestrator event producer is down.

**Symptom**: SSE connection fails; page switches to polling. A visual indicator shows "Eventos no disponibles — usando modo polling".

**Fix**:
```bash
# Check Kafka health
docker compose ps kafka

# View Kafka logs
docker compose logs kafka | tail -20
```

### Detail view shows "Waiting for execution to begin"

**Cause**: The workflow execution was created but has not started processing yet.

**Action**: No action needed — the view will update automatically via SSE or polling when steps are recorded.

### Empty filtered results

**Cause**: The filter combination matches no executions.

**Symptom**: The list shows "No se encontraron ejecuciones" with a suggestion to adjust filters.

### Pagination shows fewer items than expected

**Default**: 50 items per page. Verify via query:

```sql
SELECT COUNT(*) FROM workflow_executions;
```

If the count is wrong, the orchestrator may not have written executions to PostgreSQL yet.
