# driftwatch

> CLI tool that detects configuration drift between deployed services and their declared specs

---

## Installation

```bash
pip install driftwatch
```

Or install from source:

```bash
git clone https://github.com/yourorg/driftwatch.git && cd driftwatch && pip install -e .
```

---

## Usage

Point `driftwatch` at your spec file and a running service to check for drift:

```bash
driftwatch check --spec services/api.yaml --target production
```

Compare multiple services at once:

```bash
driftwatch check --spec services/ --target staging --output table
```

Example output:

```
Service        Field              Expected        Actual          Status
─────────────────────────────────────────────────────────────────────────
api-gateway    replicas           3               2               DRIFT
auth-service   image.tag          v1.4.2          v1.4.0          DRIFT
worker         env.LOG_LEVEL      INFO            DEBUG           DRIFT
cache          replicas           2               2               OK
```

Exit code is `1` if any drift is detected, `0` if all services match their specs. This makes `driftwatch` easy to integrate into CI pipelines:

```bash
driftwatch check --spec services/ --target production || echo "Drift detected — review before deploying"
```

Run `driftwatch --help` to see all available commands and options.

---

## Output Formats

Use `--output` to control how results are displayed:

| Format  | Flag               | Description                        |
|---------|--------------------|---------------------------------|
| table   | `--output table`   | Human-readable table (default)     |
| json    | `--output json`    | Machine-readable JSON              |
| quiet   | `--output quiet`   | Exit code only, no output          |

---

## Ignoring Fields

To suppress known or expected differences, use a `.driftigore` file in your project root or pass `--ignore` flags directly:

```bash
driftwatch check --spec services/ --target production --ignore env.DEBUG --ignore metadata.annotations
```

Or define ignored fields in `.driftigore`:

```
env.DEBUG
metadata.annotations
metadata.labels.deploy-timestamp
```

Ignored fields are excluded from drift detection and will not affect the exit code.

---

## License

This project is licensed under the [MIT License](LICENSE).
