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

Run `driftwatch --help` to see all available commands and options.

---

## License

This project is licensed under the [MIT License](LICENSE).