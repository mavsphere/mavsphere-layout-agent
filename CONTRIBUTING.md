# Contributing to MavSphere

Thank you for your interest in contributing. MavSphere is a platform for shared remote
control of model railways and autonomous vehicles, developed and maintained by a single
developer alongside full-time employment.

**Please set realistic expectations.** Pull request review may take days or weeks.
Opening an issue before starting significant work is strongly recommended so effort
is not wasted on something that cannot be merged.

**Hosted instance access:** Both the railway and MAV platforms are currently
invite-only. To request access contact hello@mavsphere.com.

---

## What's open source

The hardware-side components are fully open source under the MIT licence.
The platform backend and UI are proprietary and closed source.

| Repository | Licence |
|---|---|
| `mavsphere-layout-agent` | MIT — no restrictions |
| `mavsphere-layout-simulator` | MIT — no restrictions |
| `mavsphere-sensor-node` | MIT — no restrictions |
| `mavsphere-mav-agent` | MIT — no restrictions |

The backend and UI are not publicly available. UI and platform suggestions are still
welcome — open an issue on the most relevant public repo or email hello@mavsphere.com.

---

## Patent notice

The core session and control authority architecture is the subject of a UK patent
priority application filed by Mavsphere Limited. Contributing to this project does
not grant you any rights to that patent. If you are unsure whether your contribution
touches the core architecture, please open an issue to discuss it first.

---

## Good first contributions

- **New command station integrations** — the `CommandStation` interface in
  `mavsphere-layout-agent` is designed for extension; adding support for Lenz, Z21,
  NCE, or other DCC systems is entirely self-contained within the agent
- **Sensor node firmware** — additional hardware profiles, new sensor types,
  alternative ESP32 board support
- **Tests** — the layout agent and simulator have no test coverage; test PRs are
  very welcome and the highest-value contribution in either repo
- **Documentation** — corrections, hardware guides, wiring diagrams

---

## Development setup

Each repository has its own README with full local setup instructions:

- **mavsphere-layout-agent** — Go 1.22+
- **mavsphere-layout-simulator** — Go 1.22+
- **mavsphere-sensor-node** — PlatformIO, ESP32
- **mavsphere-mav-agent** — Go 1.22+

To run the full platform locally without physical hardware, start with
`mavsphere-layout-simulator` — it runs a virtual layout that the backend and UI
treat identically to a real one.

---

## Pull request process

1. Open an issue first for anything significant.
2. Fork the repository and create a feature branch from `main`.
3. Make focused commits. Add or update tests for logic you change.
4. Ensure `go build ./...`, `go vet ./...`, and `gofmt` pass before pushing.
5. Open a pull request against `main` with a clear description of what and why.

### Commit style

```
feat: add Z21 command station adapter
fix: correct BFS traversal at double-slip
docs: add sensor node wiring diagram
test: add throttle guard unit tests
chore: update Go dependencies
```

### Code style

- **Go** — `gofmt` formatted, standard Go conventions
- **C++ (sensor node)** — follow the existing Arduino-style conventions in the repo

---

## Reporting bugs

Open a GitHub issue with: repository and component, steps to reproduce, expected vs
actual behaviour, relevant logs or screenshots.

For **security vulnerabilities** do not open a public issue — see
[SECURITY.md](SECURITY.md).

---

## Questions

Open a GitHub issue tagged `question`.