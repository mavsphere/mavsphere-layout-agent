# Security Policy

## Supported versions

| Repository | Supported |
|---|---|
| mavsphere-backend (latest main) | Yes |
| mavsphere-layout-agent (latest main) | Yes |
| mavsphere-sensor-node (latest main) | Yes |
| All others | Best effort |

## Reporting a vulnerability

**Do not open a public GitHub issue for security vulnerabilities.**

MavSphere components control physical vehicles and hardware. A vulnerability in the control authority, authentication, or safety systems could have real-world consequences. Please treat all such findings responsibly.

Email: **security@mavsphere.com**

Include:
- Which repository and component is affected
- A description of the vulnerability and potential impact
- Steps to reproduce or a proof of concept
- Your name/handle if you would like to be credited

You can expect an acknowledgement within 48 hours and a resolution timeline within 7 days of triage. We will credit researchers in release notes unless you prefer to remain anonymous.

## Scope of particular concern

Given the nature of the platform, we treat the following as critical priority:

- Authentication or session bypass
- Control claim registry manipulation (ability to seize control of a vehicle)
- Dead-man failsafe bypass
- Agent authentication bypass (spoofing a layout or MAV agent)
- Privilege escalation between user roles
