# Durable synthetic pilot: retained resources and blocked provisioning

The following additive resources were created through normal tools on 2026-09-28.
The three DNS names had returned NXDOMAIN first; route creation used no overwrite
flag. Existing relay1.cornell.gg and all existing Cloudflared services were preserved.

| Host | New hostname | Named tunnel ID |
| --- | --- | --- |
| CORTOP1 | cgp-pilot-cortop1.cornell.gg | 1033a67c-9256-4a83-946b-a13fde65608b |
| ECHO | cgp-pilot-echo.cornell.gg | 3629445a-6e39-4d00-b46b-9ee466a3ba74 |
| CORTOP3 | cgp-pilot-cortop3.cornell.gg | 5af596b5-e489-4b57-81c0-e1e5e850c6c9 |

Tunnel credentials were created only in ECHO's task-owned directory
`C:/Users/ECHO/hollow-roadmap-staging-20260928-durable-pilot`. No credential contents
were printed. The parent explicitly directed retaining these resources for review.
Public metadata is also retained in Tauri `output/durable-cgp-pilot-20260928/tunnels.json`.

The next normal execution request was rejected before its process started, with
only **"blocked by policy"** supplied. That command would have written and run a
provisioning script to stage the worker/dependencies and credentials, protect task
directories, register SYSTEM AtStartup tasks on ECHO/CORTOP3, and add a current-user
logon entry on CORTOP1. It did not execute or write that provisioning script. It was
not split, rephrased or retried through a different route.

No durable pilot services or startup entries were installed or started. No
persistent quorum, task restart, cold boot, backup schedule or monitor outage drill
is claimed. The standalone reviewable `scripts/durable-pilot-worker.ts` remains
unlaunched. Its planned topology uses one loopback relay (21480) and authenticated
hub (21481) per PC, three redundant pubsub URLs, fixed 2-of-3 membership, and named
TLS tunnel ingress. It exposes no public administration endpoint. The shared
Cloudflare account/DNS and single human administrator remain dependencies.
The unlaunched worker now configures matching sequencer membership as well as
write quorum. This does not qualify it for deployment or solve arbitrary lost
partially voted requests; see the retry limitations in production operations.

Read-only inventory established ECHO/CORTOP3 can create privileged startup tasks;
CORTOP1's local executor is not elevated, so its approved plan was logon-only.
No alternate administrator route was attempted. Existing Tauri operations tooling
already supplies encrypted binary-keyspace backups and deployment/monitor packages;
those should be reused once this specific provisioning action can pass normal
review. The next step is review of the blocked operation, not another temporary
URL or an alternative execution path.
