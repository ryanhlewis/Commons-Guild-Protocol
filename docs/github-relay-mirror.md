# GitHub Relay Mirror

CGP relay mirroring uses the relay log as the source of truth. GitHub is a portable mirror and bootstrap source, not a second consensus system.

## Relay Plugin

`createGitHubRelayMirrorPlugin` exposes `cgp.github.mirror`.

Routes:

- `GET /plugins/cgp.github.mirror/status`
- `GET /plugins/cgp.github.mirror/manifest`
- `GET /plugins/cgp.github.mirror/files/<path>`
- `POST /plugins/cgp.github.mirror/flush`
- `POST /plugins/cgp.github.mirror/ingest`

Write routes require `CGP_GITHUB_MIRROR_ADMIN_TOKEN` unless the relay is explicitly configured with `allowUnauthenticatedHttpWrites` for local tests.

Environment:

- `CGP_GITHUB_MIRROR_AUTO=1`
- `CGP_GITHUB_MIRROR_FREQUENCY=manual|per-event|batch|interval`
- `CGP_GITHUB_MIRROR_BATCH_SIZE=50`
- `CGP_GITHUB_MIRROR_INTERVAL_MS=86400000`
- `CGP_GITHUB_MIRROR_DIR=./relay-github-mirror`
- `CGP_GITHUB_MIRROR_BASE_PATH=cgp/backups/main-relay`
- `CGP_GITHUB_MIRROR_REPOSITORY=owner/repo`
- `CGP_GITHUB_MIRROR_BRANCH=main`
- `CGP_GITHUB_MIRROR_TOKEN=<github token>`
- `CGP_GITHUB_MIRROR_APP_ID=<github app id>`
- `CGP_GITHUB_MIRROR_APP_INSTALLATION_ID=<installation id>`
- `CGP_GITHUB_MIRROR_APP_PRIVATE_KEY_FILE=<path to PEM>`
- `CGP_GITHUB_MIRROR_SOURCES=<manifest-or-jsonl-url,...>`

`CGP_GITHUB_MIRROR_TOKEN` is the local/dev fallback. Production backup flows should prefer GitHub App installation auth:

- The app is installed on exactly one backup repository, or on a selected set of repositories.
- The relay mints short-lived installation tokens with `contents:write`.
- The relay never needs a broad user token or access to unrelated repositories.

## Single Repository GitHub App Setup

The helper below uses the local `gh` login to optionally create one private repository, then starts a localhost setup page for GitHub's App Manifest flow:

```bash
npm run github:backup-app -- -- --repo hollow-backup --create-repo
```

If GitHub rejects `127.0.0.1` manifest URLs, expose the helper through a
temporary HTTPS tunnel and pass that public origin:

```bash
cloudflared tunnel --url http://127.0.0.1:17876
npm run github:backup-app -- -- --repo hollow-backup --public-url https://example.trycloudflare.com
```

The GitHub App Manifest flow still requires a browser confirmation on GitHub. That is a GitHub safety boundary: GitHub creates the app after the user confirms the manifest, then redirects back to the local helper with a temporary code. The helper exchanges that code for the app id, private key, and webhook secret, then writes:

- `.github-app/github-app.json`
- `.github-app/github-app.private-key.pem`
- `.github-app/cgp-github-mirror.env`

The generated env file also includes commented `CGP_IPFS_FAUX_GITHUB_APP_*`
lines. Enable them when the same selected repo should store small
content-addressed bootstrap objects for `cgp.ipfs.faux`.

When installing the app, choose **Only select repositories** and select the backup repo. GitHub redirects back with `installation_id`; the helper writes that into the env file.

After installation, verify that the relay plugin can mint an installation token
and upload a real CGP mirror chunk:

```bash
npm run github:backup-check
```

## Hosted Hollow Workflow

The local `gh` path is only for developer setup. In a normal Hollow product flow,
repo creation is a short-lived user-authorized GitHub step:

1. The user chooses **Create backup repo** or **Use existing repo** in Hollow.
2. For a new user-owned private repo, Hollow asks GitHub for a user-scoped authorization that can call `POST /user/repos`. GitHub documents this endpoint as requiring `repo` scope for private repos with classic OAuth/PAT tokens, and it also supports fine-grained token types including GitHub App user access tokens.
3. Hollow creates `hollow-backup` or another user-approved name with a README.
4. Hollow sends the user to install the Hollow Backup GitHub App on **Only select repositories**, scoped to that one repo.
5. After GitHub returns `installation_id`, Hollow stores only the repo name, app id, installation id, and encrypted app key material in the relay/operator config.
6. From then on, relay writes use short-lived GitHub App installation tokens with repository `contents:write`; the repo-creation user token is discarded.

For organization-owned repos, Hollow should either ask the user to select an
existing repo or use the organization repository creation endpoint only after
GitHub confirms the user has the required organization permissions.

For a fully hosted Hollow flow, the same sequence should run through a Hollow-owned setup endpoint instead of the local helper:

1. Create or choose a backup repository.
2. Register/install the Hollow Backup GitHub App.
3. Store `installation_id`, `owner/repo`, branch, and base path in the relay/plugin config.
4. Use app installation tokens for all mirror writes.

## Format

The plugin writes:

- `manifest.json`: `cgp.github-relay-mirror.v1`
- `chunks/*.jsonl`: append-only signed CGP events

Every restored event is checked for:

- event id hash
- secp256k1 signature
- seq/prevHash chain continuity
- target relay prefix compatibility

If the target relay already has a divergent event at the same sequence, ingest fails instead of overwriting.

## External Bridge Plugins

Bridge plugins for third-party systems are outside the CGP core protocol. The GitHub mirror plugin does not parse third-party exports. It mirrors whatever signed CGP events a relay plugin appends to the relay log.

Bridge-originated events should be appended with `RelayPluginContext.appendEventsFromPlugin`, which routes them through relay sequencing, broadcast, and append hooks so mirrors and other plugins see the same events as normal CGP publishes.

Third-party media should remain metadata references:

- `url` for still-live HTTPS sources
- `ipfs://<cid>` when a media plugin pins the bytes
- optional hashes, mime, dimensions, size, and original attachment metadata

GitHub mirrors should not store large media blobs. Media bytes belong in `cgp.ipfs.helia`, `cgp.media.storage`, static shards, or another content-addressed storage plugin.

## Account Backups

A relay can mirror all logs it hosts, or a configured set of guild IDs. A true account-wide backup needs a client-authorized job because only the client can prove which private servers and DMs it can read. That job should export readable CGP events, encrypted DM payloads when needed, and media references, then publish the same mirror manifest/chunks to GitHub.
