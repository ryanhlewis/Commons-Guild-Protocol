# Faux IPFS Backends

`cgp.ipfs.faux` lets a relay expose IPFS-shaped CGP storage while using
operator-selected storage behind the scenes. It is intended for serverless and
managed-storage relays where running libp2p/Helia is not practical.

The backend still returns content-addressed `ipfs://<cid>` attachment metadata,
but responses include `syntheticIpfs: true` in storage metadata and
`X-CGP-Faux-IPFS: 1` on HTTP reads. Use real `cgp.ipfs.helia` or Kubo when the
object must be advertised on the public IPFS network.

## R2

```powershell
$env:CGP_RELAY_PLUGINS='cgp.ipfs.faux,cgp.media.storage'
$env:CGP_IPFS_FAUX_STORAGE='r2'
$env:CGP_IPFS_FAUX_R2_ACCOUNT_ID='<cloudflare-account-id>'
$env:CGP_IPFS_FAUX_R2_BUCKET='hollow-media'
$env:CGP_IPFS_FAUX_R2_ACCESS_KEY_ID='<r2-access-key-id>'
$env:CGP_IPFS_FAUX_R2_SECRET_ACCESS_KEY='<r2-secret-access-key>'
$env:CGP_IPFS_FAUX_KEY_PREFIX='media'
$env:CGP_IPFS_FAUX_R2_PUBLIC_BASE_URL='https://media.example.com'
```

If `CGP_IPFS_FAUX_R2_PUBLIC_BASE_URL` is omitted, the relay can still serve
objects through `/plugins/cgp.ipfs.faux/ipfs/<cid>` using signed R2 reads.

## S3-Compatible Storage

```powershell
$env:CGP_IPFS_FAUX_STORAGE='s3'
$env:CGP_IPFS_FAUX_S3_ENDPOINT='https://s3.us-east-1.amazonaws.com'
$env:CGP_IPFS_FAUX_S3_BUCKET='hollow-media'
$env:CGP_IPFS_FAUX_S3_REGION='us-east-1'
$env:CGP_IPFS_FAUX_S3_ACCESS_KEY_ID='<access-key-id>'
$env:CGP_IPFS_FAUX_S3_SECRET_ACCESS_KEY='<secret-access-key>'
$env:CGP_IPFS_FAUX_KEY_PREFIX='media'
```

For S3-compatible providers that require path-style URLs, set:

```powershell
$env:CGP_IPFS_FAUX_S3_FORCE_PATH_STYLE='1'
```

## GitHub

```powershell
$env:CGP_IPFS_FAUX_STORAGE='github'
$env:CGP_IPFS_FAUX_GITHUB_REPOSITORY='owner/repo'
$env:CGP_IPFS_FAUX_GITHUB_BRANCH='main'
$env:CGP_IPFS_FAUX_GITHUB_TOKEN='<contents-write-token>'
$env:CGP_IPFS_FAUX_GITHUB_BASE_PATH='ipfs'
```

For scoped production setup, prefer the same single-repository GitHub App used
by `cgp.github.mirror`:

```powershell
$env:CGP_IPFS_FAUX_STORAGE='github'
$env:CGP_IPFS_FAUX_GITHUB_REPOSITORY='owner/repo'
$env:CGP_IPFS_FAUX_GITHUB_BRANCH='main'
$env:CGP_IPFS_FAUX_GITHUB_BASE_PATH='ipfs'
$env:CGP_IPFS_FAUX_GITHUB_APP_ID='<github-app-id>'
$env:CGP_IPFS_FAUX_GITHUB_APP_INSTALLATION_ID='<installation-id>'
$env:CGP_IPFS_FAUX_GITHUB_APP_PRIVATE_KEY_FILE='<path-to-pem>'
```

If the faux-IPFS-specific app variables are omitted, the backend falls back to
`CGP_GITHUB_MIRROR_APP_*` and then `CGP_GITHUB_APP_*`, so one installed app can
write both mirror chunks and small content-addressed bootstrap objects.

GitHub mode writes one content-addressed object per CID using the Contents API.
It is useful for small, public, reproducible artifacts and bootstrap mirrors, not
large media at production scale.

## Generic PUT/GET URLs

Operators that already have signed URLs or an object gateway can use templates:

```powershell
$env:CGP_IPFS_FAUX_STORAGE='r2'
$env:CGP_IPFS_FAUX_PUT_URL_TEMPLATE='https://upload.example.com/{key}'
$env:CGP_IPFS_FAUX_GET_URL_TEMPLATE='https://cdn.example.com/{key}'
$env:CGP_IPFS_FAUX_KEY_PREFIX='media'
```

Supported template fields are `{cid}`, `{sha256}`, `{key}`, `{name}`, and
`{bytes}`.
