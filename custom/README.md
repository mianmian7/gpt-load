# Custom build of gpt-load

This branch carries a small set of local patches on top of an upstream
release tag of [tbphp/gpt-load](https://github.com/tbphp/gpt-load).

## Branch layout

| Ref | Meaning |
| --- | --- |
| `upstream` remote | `tbphp/gpt-load`, fetch only (push URL is `DISABLED`) |
| `v2.0.0-rc.61` | upstream release tag the patches are based on |
| `custom/grok-media` | upstream tag + the patches below + this `custom/` directory |

Patches (one commit each, oldest first):

1. `feat(grok): proxy xAI video and image endpoints` — `/v1/videos*` routes,
   native OpenAI Images routes for the Grok channel, embedded Grok executor
   support, updated route snapshot tests.
2. `feat(codex): allow gpt-image-2.6 models on the direct image route` —
   works around the `gpt-image-2.5` whitelist in embedded CLIProxyAPI.
3. `chore(custom): ...` — this directory (build and smoke-test scripts).

List them with `git log --oneline <base-tag>..custom/grok-media`.

## Build an image

```bash
custom/build-image.sh                      # -> gpt-load:v2.0.0-rc.61-grok-media-<sha>
custom/smoke-test.sh gpt-load:v2.0.0-rc.61-grok-media-<sha>
```

`build-image.sh` uses the upstream Dockerfile's default `source-build` target,
so only Docker/buildx is needed on the host. It refuses to build with
uncommitted tracked changes (override with `ALLOW_DIRTY=1`) and labels the
image with the git revision (`docker image inspect --format '{{json .Config.Labels}}'`).

`smoke-test.sh` runs a throwaway container on `127.0.0.1:13001`, checks
`/health`, and checks that the custom routes answer 401 (registered, auth
required) while an unknown path answers 404.

## Deploy

1. Build and smoke-test a new image tag (above). Tags include the commit sha,
   so building never retags the image a running container uses.
2. In the deployment's compose file, change `image:` to the new tag. Keep a
   copy of the old file; the old image tag stays available for rollback.
3. `docker compose up -d` in the deployment directory, then check
   `curl -fsS http://127.0.0.1:<port>/health` reports the new `version`.

Rollback: put the previous `image:` tag back and run `docker compose up -d`.

## Upgrade to a newer upstream release

```bash
git fetch upstream --tags
git switch -c custom/grok-media-<new-tag> custom/grok-media
git rebase --onto <new-tag> <old-tag>          # replay only the local patches
go test -count=1 . ./internal/...              # route snapshot tests may need refreshing
(cd third_party/cpaembedded && go test -count=1 ./embedded/...)
custom/build-image.sh && custom/smoke-test.sh <image>
```

Before rebasing, check whether upstream already ships an equivalent feature;
drop the local commit if it does. When the new branch is deployed and
verified, move `custom/grok-media` to it (`git branch -f custom/grok-media
custom/grok-media-<new-tag>`).
