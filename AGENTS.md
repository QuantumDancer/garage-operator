# garage-operator - AI Agent Guide

## Project Structure

**Single-group layout (default):**

```
cmd/main.go                    Manager entry (registers controllers/webhooks)
api/<version>/*_types.go       CRD schemas (+kubebuilder markers)
api/<version>/zz_generated.*   Auto-generated (DO NOT EDIT)
internal/controller/*          Reconciliation logic
internal/webhook/*             Validation/defaulting (if present)
config/crd/bases/*             Generated CRDs (DO NOT EDIT)
config/rbac/role.yaml          Generated RBAC (DO NOT EDIT)
config/samples/*               Example CRs (edit these)
Makefile                       Build/test/deploy commands
PROJECT                        Kubebuilder metadata Auto-generated (DO NOT EDIT)
```

**Multi-group layout** (for projects with multiple API groups):

```
api/<group>/<version>/*_types.go       CRD schemas by group
internal/controller/<group>/*          Controllers by group
internal/webhook/<group>/<version>/*   Webhooks by group and version (if present)
```

Multi-group layout organizes APIs by group name (e.g., `batch`, `apps`). Check the `PROJECT` file for `multigroup: true`.

**To convert to multi-group layout:**

1. Run: `kubebuilder edit --multigroup=true`
2. Move APIs: `mkdir -p api/<group> && mv api/<version> api/<group>/`
3. Move controllers: `mkdir -p internal/controller/<group> && mv internal/controller/*.go internal/controller/<group>/`
4. Move webhooks (if present): `mkdir -p internal/webhook/<group> && mv internal/webhook/<version> internal/webhook/<group>/`
5. Update import paths in all files
6. Fix `path` in `PROJECT` file for each resource
7. Update test suite CRD paths (add one more `..` to relative paths)

## Critical Rules

### Never Edit These (Auto-Generated)

- `config/crd/bases/*.yaml` - from `make manifests`
- `config/rbac/role.yaml` - from `make manifests`
- `config/webhook/manifests.yaml` - from `make manifests`
- `**/zz_generated.*.go` - from `make generate`
- `internal/garageadmin/zz_generated.client.go` - from `make generate-client` (oapi-codegen)
- `internal/garageadmin/openapi/garage-admin-v2.openapi30.json` - from `make generate-client` (down-converted spec)
- `PROJECT` - from `kubebuilder [OPTIONS]`

### Never Remove Scaffold Markers

Do NOT delete `// +kubebuilder:scaffold:*` comments. CLI injects code at these markers.

### Keep Project Structure

Do not move files around. The CLI expects files in specific locations.

### Always Use CLI Commands

Always use `kubebuilder create api` and `kubebuilder create webhook` to scaffold. Do NOT create files manually.

### E2E Tests Require an Isolated Kind Cluster

The e2e tests are designed to validate the solution in an isolated environment (similar to GitHub Actions CI).
Ensure you run them against a dedicated [Kind](https://kind.sigs.k8s.io/) cluster (not your “real” dev/prod cluster).

### Humans Own the User-Facing Surface

This operator is maintained agent-first, but everything a **user** reads, writes, or
installs against is owned by a human. An AI agent must **not** author or edit these — the
maintainer does, so the product feels natural for humans to use:

- **User-facing documentation.** `README.md` and (future) `docs/` are off-limits. Do not
  create, rewrite, or "improve" them. If docs are out of date, flag it; do not fix it.
- **The user-facing API.** This is the contract users depend on:
  - **CRD shape** — the spec/status fields users write in their CRs (field names, types,
    defaults, enums, structure, comments that become CRD descriptions).
  - **The Helm chart** (when it exists) — values schema, templates, defaults.

  Do not invent or restructure these surfaces on your own.

**The one concession (CRDs).** The maintainer may hand you **example CRs** (YAML showing how
a `GarageCluster` / `GarageBucket` / `GarageKey` should look). From those examples you may
infer and write the Go types in `api/<version>/*_types.go` (and their kubebuilder markers)
so that `make manifests` regenerates CRDs matching the examples. You are implementing a
human-specified shape, not designing it. If an example is ambiguous or incomplete, ask —
do not fill the gap with your own API design.

Everything _behind_ this surface — controllers, the Admin API client, reconciliation,
finalizers, status logic, tests, build/CI — remains fully agent-owned.

## After Making Changes

**After editing `*_types.go` or markers:**

```
make manifests  # Regenerate CRDs/RBAC from markers
make generate   # Regenerate DeepCopy methods
```

**After editing `*.go` files:**

```
make lint-fix   # Auto-fix code style
make test       # Run unit tests
```

## Garage Admin API v2 Client (`internal/garageadmin`)

The operator never `kubectl exec`s into Garage — all cluster/bucket/key operations go
through Garage's **Admin API v2** over HTTP. The `internal/garageadmin` package is the
shared, typed client every controller uses.

### Layout

```
internal/garageadmin/
  client.go                          Operator-facing wrapper (hand-written, EDIT THIS)
  client_test.go                     Wrapper test against an httptest fake server
  generate.go                        go:generate directives + package doc
  oapi-codegen.yaml                  oapi-codegen config
  zz_generated.client.go             Generated client (DO NOT EDIT)
  openapi/
    garage-admin-v2.json             Vendored upstream spec, OpenAPI 3.1.0 (source of truth)
    garage-admin-v2.openapi30.json   Down-converted 3.0 spec (DO NOT EDIT, generated)
    downconvert/main.go              3.1 -> 3.0 converter (hand-written, EDIT THIS)
```

- **Use the client** via `garageadmin.NewAdminClient(baseURL, adminToken)`. It embeds the
  generated `ClientWithResponses` (so every `<Operation>WithResponse` method is available)
  and injects `Authorization: Bearer <token>` on every request. `client.go` is the stable
  surface — add convenience helpers there, not in the generated file.
- The generated package already owns `Client` / `NewClient` (its low-level untyped client),
  which is why the wrapper is named `AdminClient` / `NewAdminClient`.

### Why there is a down-convert step

oapi-codegen (via kin-openapi) only parses **OpenAPI 3.0**, but Garage publishes a **3.1.0**
spec. `openapi/downconvert` rewrites the handful of 3.1 constructs Garage actually uses into
their 3.0 equivalents before generation:

- nullable type arrays `"type": ["string", "null"]` -> `"type": "string", "nullable": true`
- nullable refs `"oneOf": [{"type": "null"}, {"$ref": ...}]` -> `"allOf": [{"$ref": ...}], "nullable": true`
- boolean `items` (JSON Schema 2020-12) -> empty schema

Genuine multi-member `oneOf` unions (no null member) are left untouched — oapi-codegen
handles those natively. `oapi-codegen.yaml` also sets `response-type-suffix: Resp` because
several component schemas are literally named `<Operation>Response` and would otherwise
collide with the generated per-operation response wrappers.

### Regenerating the client

```bash
make generate-client   # runs: downconvert 3.1 -> 3.0, then oapi-codegen
```

Both the converted spec and `zz_generated.client.go` are committed; CI
(`.github/workflows/generate.yml`) runs `make manifests generate generate-client` and fails
on any `git diff`, so always regenerate and commit after touching the spec or config.

### When Garage changes their Admin API

1. Update the vendored spec: copy the new `garage-admin-v2.json` into
   `internal/garageadmin/openapi/` (upstream lives at `doc/api/garage-admin-v2.json` in the
   Garage repo — note `references/` is **not** tracked, so the vendored copy is the source of
   truth used by builds and CI).
2. Run `make generate-client`. If it errors with something like
   `unhandled Schema type` or `cannot unmarshal ... into ... Schema`, Garage introduced a 3.1
   construct the converter doesn't handle yet (e.g. `const`, numeric `exclusiveMinimum`,
   `anyOf`, discriminators). Extend `openapi/downconvert/main.go` to rewrite it, then rerun.
3. Run `make build test`. Fix any controller call sites the regenerated types broke.
4. Pin the matching Garage image tag in the cluster defaults if the API change is
   version-gated, and commit the spec + regenerated client together.

## CLI Commands Cheat Sheet

### Create API (your own types)

```bash
kubebuilder create api --group <group> --version <version> --kind <Kind>
```

### Deploy Image Plugin (scaffold to deploy/manage ANY container image)

Generate a controller that deploys and manages a container image (nginx, redis, memcached, your app, etc.):

```bash
# Example: deploying memcached
kubebuilder create api --group example.com --version v1alpha1 --kind Memcached \
  --image=memcached:alpine \
  --plugins=deploy-image.go.kubebuilder.io/v1-alpha
```

Scaffolds good-practice code: reconciliation logic, status conditions, finalizers, RBAC. Use as a reference implementation.

### Create Webhooks

```bash
# Validation + defaulting
kubebuilder create webhook --group <group> --version <version> --kind <Kind> \
  --defaulting --programmatic-validation

# Conversion webhook (for multi-version APIs)
kubebuilder create webhook --group <group> --version v1 --kind <Kind> \
  --conversion --spoke v2
```

### Controller for Core Kubernetes Types

```bash
# Watch Pods
kubebuilder create api --group core --version v1 --kind Pod \
  --controller=true --resource=false

# Watch Deployments
kubebuilder create api --group apps --version v1 --kind Deployment \
  --controller=true --resource=false
```

### Controller for External Types (e.g., from other operators)

Watch resources from external APIs (cert-manager, Argo CD, Istio, etc.):

```bash
# Example: watching cert-manager Certificate resources
kubebuilder create api \
  --group cert-manager --version v1 --kind Certificate \
  --controller=true --resource=false \
  --external-api-path=github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1 \
  --external-api-domain=io \
  --external-api-module=github.com/cert-manager/cert-manager
```

**Note:** Use `--external-api-module=<module>@<version>` only if you need a specific version. Otherwise, omit `@<version>` to use what's in go.mod.

### Webhook for External Types

```bash
# Example: validating external resources
kubebuilder create webhook \
  --group cert-manager --version v1 --kind Issuer \
  --defaulting \
  --external-api-path=github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1 \
  --external-api-domain=io \
  --external-api-module=github.com/cert-manager/cert-manager
```

## Testing & Development

```bash
make test              # Run unit tests (uses envtest: real K8s API + etcd)
make run               # Run locally (uses current kubeconfig context)
```

Tests use **Ginkgo + Gomega** (BDD style). Check `suite_test.go` for setup.

## Deployment Workflow

```bash
# 1. Regenerate manifests
make manifests generate

# 2. Build & deploy
export IMG=<registry>/<project>:tag
make docker-build docker-push IMG=$IMG  # Or: kind load docker-image $IMG --name <cluster>
make deploy IMG=$IMG

# 3. Test
kubectl apply -k config/samples/

# 4. Debug
kubectl logs -n <project>-system deployment/<project>-controller-manager -c manager -f
```

### API Design

**Key markers for** `api/<version>/*_types.go`:

```go
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:printcolumn:name="Status",type=string,JSONPath=".status.conditions[?(@.type=='Ready')].status"

// On fields:
// +kubebuilder:validation:Required
// +kubebuilder:validation:Minimum=1
// +kubebuilder:validation:MaxLength=100
// +kubebuilder:validation:Pattern="^[a-z]+$"
// +kubebuilder:default="value"
```

- **Use** `metav1.Condition` for status (not custom string fields)
- **Use predefined types**: `metav1.Time` instead of `string` for dates
- **Follow K8s API conventions**: Standard field names (`spec`, `status`, `metadata`)

### Controller Design

**RBAC markers in** `internal/controller/*_controller.go`:

```go
// +kubebuilder:rbac:groups=mygroup.example.com,resources=mykinds,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=mygroup.example.com,resources=mykinds/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=mygroup.example.com,resources=mykinds/finalizers,verbs=update
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
```

**Implementation rules:**

- **Idempotent reconciliation**: Safe to run multiple times
- **Re-fetch before updates**: `r.Get(ctx, req.NamespacedName, obj)` before `r.Update` to avoid conflicts
- **Structured logging**: `log := log.FromContext(ctx); log.Info("msg", "key", val)`
- **Owner references**: Enable automatic garbage collection (`SetControllerReference`)
- **Watch secondary resources**: Use `.Owns()` or `.Watches()`, not just `RequeueAfter`
- **Finalizers**: Clean up external resources (buckets, VMs, DNS entries)

### Logging

**Follow Kubernetes logging message style guidelines:**

- Start from a capital letter
- Do not end the message with a period
- Active voice: subject present (`"Deployment could not create Pod"`) or omitted (`"Could not create Pod"`)
- Past tense: `"Could not delete Pod"` not `"Cannot delete Pod"`
- Specify object type: `"Deleted Pod"` not `"Deleted"`
- Balanced key-value pairs

```go
log.Info("Starting reconciliation")
log.Info("Created Deployment", "name", deploy.Name)
log.Error(err, "Failed to create Pod", "name", name)
```

**Reference:** https://github.com/kubernetes/community/blob/master/contributors/devel/sig-instrumentation/logging.md#message-style-guidelines

### Webhooks

- **Create all types together**: `--defaulting --programmatic-validation --conversion`
- **When`--force`is used**: Backup custom logic first, then restore after scaffolding
- **For multi-version APIs**: Use hub-and-spoke pattern (`--conversion --spoke v2`)
  - Hub version: Usually oldest stable version (v1)
  - Spoke versions: Newer versions that convert to/from hub (v2, v3)
  - Example: `--group crew --version v1 --kind Captain --conversion --spoke v2` (v1 is hub, v2 is spoke)

### Learning from Examples

The **deploy-image plugin** scaffolds a complete controller following good practices. Use it as a reference implementation:

```bash
kubebuilder create api --group example --version v1alpha1 --kind MyApp \
  --image=<your-image> --plugins=deploy-image.go.kubebuilder.io/v1-alpha
```

Generated code includes: status conditions (`metav1.Condition`), finalizers, owner references, events, idempotent reconciliation.

## Distribution Options

### Option 1: YAML Bundle (Kustomize)

```bash
# Generate dist/install.yaml from Kustomize manifests
make build-installer IMG=<registry>/<project>:tag
```

**Key points:**

- The `dist/install.yaml` is generated from Kustomize manifests (CRDs, RBAC, Deployment)
- Commit this file to your repository for easy distribution
- Users only need `kubectl` to install (no additional tools required)

**Example:** Users install with a single command:

```bash
kubectl apply -f https://raw.githubusercontent.com/<org>/<repo>/<tag>/dist/install.yaml
```

### Option 2: Helm Chart

```bash
kubebuilder edit --plugins=helm/v2-alpha                      # Generates dist/chart/ (default)
kubebuilder edit --plugins=helm/v2-alpha --output-dir=charts  # Generates charts/chart/
```

**For development:**

```bash
make helm-deploy IMG=<registry>/<project>:<tag>          # Deploy manager via Helm
make helm-deploy IMG=$IMG HELM_EXTRA_ARGS="--set ..."    # Deploy with custom values
make helm-status                                         # Show release status
make helm-uninstall                                      # Remove release
make helm-history                                        # View release history
make helm-rollback                                       # Rollback to previous version
```

**For end users/production:**

```bash
helm install my-release ./<output-dir>/chart/ --namespace <ns> --create-namespace
```

**Important:** If you add webhooks or modify manifests after initial chart generation:

1. Backup any customizations in `<output-dir>/chart/values.yaml` and `<output-dir>/chart/manager/manager.yaml`
2. Re-run: `kubebuilder edit --plugins=helm/v2-alpha --force` (use same `--output-dir` if customized)
3. Manually restore your custom values from the backup

### Publish Container Image

```bash
export IMG=<registry>/<project>:<version>
make docker-build docker-push IMG=$IMG
```

## References

### Essential Reading

- **Kubebuilder Book**: https://book.kubebuilder.io (comprehensive guide)
- **controller-runtime FAQ**: https://github.com/kubernetes-sigs/controller-runtime/blob/main/FAQ.md (common patterns and questions)
- **Good Practices**: https://book.kubebuilder.io/reference/good-practices.html (why reconciliation is idempotent, status conditions, etc.)
- **Logging Conventions**: https://github.com/kubernetes/community/blob/master/contributors/devel/sig-instrumentation/logging.md#message-style-guidelines (message style, verbosity levels)

### API Design & Implementation

- **API Conventions**: https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md
- **Operator Pattern**: https://kubernetes.io/docs/concepts/extend-kubernetes/operator/
- **Markers Reference**: https://book.kubebuilder.io/reference/markers.html

### Tools & Libraries

- **controller-runtime**: https://github.com/kubernetes-sigs/controller-runtime
- **controller-tools**: https://github.com/kubernetes-sigs/controller-tools
- **Kubebuilder Repo**: https://github.com/kubernetes-sigs/kubebuilder
