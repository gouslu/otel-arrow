# azure-identity-auth

Active + Shared extension that implements the `BearerTokenProvider` capability
using `azure_identity`. Tokens are refreshed in the background by the
extension's `start()` task and published to consumers via a `watch` channel.

## Configuration

```yaml
extensions:
  azure_identity:
    urn: "urn:microsoft:extension:azure_identity_auth"
    config:
      method: managed_identity        # or "development"
      client_id: "<optional UA-MSI client id>"
      scope: "https://monitor.azure.com/.default"
```

The refresh cadence (skew before expiry, retry delay on failure) is fixed by
constants in `extension.rs` and is not user-configurable.

## Auth methods

| Method            | Notes                                                                  |
|-------------------|------------------------------------------------------------------------|
| `managed_identity`| System-assigned by default; supply `client_id` for user-assigned MSI.  |
| `development`     | Uses local developer tooling (Azure CLI / `azd`) — for local dev only. |

## Capabilities

| Capability             | Variants            |
|------------------------|---------------------|
| `BearerTokenProvider`  | Shared (Send)       |
