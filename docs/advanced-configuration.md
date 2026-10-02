# Advanced Configuration

`dbt-loom` also has a couple advanced configuration options for power users.

## Using environment variables in the `dbt-loom` config

You can easily incorporate your own environment variables into the config file. This allows for dynamic configuration values that can change based on the environment. To specify an environment variable in the `dbt-loom` config file, use one of the following formats:

`${ENV_VAR}` or `$ENV_VAR`

### Example:

```yaml
manifests:
  - name: revenue
    type: gcs
    config:
      project_id: ${GCP_PROJECT}
      bucket_name: ${GCP_BUCKET}
      object_name: ${MANIFEST_PATH}
```

## Exclude nested packages

In some circumstances, like running `dbt-project-evaluator`, you may not want a
given package in an upstream project to be imported into a downstream project.
You can manually exclude downstream projects from injecting assets from packages
by adding the package name to the downstream project's `excluded_packages` list.

```yaml
manifests:
  - name: revenue
    type: file
    config:
      path: ../revenue/target/manifest.json
    excluded_packages:
      # Provide the string name of the package to exclude during injection.
      - dbt_project_evaluator
```

## Include only specific packages

In a mesh with bi-directional references between many projects, maintaining
`excluded_packages` lists can become unwieldy. As an alternative, you can use
`included_packages` to explicitly allowlist which packages from a referenced
manifest should be injected:

```yaml
manifests:
  - name: project_b
    type: file
    config:
      path: ../project_b/target/manifest.json
    included_packages:
      - project_b # Only import nodes owned by this project
```

> **Note:** `included_packages` and `excluded_packages` are mutually exclusive.
> You may only use one or the other, not both.

## Gzipped files

`dbt-loom` natively supports decompressing gzipped manifest files. This is useful to reduce object storage size and to minimize loading times when reading manifests from object storage. Compressed file detection is triggered when the file path for the manifest is suffixed
with `.gz`.

```yaml
manifests:
  - name: revenue
    type: s3
    config:
      bucket_name: example_bucket_name
      object_name: manifest.json.gz
```

## Enabling Telemetry

By default, the `dbt-loom` plugin blocks outbound telemetry that reports on
the use of this plugin. This is a privacy-preserving measure for `dbt-loom`
users that does not impact the function of dbt-core and does not impede
dbt-core development in any way. If you _want_ this telemetry to be sent, you
can re-enable this behavior by setting the `enable_telemetry` property
in the `dbt_loom.config.yml` file.

```yaml
enable_telemetry: true
manifests: ...
```

## Custom Manifest Node Transformers

You can apply custom transformations to nodes after they are loaded from
upstream manifests. Each transformer is a Python callable that receives
the selected nodes and a context object, and returns the (potentially modified)
nodes dictionary.

### Writing a transformer

Create a Python module in your project or an installable package:

```python
# my_company/dbt_transforms.py
from dbt_loom.transformers import TransformerContext
from dbt_loom.manifests import ManifestNode


def rewrite_databases(
    nodes: dict[str, ManifestNode],
    context: TransformerContext,
) -> dict[str, ManifestNode]:
    """Rewrite database names using an alias mapping."""
    alias_map = {"upstream_db": "downstream_db"}

    for node in nodes.values():
        if node.database and node.database in alias_map:
            original_db = node.database
            alias_db = alias_map[original_db]
            node.database = alias_db
            if node.relation_name:
                node.relation_name = node.relation_name.replace(
                    original_db, alias_db, 1
                )
    return nodes
```

The `TransformerContext` provides:

- `manifest_name` — the resolved project name from manifest metadata
- `manifest_reference_name` — the `name` field from your config
- `raw_manifest` — the full manifest dictionary, for advanced use cases

### Configuring transformers

Add dotted Python import paths to the `node_transformers` list for a manifest
reference. Transformers are applied in the order listed.

```yaml
manifests:
  - name: revenue
    type: file
    config:
      path: ../revenue/target/manifest.json
    node_transformers:
      - "my_company.dbt_transforms.rewrite_databases"
```

### Execution order

For each manifest reference, the node processing pipeline is:

1. Parse raw manifest into `ManifestNode` objects
2. Apply `node_transformers` in list order
3. Filter out `excluded_packages`
4. Convert to dbt-injectable node args
