import importlib
from dataclasses import dataclass
from typing import Any, Callable, Dict, Protocol, Sequence

from dbt_loom.manifests import ManifestNode


@dataclass(frozen=True)
class TransformerContext:
    """Contextual information passed to node transformers."""

    manifest_name: str
    manifest_reference_name: str
    raw_manifest: Dict[str, Any]


class NodeTransformer(Protocol):
    """Protocol for node transformers.

    A NodeTransformer is any callable that accepts a dict of selected nodes
    and a TransformerContext, and returns a (potentially modified) dict of nodes.
    """

    def __call__(
        self,
        nodes: Dict[str, ManifestNode],
        context: TransformerContext,
    ) -> Dict[str, ManifestNode]: ...


def resolve_transformer(dotted_path: str) -> NodeTransformer:
    """Import and return a callable from a dotted Python path.

    Example: "my_package.transforms.rewrite_schemas" resolves to the function object.
    """
    module_path, _, attr_name = dotted_path.rpartition(".")
    if not module_path:
        raise ImportError(
            f"Invalid transformer path '{dotted_path}': must be a fully qualified "
            "dotted path like 'my_module.my_function'"
        )
    module = importlib.import_module(module_path)
    transformer = getattr(module, attr_name)
    if not callable(transformer):
        raise TypeError(
            f"Transformer '{dotted_path}' resolved to {type(transformer)}, "
            "which is not callable."
        )
    return transformer


def chain_transformers(
    transformers: Sequence[NodeTransformer],
) -> NodeTransformer:
    """Compose multiple transformers into a single transformer.

    Transformers are applied in order (first to last). Each receives
    the output of the previous one.
    """

    def chained(
        nodes: Dict[str, ManifestNode],
        context: TransformerContext,
    ) -> Dict[str, ManifestNode]:
        for transformer in transformers:
            nodes = transformer(nodes, context)
        return nodes

    return chained
