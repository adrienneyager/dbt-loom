import pytest

from dbt_loom.manifests import ManifestNode
from dbt_loom.transformers import (
    TransformerContext,
    chain_transformers,
    resolve_transformer,
)


def _make_node(name, database=None, relation_name=None):
    return ManifestNode(
        unique_id=f"model.pkg.{name}",
        name=name,
        package_name="pkg",
        schema="main",
        resource_type="model",
        database=database,
        relation_name=relation_name,
    )


_DUMMY_CTX = TransformerContext(
    manifest_name="test",
    manifest_reference_name="test",
    raw_manifest={},
)


def test_resolve_transformer():
    """Resolve a known stdlib callable via dotted path."""
    transformer = resolve_transformer("json.loads")
    assert callable(transformer)


def test_resolve_transformer_invalid_path():
    """Raise ImportError for a non-existent module."""
    with pytest.raises(ImportError):
        resolve_transformer("nonexistent_module_xyz.func")


def test_resolve_transformer_bare_name():
    """Raise ImportError for a bare name without module path."""
    with pytest.raises(ImportError, match="must be a fully qualified"):
        resolve_transformer("bare_name")


def test_resolve_transformer_not_callable():
    """Raise TypeError when the resolved attribute is not callable."""
    with pytest.raises(TypeError, match="not callable"):
        resolve_transformer("os.sep")


def test_chain_transformers_empty():
    """An empty chain is a no-op."""
    nodes = {"model.pkg.a": _make_node("a", "db1")}
    chained = chain_transformers([])
    result = chained(nodes, _DUMMY_CTX)
    assert result == nodes


def test_chain_transformers_ordering():
    """Transformers execute in list order, each receiving the previous output."""
    call_order = []

    def first(nodes, context):
        call_order.append("first")
        for node in nodes.values():
            node.database = "after_first"
        return nodes

    def second(nodes, context):
        call_order.append("second")
        # Verify it sees the output of 'first'
        for node in nodes.values():
            assert node.database == "after_first"
            node.database = "after_second"
        return nodes

    nodes = {"model.pkg.a": _make_node("a", "original")}
    chained = chain_transformers([first, second])
    result = chained(nodes, _DUMMY_CTX)

    assert call_order == ["first", "second"]
    assert result["model.pkg.a"].database == "after_second"


def test_transformer_context_passed():
    """Verify the context object is forwarded to transformers."""
    received_contexts = []

    def capture_context(nodes, context):
        received_contexts.append(context)
        return nodes

    ctx = TransformerContext(
        manifest_name="revenue",
        manifest_reference_name="rev_ref",
        raw_manifest={"metadata": {"project_name": "revenue"}},
    )
    nodes = {"model.pkg.a": _make_node("a")}
    chain_transformers([capture_context])(nodes, ctx)

    assert len(received_contexts) == 1
    assert received_contexts[0].manifest_name == "revenue"
    assert received_contexts[0].manifest_reference_name == "rev_ref"
    assert received_contexts[0].raw_manifest == {"metadata": {"project_name": "revenue"}}


def test_database_alias_as_transformer():
    """Prove that a database-alias transformation can be achieved via the hook system.

    This replicates the behavior of the old apply_database_alias function
    using a custom transformer, validating that the hook system is sufficient.
    """
    alias_map = {"db1": "db1_alias"}

    def database_alias_transformer(nodes, context):
        for node in nodes.values():
            if node.database and node.database in alias_map:
                original_db = node.database
                alias_db = alias_map[original_db]
                node.database = alias_db
                if node.relation_name:
                    node.relation_name = node.relation_name.replace(original_db, alias_db, 1)
        return nodes

    node1 = _make_node("my_model1", "db1", '"db1"."schema"."table"')
    node2 = _make_node("my_model2", "db1", "`db1`.`schema`.`table`")
    node3 = _make_node("my_model3", "db2", '"db2"."schema"."table"')
    node4 = _make_node("my_model4", None, None)
    nodes = {
        "model.pkg.my_model1": node1,
        "model.pkg.my_model2": node2,
        "model.pkg.my_model3": node3,
        "model.pkg.my_model4": node4,
    }

    result = chain_transformers([database_alias_transformer])(nodes, _DUMMY_CTX)

    # Alias applied
    assert result["model.pkg.my_model1"].database == "db1_alias"
    assert result["model.pkg.my_model1"].relation_name == '"db1_alias"."schema"."table"'

    # Alias applied with backticks
    assert result["model.pkg.my_model2"].database == "db1_alias"
    assert result["model.pkg.my_model2"].relation_name == "`db1_alias`.`schema`.`table`"

    # No alias for db2
    assert result["model.pkg.my_model3"].database == "db2"
    assert result["model.pkg.my_model3"].relation_name == '"db2"."schema"."table"'

    # Null database/relation_name handled
    assert result["model.pkg.my_model4"].database is None
    assert result["model.pkg.my_model4"].relation_name is None


def test_transformer_can_add_nodes():
    """A transformer can add new nodes to the dict."""
    def add_node(nodes, context):
        nodes["model.pkg.new"] = _make_node("new", "db1")
        return nodes

    nodes = {"model.pkg.a": _make_node("a")}
    result = chain_transformers([add_node])(nodes, _DUMMY_CTX)
    assert "model.pkg.new" in result
    assert len(result) == 2


def test_transformer_can_remove_nodes():
    """A transformer can remove nodes from the dict."""
    def remove_node(nodes, context):
        return {k: v for k, v in nodes.items() if v.name != "to_remove"}

    nodes = {
        "model.pkg.keep": _make_node("keep"),
        "model.pkg.to_remove": _make_node("to_remove"),
    }
    result = chain_transformers([remove_node])(nodes, _DUMMY_CTX)
    assert "model.pkg.keep" in result
    assert "model.pkg.to_remove" not in result
