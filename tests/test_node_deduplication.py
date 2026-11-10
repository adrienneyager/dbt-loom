"""Tests for node deduplication when the same node appears in multiple upstream manifests."""

from dbt_loom import (
    convert_model_nodes_to_model_node_args,
    identify_node_subgraph,
    merge_loom_nodes,
)


def _make_manifest(project_name, nodes):
    """Helper to create a minimal manifest dict."""
    return {
        "metadata": {"project_name": project_name},
        "nodes": nodes,
    }


def _make_node(
    unique_id,
    package_name,
    access="protected",
    *,
    name=None,
    relation_name=None,
    materialized=None,
):
    """Helper to create a minimal manifest node dict."""
    node = {
        "unique_id": unique_id,
        "name": name or unique_id.split(".")[-1],
        "package_name": package_name,
        "schema": "core",
        "resource_type": "model",
        "access": access,
    }
    if relation_name is not None:
        node["relation_name"] = relation_name
    if materialized is not None:
        node["config"] = {"materialized": materialized}
    return node


def _merge_manifests(manifest_pairs):
    """Simulate the initialize() merging logic from dbtLoom.

    Args:
        manifest_pairs: list of (manifest_dict, project_name) tuples in load order.
    """
    models = {}
    for manifest, manifest_name in manifest_pairs:
        selected_nodes = identify_node_subgraph(manifest)
        loom_nodes = convert_model_nodes_to_model_node_args(selected_nodes)
        merge_loom_nodes(models, loom_nodes, manifest_name)
    return models


def test_authoritative_node_wins_over_transitive_copy():
    """The owning manifest's public node should not be overwritten by a
    protected transitive copy from a later manifest."""

    uid = "model.project_a.shared_model"

    models = _merge_manifests(
        [
            (
                _make_manifest(
                    "project_a", {uid: _make_node(uid, "project_a", access="public")}
                ),
                "project_a",
            ),
            (
                _make_manifest(
                    "project_b", {uid: _make_node(uid, "project_a", access="protected")}
                ),
                "project_b",
            ),
        ]
    )

    assert models[uid].access == "public"


def test_authoritative_node_wins_regardless_of_load_order():
    """Even if the transitive copy is loaded first, the authoritative manifest
    should overwrite it."""

    uid = "model.project_a.shared_model"

    models = _merge_manifests(
        [
            (
                _make_manifest(
                    "project_b", {uid: _make_node(uid, "project_a", access="protected")}
                ),
                "project_b",
            ),
            (
                _make_manifest(
                    "project_a", {uid: _make_node(uid, "project_a", access="public")}
                ),
                "project_a",
            ),
        ]
    )

    assert models[uid].access == "public"


def test_transitive_node_added_when_no_authoritative_source():
    """Nodes from packages without their own manifest entry should still be
    injected via the first manifest that provides them."""

    uid = "model.project_c.some_model"

    models = _merge_manifests(
        [
            (
                _make_manifest(
                    "project_a", {uid: _make_node(uid, "project_c", access="protected")}
                ),
                "project_a",
            ),
        ]
    )

    assert uid in models


def test_first_transitive_copy_preserved_over_later_copies():
    """When a non-authoritative node appears in multiple manifests, the first
    loaded version should be kept."""

    uid = "model.project_c.some_model"

    models = _merge_manifests(
        [
            (
                _make_manifest(
                    "project_a", {uid: _make_node(uid, "project_c", access="public")}
                ),
                "project_a",
            ),
            (
                _make_manifest(
                    "project_b", {uid: _make_node(uid, "project_c", access="protected")}
                ),
                "project_b",
            ),
        ]
    )

    assert models[uid].access == "public"


def test_ephemeral_version_is_not_injected_alongside_materialized_version():
    """Ephemeral versions have no relation and can collide with a versioned model."""

    model_name = "STG_H_ICOACH_COACHING_SESSION"
    v1_uid = f"model.icoach_source.{model_name}.v1"
    v3_uid = f"model.icoach_source.{model_name}.v3"

    models = _merge_manifests(
        [
            (
                _make_manifest(
                    "icoach_source",
                    {
                        v1_uid: _make_node(
                            v1_uid,
                            "icoach_source",
                            access="public",
                            name=model_name,
                            relation_name=(
                                "DEV_DB.STAGE_SCHEMA.STG_H_ICOACH_COACHING_SESSION"
                            ),
                            materialized="table",
                        ),
                        v3_uid: _make_node(
                            v3_uid,
                            "icoach_source",
                            name=model_name,
                            materialized="ephemeral",
                        ),
                    },
                ),
                "icoach_source",
            ),
        ]
    )

    assert v1_uid in models
    assert v3_uid not in models
    assert models[v1_uid].identifier == model_name
