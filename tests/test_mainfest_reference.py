from dbt_loom.config import ManifestReference, ManifestReferenceType, FileReferenceConfig


def test_manifest_reference_with_node_transformers():
    """Confirm ManifestReference accepts node_transformers."""
    ref = ManifestReference(
        name="test",
        type=ManifestReferenceType.file,
        config=FileReferenceConfig(path="./manifest.json"),
        node_transformers=["my_module.my_function"],
    )
    assert ref.node_transformers == ["my_module.my_function"]


def test_manifest_reference_default_node_transformers():
    """Confirm ManifestReference defaults node_transformers to empty list."""
    ref = ManifestReference(
        name="test",
        type=ManifestReferenceType.file,
        config=FileReferenceConfig(path="./manifest.json"),
    )
    assert ref.node_transformers == []
