from pv_site_production.utils.config import load_config, load_config_from_string


def test_load_config_context(tmp_path):
    context = {"x": "y", "xx": "yy"}
    config_str = """
    # Work with ${var} or $var
    a: ${x}
    b:
        c: 123
        d: $xx
        e: patate $x poil
    """
    config = load_config_from_string(config_str, context)
    expected = {"a": "y", "b": {"c": 123, "d": "yy", "e": "patate y poil"}}

    assert config == expected

    # Do the same from a file.
    config_path = tmp_path / "test_load_config_context.yaml"
    with open(config_path, "w") as f:
        f.write(config_str)

    config2 = load_config(config_path, context)
    assert config2 == expected
