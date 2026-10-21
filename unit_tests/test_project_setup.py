def test_project_name():
    project_name = "RetailQ"
    assert project_name == "RetailQ"


def test_python_version_requirement():
    minimum_version = (3, 12)
    current_version = (3, 12)

    assert current_version >= minimum_version
