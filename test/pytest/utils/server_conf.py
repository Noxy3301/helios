"""The configuration file a test hands its own storage server."""
import os
import tempfile


def write_conf(directory=None, **keys):
    """Writes `key = value` lines into <directory>/helios.cnf and
    returns the path. Keys left out keep the server's built-in defaults."""
    path = os.path.join(directory or tempfile.mkdtemp(prefix="helios_conf_"),
                        "helios.cnf")
    with open(path, "w") as out:
        out.writelines(f"{key} = {value}\n" for key, value in keys.items())
    return path
