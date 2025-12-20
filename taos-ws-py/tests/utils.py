import os
import subprocess
import toml


def get_connector_info():
    cargo_toml = toml.load(os.path.join(os.path.dirname(__file__), "../Cargo.toml"))
    version = cargo_toml["package"]["version"]

    git_commit_id = (
        subprocess.check_output(
            ["git", "rev-parse", "--short=7", "HEAD"],
            cwd=os.path.dirname(os.path.abspath(__file__)),
        )
        .decode()
        .strip()
    )

    return f"python-ws-v{version}-{git_commit_id}"
