import os
import shutil
from setuptools import setup, find_packages
from pkutils import parse_requirements
from pathlib import Path
from dotenv import load_dotenv
from dqlabs.file_crypto import encrypt_env_file


try:
    base_dir = str(
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
    )
    # requirements_file_path = os.path.join(base_dir, "infra/airflow/dags", "requirements.txt")
    # requirements = []
    # if os.path.exists(requirements_file_path):
    #     requirements = list(parse_requirements(requirements_file_path))

    env_file_path = os.path.join(base_dir, "infra/airflow/dags", ".env")
    env_dst_file_path = os.path.join(base_dir, "infra/airflow/dags/dqlabs", ".env.enc")
    if os.path.exists(env_file_path):
        load_dotenv(dotenv_path=env_file_path)

    client_name = os.environ.get("CLIENT_NAME")
    client_name = client_name if client_name else "dqlabs"
    client_name = client_name.replace(" ", "-").lower()
    readme_file_path = os.path.join(base_dir, "infra/airflow/dags", "README.md")
    long_description = ""
    if os.path.exists(readme_file_path):
        with open(readme_file_path, "r") as description:
            long_description = description.read()

    if os.path.exists(env_file_path):
        source_env_file = env_file_path
        encrypted_env_file = encrypt_env_file(env_file_path)
        shutil.copy(encrypted_env_file, env_dst_file_path)
        dest_env_file = env_dst_file_path.replace(".enc", "")
        if os.path.exists(dest_env_file):
            os.remove(dest_env_file)

    setup(
        name=client_name,
        version="3.0",
        license="MIT",
        author=client_name,
        description="This is a python module used by airflow dags.",
        packages=find_packages(exclude=["tests", "dist", ".env"]),
        package_data={"dqlabs": ["*.md", "*.txt", ".env.enc", "LICENSE"]},
        # install_requires=requirements,
        classifiers=[
            "Programming Language :: Python :: 3.8",
            "Programming Language :: Python :: 3.8",
            "License :: OSI Approved :: MIT License",
            "Operating System :: OS Independent",
        ],
        python_requires=">=3.7",
        long_description=long_description,
        zip_safe=False,
    )
except Exception as e:
    raise e
