import os
from setuptools import setup, find_packages
from shutil import copytree


def read_requirements():
    with open("requirements.txt") as fp:
        content = fp.readlines()
    return [line.strip() for line in content if not line.startswith("#")]


def find_scripts():
    root = "scripts"
    return [os.path.join(root, f) for f in os.listdir(root)]


def copy_config_files():
    config_dir = os.path.join(
        os.environ.get("APPDATA")
        or os.environ.get("XDG_CONFIG_HOME")
        or os.path.join(os.environ["HOME"], ".config"),
        "cryptoex",
    )
    copytree("config", config_dir, dirs_exist_ok=True)


setup(
    name="lobsim",
    version="1.0.0",
    author="Ayyoub BMS",
    author_email="",
    description="Matching engine simulator",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    install_requires=read_requirements(),
    url="",
    classifiers=[],
    python_requires=">=3.10",
    packages=find_packages(),
    scripts=find_scripts(),
)


copy_config_files()
