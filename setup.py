import os
from setuptools import setup, find_packages

setup(
    name="task-framework",
    version="1.0.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.10",
    install_requires=[
        "redis>=4.0.0",
        "pydantic>=2.0.0",
        "structlog",  # optionnel
        "pyyaml",    # pour config files
    ],
    extras_require={
        "dev": [
            "pytest",
            "pytest-asyncio",
            "black",
            "mypy",
        ]
    },
    author="Baptiste Mori",
    description="Simple task worker framework",
    long_description=open("README.md").read() if os.path.exists("README.md") else "",
    long_description_content_type="text/markdown",
)