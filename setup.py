from setuptools import setup, find_packages

setup(
    name="optimized-memory-mcp-server",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "aiofiles>=23.2.1",
        "mcp>=1.1.2",
    ],
    python_requires=">=3.12",
)
