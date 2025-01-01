from setuptools import setup, find_packages

setup(
    name="optimized-memory-mcp-server",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "aiofiles>=23.2.1",
        "mcp>=1.1.2",
        "aiosqlite>=0.20.0",
        "pydantic>=2.0.0",  # For better data validation
        "orjson>=3.9.0",    # For faster JSON handling
        "asyncpg>=0.28.0",  # For better async DB performance
        "aiohttp>=3.9.0",   # For async HTTP operations
        "cachetools>=5.3.0", # For in-memory caching
    ],
    python_requires=">=3.12",
)
