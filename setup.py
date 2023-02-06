from setuptools import setup, find_packages

setup(
    name="pascrd",
    version="0.1.0",
    url='https://github.com/matt-sd-watson/pascrd/',
    project_urls={
        "Issues": "https://github.com/matt-sd-watson/pascrd/issues",
        "Source": "https://github.com/matt-sd-watson/pascrd",
    },
    author="Matthew Watson",
    author_email="mwatson@lunenfeld.ca",
    packages=find_packages(),
    package_dir={"pascrd": "pascrd"},
    package_data={'': ['*.json']},
    include_package_data=True,
    description="",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    keywords=["single-cell download repository"],
    classifiers=[
        "Intended Audience :: Science/Research",
        "Programming Language :: Python :: 3.9"
    ],
    license="Unlicensed",
    install_requires=["selenium", "requests", "asyncio", "aiohttp"],
    python_requires=">=3.9.0",
)
