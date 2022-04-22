import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()


setuptools.setup(
    name="household_pulse",
    install_requires=[
        'numpy>=1.21.2',
        'pandas>=1.3.3',
        'requests>=2.26.0',
        'beautifulsoup4>=4.10.0',
        'mysql-connector-python>=8.0.26',
        'boto3>=1.20.24',
        'pyarrow>=4.0.1',
        'tqdm>=4.63.0'
    ],
    version='1.2.10',
    author="Manuel Martinez",
    author_email="manmart@uchicago.edu",
    description=(
        'Python package containing functionality to run the ETL pipeline that '
        'serves the Household Pulse project.'),
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/mansueto-institute/household-pulse",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.9')
