import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()


setuptools.setup(
    name="household_pulse",
    install_requires=[],
    version='0.0.1',
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
