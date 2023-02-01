import setuptools


with open("README.md") as fp:
    long_description = fp.read()


setuptools.setup(
    name="tahoe_infrastructure",
    version="0.0.1",

    description="Tahoe infrastructure library",
    long_description=long_description,
    long_description_content_type="text/markdown",

    author="author",

    package_dir={"": "tahoe_infrastructure"},
    packages=setuptools.find_packages(where="tahoe_infrastructure"),

    install_requires=[
        "aws-cdk-lib>=2.8.0",
        "aws-cdk.aws-redshift-alpha",
        "aws-cdk.aws-glue-alpha",
        "constructs>=10.0.0",
        "boto3>=1.15.0,<=1.15.3",
        "wheel",
        "python-utils>=3.3.3",
        "pkginfo>=1.8.3",
        "sphinx",
        "sphinx_rtd_theme",
        "pycodestyle",
        "pydocstyle"
    ],

    python_requires=">=3.6",

    classifiers=[
        "Development Status :: 4 - Beta",

        "Intended Audience :: Developers",

        "Programming Language :: JavaScript",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",

        "Topic :: Software Development :: Code Generators",
        "Topic :: Utilities",

        "Typing :: Typed",
    ],
)
