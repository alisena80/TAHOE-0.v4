from setuptools import setup, find_packages
setup(
    name="tahoe_cli",
    version='0.2',
    install_requires=[
        'Click',
        'jinja2'
    ],
    packages=find_packages(),
    entry_points='''
        [console_scripts]
        tahoe=tahoe_cli.cli:cli
    ''',
    include_package_data=True,
    package_data={'tahoe_cli': ['templates/*']}
)
