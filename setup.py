from setuptools import setup


setup(
    name='espdf',
    version='0.1.0-dev',
    url='https://github.com/flother/pdf-search',
    py_modules=(
        'espdf',
    ),
    install_requires=(
        'certifi',
        'elasticsearch-dsl',
    ),
    entry_points={
        'console_scripts': (
            'espdf=espdf:cli',
        ),
    },
)
