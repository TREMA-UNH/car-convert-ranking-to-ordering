try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(
    name='trec-car-y3-conversion',
    version='1.0',
    packages=['trec_car_y3_conversion'],
    url='https://github.com/TREMA-UNH/car-convert-ranking-to-ordering',
    keywords=['wikipedia','complex answer retrieval','trec car'],
    license='BSD 3-Clause',
    author='laura-dietz, jramsdell',
    author_email='Laura.Dietz@unh.edu',
    description='support tools for TREC CAR Y3 participants: converting passage rankings into passage orderings; validation; evaluation',
    install_requires=['trec-car-tools>=2.3', 'typing'],
    python_requires='>=3',
    classifiers=[
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ]

)


