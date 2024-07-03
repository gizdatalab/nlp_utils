import setuptools

install_requires = [
        "pymupdf == 1.24.7",

]


setuptools.setup(
        name='nlputils',
        version='1.0.1',
        description='utils for NLP',
        author='Data Service Center GIZ',
        author_email='ppsingh.iitk@gmail.com',
        package_dir={"": "src"},
        packages=setuptools.find_packages(where='src'),  
        install_requires = install_requires,
)