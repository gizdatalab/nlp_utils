import setuptools



setuptools.setup(
        name='utils',
        version='1.0.1',
        description='Haystack based utils for NLP',
        author='Data Service Center GIZ',
        author_email='prashant.singh@giz.de',
        package_dir={"": "nlp_utils"},
        packages=setuptools.find_packages(where='nlp_utils'),  
         #external packages as dependencies
)