import setuptools



setuptools.setup(
        name='utils',
        version='1.0.1',
        description='Haystack based utils for NLP',
        author='Data Service Center GIZ',
        author_email='prashant.singh@giz.de',
        package_dir={"": "nlputils"},
        packages=setuptools.find_packages(where='nlputils'),  
         #external packages as dependencies
)