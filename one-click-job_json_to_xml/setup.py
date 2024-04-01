import setuptools
 
REQUIRED_PACKAGES = [
    'google-cloud-storage==1.35.0',
    'pandas==1.2.2',
    'gcsfs==0.7.1',
    'google-cloud-documentai'
]
 
PACKAGE_NAME = 'TheCodebuzzDataflow'
PACKAGE_VERSION = '0.0.1'
 
setuptools.setup(
    name=PACKAGE_NAME,
    version=PACKAGE_VERSION,
    description='Set up file for the required Dataflow packages ',
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages(),
)