from setuptools import setup

setup(
    name='rsyba',
    version='0.1.0',
    description='Rsync backup system for multiple hosts with file synchronization',
    url='https://github.com/tommie/rsyba',
    author='Tommie Gannert',
    author_email='tommie+py@gannert.se',
    license='MIT',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Topic :: System :: Archiving :: Backup',
    ],
    packages=['rsyba'],
    entry_points={
        'console_scripts': [
            'rsyba-client = rsyba.client:main',
            'rsyba-server = rsyba.server:main',
        ],
    },
)
