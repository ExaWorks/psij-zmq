from setuptools import setup, find_packages

with open("src/psij-zmq/VERSION") as f:
    VERSION = f.read().strip()

if __name__ == '__main__':
    with open('requirements.txt') as f:
        install_requires = f.readlines()

    setup(
        name='psij-zmq',
        version=VERSION,

        description='''This is an implementation of the PSI/J (Portable Submission Interface for Jobs)
        specification.''',

        author='The ExaWorks Team',
        author_email='andre@merzky.net',

        url='https://github.com/exaworks/psij-zmq',

        classifiers=[
            'Programming Language :: Python :: 3',
            'License :: OSI Approved :: MIT License',
        ],


        packages=find_packages(where='src') + ['psij-descriptors'],
        package_dir={'': 'src'},

        package_data={
            '': ['README.md', 'LICENSE']
        },

        scripts=['bin/psij_zmq_service.py'],

        entry_points={
        },

        install_requires=install_requires,
        python_requires='>=3.7'
    )
