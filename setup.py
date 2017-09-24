from distutils.core import setup

classifiers = [
    'Development Status :: 4 - Beta',
    'License :: OSI Approved :: MIT License',
    'Operating System :: OS Independent',
    'Programming Language :: Python',
    'Programming Language :: Python :: 3',
    'Topic :: Database',
]

setup(
    name="minicql",
    version=__import__('minicql').__version__,
    url='https://github.com/nakagami/minicql/',
    classifiers=classifiers,
    keywords=['Cassandra'],
    author='Hajime Nakagami',
    author_email='nakagami@gmail.com',
    description='Yet another Cassandra database driver',
    long_description=open('README.rst').read(),
    license="MIT",
    py_modules=['minicql'],
)
