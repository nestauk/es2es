from setuptools import setup

common_kwargs = dict(
    version='0.1.0',
    license='MIT',
    install_requires=["requests==2.22.0",
                      "mock==2.0.0",
                      "pytest==3.9.2"],
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown",
    url='https://github.com/nesta/es2es',
    author='Joel Klinger',
    author_email='joel.klinger@nesta.org.uk',
    maintainer='Joel Klinger',
    maintainer_email='joel.klinger@nesta.org.uk',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.6'
    ],
    python_requires='>3.6',
    include_package_data=True,
)

setup(name='es2es',
      packages=['es2es'],
      **common_kwargs)
