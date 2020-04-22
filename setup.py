#!/usr/bin/env python

from setuptools import setup
from setuptools.command.install import install as _install


# parse 'requirements.txt' to install_requires format: ['rsa==3.4.2', '', ...]
def parse_requirements(requirements):
    with open(requirements) as f:
        return [l.strip('\n') for l in f if l.strip('\n') and not l.startswith('#')]


install_reqs = parse_requirements('requirements.txt')


class install(_install):
    def pre_install_script(self):
        pass

    def post_install_script(self):
        pass

    def run(self):
        self.pre_install_script()

        _install.run(self)

        self.post_install_script()


if __name__ == '__main__':
    setup(
        name = 'altmaker',
        version = '0.9.0',
        description = '',
        long_description = '',
        author = 'CK',
        author_email = '',
        license = '',
        url = '',
        scripts = [],    #
        packages = [
        ],
        namespace_packages = [],
        py_modules = [
            'alt_reranker',
        ],
        classifiers = [
            'Development Status :: 3 - Alpha',
            'Programming Language :: Python'
        ],
        entry_points = {},
        data_files = [],
        package_data = {},
        install_requires=install_reqs,
        dependency_links = [],
        zip_safe = True,
        cmdclass = {'install': install},
        keywords = '',
        python_requires = '',
        obsoletes = [],
    )
