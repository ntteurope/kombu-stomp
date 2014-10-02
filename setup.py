import os

from pip import req
import setuptools

HERE = os.path.abspath(os.path.dirname(__file__))


def get_requirements(reqfile):
    path = os.path.join(HERE, reqfile)
    return set([dep.name
                for dep in req.parse_requirements(path)])


setuptools.setup(
    zip_safe=True,
    install_requires=get_requirements('requirements/base.txt'),
    tests_require=get_requirements('requirements/tests.txt'),
    setup_requires='d2to1',
    d2to1=True,
)
