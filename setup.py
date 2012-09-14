from setuptools import setup, find_packages


setup(name='django-mysql-pool',
      version='0.2',
      description='Django application.',
      long_description='',
      author='Andy McKay',
      author_email='andym@mozilla.com',
      license='BSD',
      url='https://github.com/andymckay/django-mysql-pool',
      include_package_data=True,
      classifiers = [],
      packages=find_packages(exclude=['tests']),
      install_requires=[])
