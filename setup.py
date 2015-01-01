from distutils.core import setup

setup(name='krakenous',
      version='0.1',
      description='A backend for machine learning-related feature extraction and storing',
      author='Viktor Evstratov, George Oblapenko',
      author_email='kunstmord@kunstmord.com',
      url='https://github.com/Kunstmord/krakenlib',
      license="GPL",
      packages=['krakenous'],
      package_dir={'krakenous': 'krakenlib'},
      requires=['numpy'],
      )