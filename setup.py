from distutils.core import setup

setup(name='krakenous',
      version='0.3',
      description='A backend for machine learning-related feature extraction and storing',
      author='George Oblapenko',
      author_email='kunstmord@kunstmord.com',
      url='https://github.com/Kunstmord/krakenous',
      license="GPL",
      packages=['krakenous'],
      package_dir={'krakenous': 'krakenous'},
      requires=['numpy'],
      )