from setuptools import setup

setup(name='keeval',
      version='0.2',
      description='Read and write values to s3 keys.',
      url='http://github.com/Telmediq/keeval',
      author='Telmediq',
      author_email='jeff@telmediq.com',
      license='MIT',
      packages=['keeval'],
      zip_safe=False,
      entry_points={
                  'console_scripts': [
                      'keeval = keeval:main',
                  ],
              },
)
