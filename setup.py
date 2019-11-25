from setuptools import setup

setup(name='keeval',
      version='0.6',
      description='Read and write values to s3 keys.',
      url='http://github.com/Telmediq/keeval',
      author='Telmediq',
      author_email='jeff@telmediq.com',
      license='MIT',
      packages=['keeval'],
      zip_safe=False,
      install_requires=[
        'boto3>=1.7.0',
      ],
      entry_points={
                  'console_scripts': [
                      'keeval = keeval:run',
                  ],
              },
)
