from setuptools import setup

setup(
   name='lib_ims',
   version='0.0.1',
   description='Dataset Helper library for the IMS Racing datasets',
   author='Richard Vogel',
   author_email='richard.vogel@gmx.net',
   packages=['lib_ims', 'lib_ims.db'],
   install_requires=['aiohttp', 'pandas'],
)
