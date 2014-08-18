
from distutils.core import setup

setup(name="syncfs",
      version="0.1",
      packages = ["syncfs"],
      package_dir = {"": "lib"},
      author="Zsolt Cserna",
      author_email="cserna.zsolt@gmail.com",
      description="Cache-invalidation filesystem",
      long_description="A PoC code for a cache-invalidation filesystem.",
      license="Apache 2.0",
      classifiers=[
                "Development Status :: 2 - Pre-Alpha",
      ]

)
