import setuptools

setuptools.setup(
    name="beam-flex-template",
    version="0.0.1",
    author="IDRIS ADEBISI",
    description="A Beam Flex Template for Python",
    packages=setuptools.find_packages(),
    install_requires=[
        "Faker==19.6.2",
        "apache-beam[gcp]==2.48.0"
    ],
    )