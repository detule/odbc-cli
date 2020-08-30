#!/bin/bash -ue

if ! [[ "$TRAVIS_TAG" == "" ]]
then
  echo "Deploying wheel to test pypi"
  twine upload --repository-url https://test.pypi.org/legacy/ -u $TEST_PYPI_USERNAME -p $TEST_PYPI_PASSWORD  $TRAVIS_BUILD_DIR/dist/odbcli*.tar.gz
  echo "Deploying wheel to pypi"
  twine upload -u $PYPI_USERNAME -p $PYPI_PASSWORD  $TRAVIS_BUILD_DIR/dist/odbcli*.tar.gz
else
  echo "Deploying wheel to test pypi"
  twine upload --repository-url https://test.pypi.org/legacy/ -u $TEST_PYPI_USERNAME -p $TEST_PYPI_PASSWORD  $TRAVIS_BUILD_DIR/dist/odbcli*.tar.gz
fi
