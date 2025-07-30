#!/bin/bash

PACKAGE_ZIP="packages_layer.zip"

# if you installed third party packages they would be installed with the following line
# pip3 install <package_name> <package_name> <package_name> -t .

zip -r  .

echo """Upload ${PACKAGE_ZIP} as a Lambda Layer"""