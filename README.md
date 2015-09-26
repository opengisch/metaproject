# metaproject

[![Build
Status](https://travis-ci.org/opengisch/metaproject.svg?branch=master)](https://travis-ci.org/opengisch/metaproject)

A suite of tools and scripts to maintain QGIS project information in json files.

From these json files, postgres data structures and QGIS projects can be generated.

To be modular, json files can build on top of each other (dependencies). It is 
therefore possible to use a generated json file as basis and add custom configuration
on top of this base file.

## Init scripts

It includes tools to generate json files from existing postgis databases as a starting
point.

## Generator scripts

It includes tools to generate postgres/postgis databases.

It includes tools to generate to genreate QGIS projects.
