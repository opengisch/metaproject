
Concept
=======

The basic idea is to be able to integrate information from different sources
into a QGIS project file. Sources can be manually written or generated and be
spread over different files.

In general the file syntax is yaml. Other files may be referenced from a yaml
file.


Generators
----------

Generators extract information which can be used in a .qgs project file from
data sources like postgres databases. This will be brought into an intermediate
yaml metafile which can then be referenced by other files, which most likely
are manually written control files.

An example for a yaml file created from a postgres database can look like this:

```
schema: qgep
tables:
  od_wastewater_structure:
    fields:
      obj_id:
        default: [function to generate serial]
        primary: true
      level:
        type: int
        min: 0
        max: 4000
  od_manhole:
    fields:
      obj_id:
        default: [function to generate serial]
```



Project Description
-------------------

A project description file contains metainformation for the project to generate
as well as a legend.

```
include:
  qgep_db.yaml

project:
  name: qgep
  author: OPENGIS.ch
  crs: 2056
  extent: [2607360, 1182010, 2625720, 1174450]
  legend:
    - od_manhole: # Dicionaries are layers
      type: layer
      source: @od_manhole
      style: manhole.qml
      name: Normschacht # This can be skipped and left to the translation unit
    - Base layer: # Lists are groups
      - cadastral_data:
        source: [some wms]
```


View generator
--------------

For object inheritance, views will be generated, that generate one view for the
parent object.

 * with a special attribute `type` which specifies on which subtable
an object is found
 * all attributes of the subtables are grouped in the view
 * prefixes based on the subtable name will be added on collision

Object inheritance tables are always abstract.
