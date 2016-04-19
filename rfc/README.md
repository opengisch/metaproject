
Concept
=======

The basic idea is to be able to integrate information from different sources
into a QGIS project file. Sources can *generated* from resources like a database
or *manually written* and they may be spread over different files.

In general the file syntax is yaml. Other files may be referenced from a yaml
file if suitable.

Requirements
------------

  * understands table inheritance concept
  * styles are loaded from a qml file
  * understands relations
  * extracts translatable strings into translation files (e.g. .ts)
  * labelling? qml file? translation?
  * creates a legend
  * can be split into multiple files from generic → specific
  * joins

Configurable aspects
--------------------

### Project

The project node contains base information about a project which is not specific to a particular layer.

The project will be read from the key project

```
project:
  crs: 21781
  name: My Project
  legend:
    - covers:
      <<: *lyr_qgep_vw_qgep_cover
    - a layer group:
      - baselayer:
        source: baselayer.shp
        style: layer2.qml
```

#### Legend

The legend consists of a list where each element is either a layer or a layer group.
If an element is a key/value pair it's interpreted as a layer, if it's a list, it's treated as a layer group.

```
project:
  legend:
  - layer:
    source: layer.shp
    style: layer.qml
  - group:
    - layer2:
      provider: postgres
      source: service='pg_qgep' key='"obj_id"' estimatedmetadata=true table="qgep"."od_maintenance_event" sql=
    - layer3:
      ...
```

This is the "main entry point" for loading layers. While it's possible to specify layers directly in here, it's often more advisable to load them from somewhere else via reference.

#### CRS

The CRS is specified as a simple EPSG number

```
project:
  crs: 21781
```
  
If the CRS is not specified, the crs of the first layer which is found will be used.
    
Further requirements like custom CRSes are not planned at the moment but may be added on request.

#### Initial extent

The initial extent can be specified as an array of xmin, xmax, ymin, ymax

```
project:
  extent: [643700, 649410, 18300, 180200]
```
    
or a layer can be specified which will be queried for the extent. Just specify the layer id as string.

```
project:
  extent: layer_id
```

#### Relations

relations:

#### Style presets

To be done

#### Custom properties



#### Transaction groups

Is a simple boolean flag



### Layers

#### Layer ID

#### Data source

#### Style (symbology)

#### Labelling

#### Actions

#### Forms

#### Widgets/Fields

#### Joins

#### Virtual fields

#### Custom layer properties

```
layer:
  properties:
    pluginX/configOption: 1
    pluginY/cfg: foo
```

#### Feature title

```
layer:
  feature_title: COALESCE("identifier", 'N/A')
```

## Generators

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

## Includes

Source files can be included with an `include` directive. An `ìnclude` directive
loads a yaml file. The structure defined by the included file will be loaded
before the current file is loaded. The current file has the possibility to overwrite
any of the specifications in the source file.

```
layers:
  fields:
    - id:
      alias: ID
      
  
```

## Data sources

Describe a datasource and how to connect to it. Which fields are available and which constraints apply to the fields.

Layers
------

Describe how a datasource should be visualized and may override properties of a data source. Multiple layers may reference the same data source.

* Layers can reference a QML style
* Layers can define actions
  * python (required)
* Fields will be loaded from the dataprovider and appropriate widgets will be created - based on:
  * datatype
  * constraints
  * relations
* Field definitions can be overwritten with more appropriate user values

Project Description
-------------------

A project description file contains metainformation for the project to generate
as well as a legend.

```
include: qgep_db.yaml # Load the qgep db 

project:
  name: qgep
  author: OPENGIS.ch
  crs: 2056
  extent: [2607360, 1182010, 2625720, 1174450]
  legend:
    - od_manhole: # Dictionaries are layers
      type: layer
      source: @od_manhole
      style: manhole.qml
      name: Manhole # This can be skipped and left to the translation unit
    - Base layers: # Lists are groups
      - cadastral_data:
        source: [some wms]
      - some_features:
        source [some wfs]
        style: some_features.qml
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

Translation
-----------

When loading a metaproject file, a .ts file gets produced as a by-product. This file includes the following strings as translation sources:

  * field aliases
  * layer legend name
  * value relation value column
  * value map column
  * labelling - **how?**

The .ts file can be translated via transifex or qt linguist or other means to any language.

The load metaproject function takes an optional parameter for a translated translation file. In this case, the generated project will be translated with the resources found in the .ts file.
