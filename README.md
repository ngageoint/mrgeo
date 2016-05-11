##Origin
[![Build Status](http://jenkins.dgis-dev.com:8080/job/mrgeo-opensource-aws-rpm/badge/icon)](http://jenkins.dgis-dev.com:8080/job/mrgeo-opensource-aws-rpm)

[![Join the chat at https://gitter.im/ngageoint/mrgeo](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/ngageoint/mrgeo?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)


MrGeo was developed at the National Geospatial-Intelligence Agency (NGA) in collaboration with [DigitalGlobe](https://www.digitalglobe.com/). The government has ["unlimited rights"](https://github.com/ngageoint/mrgeo/blob/master/NOTICE) and is releasing this software to increase the impact of government investments by providing developers with the opportunity to take things in new directions. The software use, modification, and distribution rights are stipulated within the [Apache 2.0 license](http://www.apache.org/licenses/LICENSE-2.0.html).

###Pull Requests

All pull request contributions to this project will be released under the Apache 2.0 license. 

Software source code previously released under an open source license and then modified by NGA staff is considered a "joint work" (see 17 USC 101); it is partially copyrighted, partially public domain, and as a whole is protected by the copyrights of the non-government authors and must be released according to the terms of the original open source license.

###MrGeo in Action
See [YouTube explainer](http://youtu.be/Z3fPTTtZ60I?list=FLBRaZ-IsIB44ikg-9n1RKtw)

###MrGeo in the News
[NGA Press Release](https://www1.nga.mil/MediaRoom/PressReleases/Pages/2015-02.aspx)

[DigitalGlobe Press Release](http://investor.digitalglobe.com/phoenix.zhtml?c=70788&p=RssLanding&cat=news&id=2007262)

MrGeo got a mention in an [article on NGA Opensourcing from Reuters](http://www.reuters.com/article/2015/05/23/us-usa-military-nga-idUSKBN0O72JE20150523)

###MrGeo Overview

MrGeo (pronounced "Mister Geo") is an open source geospatial toolkit designed to provide raster-based geospatial processing capabilities performed at scale. MrGeo enables global geospatial big data image processing and analytics.

MrGeo is built upon the Apache Spark distributed processing frarmework to leverage the storage and processing of 100’s of commodity computers.  Functionally,  MrGeo stores large raster datasets as a collection of individual tiles stored in Hadoop to enable large-scale data and analytic services.  The co-location of data and analytics offers the advantage of minimizing the movement of data in favor of bringing the computation to the data; a more favorable compute method for Geospatial Big Data. This framework has enabled the servicing of terabyte scale raster databases and  performed terrain analytics on databases exceeding 100’s of gigabytes in size.

MrGeo has been fully deployed and tested in Amazon EMR.

See [Wiki](https://github.com/ngageoint/mrgeo/wiki) for detailed documentation

Unique features/solutions of MrGeo:

* Scalable storage and processing of raster data
* Application ready data: data is stored in MrGeo in a format that is ready for computation, eliminating several data pre-processing steps from production workflows.
* A suite of robust Spark analytics that that include algebraic math operations, focal operations (e.g. slope and gaussian)
* A third generation data storage model that 
  * Maintains data locality via  spatial indexing. 
  * An abstraction layer between the analytics and storage methods to enables a diverse set of cloud storage options such as HDFS, Accumulo, HBASE etc.
* A Map algebra interface that enables the development of custom algorithms in a simple scripting API
*	A plugin architecture that facilitates a modular software development and deployment strategies
*	Data and Analytic capabilities provisioned by OGC and REST service end points

Exemplar MrGeo Use Cases:

*	Raster Storage and Provisioning:  MrGeo has been used to store, index, tile, and pyramid multi-terabyte scale image databases.  Once stored, this data is made available through a simple Tiled Map Services (TMS) and Web Mapping Services (WMS) and can be made available through GeoServer via a [MrGeo plugin](https://github.com/ngageoint/mrgeo-geoserver-plugin).
*	Large Scale Batch Processing and Serving:  MrGeo has been used to pre-compute global 1 ArcSecond (nominally 30 meters) elevation data (300+ GB) into derivative raster products : slope, aspect, relative elevation, terrain shaded relief (collectively terabytes in size), and Tobler and Pingel friction surfaces
*	Global Computation of Cost Distance:  Given all pub locations in OpenStreetMap, compute 2 hour drive  times from each location.  The full resolution is  1 ArcSecond (30 meters nominally) 
