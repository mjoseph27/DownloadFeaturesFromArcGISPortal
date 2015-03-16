# DownloadFeaturesFromArcGISPortal

A simple geoprocessing tool to download features as geodatabase feature class or shape file from external feature services.

In this tool, the python script sends multiple query requests in threads based on ObjectIds of the feature service and downloads all the features irrespective of Maximum Record Count specified by the service.

Download the files and open ExternalFS geoprocessing toolbox in ArcMap.
Open GetFeatures toolbox

Specify the required parameters:

1. Input Feature Service Url: Provide an external feature service or map service url
*currently supports only public urls and cannot access secure urls

2. Output Features: Browse to a gdb feature class 

Specify the optional parameters if needed:

1. Query JSON: Specify query parameters as JSON String if you wish to download a specific set of features from feature services.

2. Maximum number of features per request:  Specify the maximum number of records to fetch in each request. 
Recommendations:
500: for point features
200: for polyline and polygon features
10 - 30 : for non-generalized boundary or coastal area polygons

