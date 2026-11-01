#!/bin/sh
# Downloads the OSM extract once, builds the routing graph once, then serves.
set -e
mkdir -p /data
if [ ! -f /data/region.osm.pbf ]; then
  if [ -z "$OSM_PBF_URL" ]; then
    echo "Set OSM_PBF_URL to an OpenStreetMap .osm.pbf extract (see README)" >&2
    exit 1
  fi
  echo "downloading $OSM_PBF_URL"
  curl -fL --retry 3 -o /data/region.osm.pbf.part "$OSM_PBF_URL"
  mv /data/region.osm.pbf.part /data/region.osm.pbf
fi
exec java ${JAVA_OPTS:--Xmx4g} \
  -Ddw.graphhopper.datareader.file=/data/region.osm.pbf \
  -Ddw.graphhopper.graph.location=/data/graph-cache \
  -jar /gh/graphhopper.jar server /gh/config.yml
