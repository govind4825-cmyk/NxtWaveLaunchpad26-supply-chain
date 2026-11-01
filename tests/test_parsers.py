"""Parser tests.

The XML/JSON strings below are format-conformance samples written to the public
CAP 1.2 and RSS specs. They exist only to test parsing and are never loaded by the app.
"""

from datetime import timezone

import pytest
from shapely.geometry import box

from rasad.connectors.cap import CapParseError, parse_cap, parse_circle, parse_polygon
from rasad.connectors.gdacs import footprint, relevant
from rasad.connectors.overpass import bridges_query, clamp_bbox, parse_crossings
from rasad.connectors.rss import parse_feed
from rasad.engine.classify import classify

CAP_POLYGON = b"""<?xml version="1.0" encoding="UTF-8"?>
<alert xmlns="urn:oasis:names:tc:emergency:cap:1.2">
  <identifier>TEST-0001</identifier>
  <sender>test@example.org</sender>
  <sent>2026-11-01T10:00:00+05:30</sent>
  <status>Actual</status><msgType>Alert</msgType><scope>Public</scope>
  <info>
    <language>hi</language><category>Met</category><event>Heavy rain</event>
    <urgency>Expected</urgency><severity>Moderate</severity><certainty>Likely</certainty>
  </info>
  <info>
    <language>en-IN</language><category>Met</category><event>Very Heavy Rainfall</event>
    <urgency>Expected</urgency><severity>Severe</severity><certainty>Likely</certainty>
    <onset>2026-11-01T12:00:00+05:30</onset><expires>2026-11-02T08:30:00+05:30</expires>
    <senderName>Test Met Office</senderName>
    <headline>Very heavy rainfall likely</headline>
    <description>Heavy to very heavy rainfall at isolated places.</description>
    <area>
      <areaDesc>Alpha, Beta</areaDesc>
      <polygon>22.0,88.0 22.0,88.5 22.5,88.5 22.5,88.0 22.0,88.0</polygon>
      <geocode><valueName>district</valueName><value>Alpha</value></geocode>
    </area>
  </info>
</alert>"""

CAP_UPDATE_NO_GEOM = b"""<alert xmlns="urn:oasis:names:tc:emergency:cap:1.2">
  <identifier>TEST-0002</identifier><sender>s</sender><sent>2026-11-01T14:00:00Z</sent>
  <status>Actual</status><msgType>Update</msgType><scope>Public</scope>
  <references>s,TEST-0001,2026-11-01T10:00:00+05:30</references>
  <info><event>Cyclonic storm</event><severity>Extreme</severity><certainty>Observed</certainty>
    <area><areaDesc>Gamma district</areaDesc></area></info>
</alert>"""


def test_cap_polygon_prefers_english_info():
    r = parse_cap(CAP_POLYGON, source="sachet", source_url="u", raw_fetch_id=7)
    assert r.source_ref == "TEST-0001"
    assert r.kind == "heavy_rain"
    assert r.severity == 3 and r.certainty == pytest.approx(0.7)
    assert r.located_by == "polygon"
    minx, miny, maxx, maxy = r.geom.bounds
    assert (minx, miny, maxx, maxy) == pytest.approx((88.0, 22.0, 88.5, 22.5))  # lon/lat swapped correctly
    assert r.onset.utcoffset().total_seconds() == 5.5 * 3600
    assert r.attrs["geocodes"] == [{"name": "district", "value": "Alpha"}]
    assert r.raw_fetch_id == 7


def test_cap_update_references_and_unlocated():
    r = parse_cap(CAP_UPDATE_NO_GEOM, source="sachet")
    assert r.msg_type == "Update"
    assert r.references == ["TEST-0001"]
    assert r.geom is None and r.located_by == "none"
    assert r.kind == "cyclone" and r.severity == 4 and r.certainty == 1.0
    assert r.sent.tzinfo == timezone.utc


def test_cap_rejects_non_alert():
    with pytest.raises(CapParseError):
        parse_cap(b"<html><body>error</body></html>", source="sachet")
    with pytest.raises(CapParseError):
        parse_cap(b"not xml at all", source="sachet")


def test_polygon_auto_close_and_invalid():
    p = parse_polygon("10,70 10,71 11,71")
    assert p is not None and p.is_valid
    assert parse_polygon("10,70 10,71") is None
    bow = parse_polygon("0,0 1,1 0,1 1,0 0,0")  # self-intersecting bow tie
    assert bow is not None and bow.is_valid


def test_circle_is_round_on_ground():
    c = parse_circle("60.0,10.0 111.32")
    minx, miny, maxx, maxy = c.bounds
    assert (maxy - miny) == pytest.approx(2.0, rel=0.01)
    assert (maxx - minx) == pytest.approx(4.0, rel=0.02)  # 1/cos(60deg) stretch
    assert parse_circle("bad") is None


def test_rss_feed_items():
    xml = b"""<rss version="2.0"><channel><title>t</title>
      <item><title>A</title><link>https://x/cap/1.xml</link><guid>g1</guid>
        <pubDate>Sun, 01 Nov 2026 10:00:00 +0530</pubDate><author>imd@x</author></item>
      <item><title>B</title><link>https://x/cap/2.xml</link></item>
    </channel></rss>"""
    items = parse_feed(xml)
    assert [i.link for i in items] == ["https://x/cap/1.xml", "https://x/cap/2.xml"]
    assert items[0].key == "g1" and items[1].key == "https://x/cap/2.xml"
    assert items[0].published.utcoffset().total_seconds() == 5.5 * 3600


def test_classify_order():
    assert classify("Severe Cyclonic Storm", None) == "cyclone"
    assert classify("Thunderstorm with lightning") == "thunderstorm"
    assert classify("Flood situation", "river level rising") == "flood"
    assert classify("Heat Wave") == "heatwave"
    assert classify("Something new") == "other"


def test_gdacs_relevance_and_footprint():
    india = (66.0, 6.0, 98.5, 37.5)
    assert relevant({"properties": {"iso3": "IND"}}, india)
    assert relevant({"properties": {"affectedcountries": [{"iso3": "IND"}]}}, india)
    assert relevant({"properties": {}, "bbox": [85, 15, 90, 20]}, india)
    assert not relevant({"properties": {}, "bbox": [-80, 20, -70, 30]}, india)
    fc = {
        "features": [
            {"geometry": box(85, 15, 87, 17).__geo_interface__, "properties": {"Class": "Poly_Orange"}},
            {"geometry": box(86, 16, 88, 18).__geo_interface__, "properties": {"Class": "Poly_Cones"}},
            {"geometry": {"type": "LineString", "coordinates": [[85, 15], [88, 18]]}, "properties": {}},
        ]
    }
    geom, classes = footprint(fc)
    assert geom.geom_type == "MultiPolygon"
    assert geom.area == pytest.approx(7.0)
    assert classes == ["Poly_Cones", "Poly_Orange"]


def test_overpass_bbox_and_dedupe():
    bb, sampled = clamp_bbox((80, 15, 84, 19), (82, 17))
    assert sampled and bb == (81.5, 16.5, 82.5, 17.5)
    q = bridges_query(*bb)
    assert "16.5000,81.5000,17.5000,82.5000" in q
    data = {"elements": [
        {"type": "way", "id": 1, "center": {"lat": 17.0, "lon": 82.0}, "tags": {"highway": "secondary", "name": "S"}},
        {"type": "way", "id": 2, "center": {"lat": 17.001, "lon": 82.001}, "tags": {"highway": "trunk", "name": "T"}},
        {"type": "way", "id": 3, "center": {"lat": 17.2, "lon": 82.2}, "tags": {"highway": "tertiary"}},
    ]}
    picked = parse_crossings(data, keep=5)
    assert [c.osm_ref for c in picked] == ["way/2", "way/3"]  # trunk wins its 1 km cell
