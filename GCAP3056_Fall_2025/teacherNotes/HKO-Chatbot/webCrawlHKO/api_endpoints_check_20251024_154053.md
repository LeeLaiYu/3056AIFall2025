# API Endpoints Check Report

**Generated on:** 2025-10-24 15:40:53

## Summary

- **Successful endpoints:** 6/7
- **Failed endpoints:** 1/7

## Detailed Results

### datasets_api
- **Status:** error
- **HTTP Code:** 401
- **Content-Type:** N/A
- **Size:** N/A bytes
- **Error:** UNAUTHORIZED

### providers_json
- **Status:** success
- **HTTP Code:** 200
- **Content-Type:** application/json
- **Size:** 31174 bytes
- **Data Type:** JSON object
- **Keys:** ['providers']

### categories_json
- **Status:** success
- **HTTP Code:** 200
- **Content-Type:** application/json
- **Size:** 4253 bytes
- **Data Type:** JSON object
- **Keys:** ['categories']

### formats_json
- **Status:** success
- **HTTP Code:** 200
- **Content-Type:** application/json
- **Size:** 634 bytes
- **Data Type:** JSON object
- **Keys:** ['formats']

### rss_feed
- **Status:** success
- **HTTP Code:** 200
- **Content-Type:** text/xml; charset=utf-8
- **Size:** 101390 bytes
- **Data Type:** Text
- **Preview:** <?xml version="1.0" encoding="utf-8"?>
<rss version="2.0"><channel><title>Information on Data Files</title><link>https://data.gov.hk</link><description>Information on data files at DATA.GOV.HK that ar...

### climate_weather_category
- **Status:** success
- **HTTP Code:** 200
- **Content-Type:** text/html; charset=utf-8
- **Size:** 41752 bytes
- **Data Type:** Text
- **Preview:** <!DOCTYPE html> <!--[if lte IE 9]><html class="no-js lte-ie9 lang-en" lang="en"><![endif]--> <!--[if gt IE 9]><!--> <html class="no-js lang-en" lang="en"> <!--<![endif]--> <head> <script src="/assets/...

### hko_provider
- **Status:** success
- **HTTP Code:** 200
- **Content-Type:** text/html; charset=utf-8
- **Size:** 41752 bytes
- **Data Type:** Text
- **Preview:** <!DOCTYPE html> <!--[if lte IE 9]><html class="no-js lte-ie9 lang-en" lang="en"><![endif]--> <!--[if gt IE 9]><!--> <html class="no-js lang-en" lang="en"> <!--<![endif]--> <head> <script src="/assets/...

## HKO Dataset Analysis

### providers_json - HKO Data Found!
```json
{
  "providers": [
    {
      "datasetCount": 12,
      "id": "hk-aw",
      "name": {
        "en": "Administration Wing, Chief Secretary for Administration's Office",
        "sc": "\u653f\u52a1\u53f8\u53f8\u957f\u529e\u516c\u5ba4\u8f96\u4e0b\u884c\u653f\u7f72",
        "tc": "\u653f\u52d9\u53f8\u53f8\u9577\u8fa6\u516c\u5ba4\u8f44\u4e0b\u884c\u653f\u7f72"
      }
    },
    {
      "datasetCount": 52,
      "id": "hk-afcd",
      "name": {
        "en": "Agriculture, Fisheries and Conservation Department",
        "sc": "\u6e14\u519c\u81ea\u7136\u62a4\u7406\u7f72",
        "tc": "\u6f01\u8fb2\u81ea\u7136\u8b77\u7406\u7f72"
      }
    },
    {
      "datasetCount": 1,
      "id": "aahk",
      "name": {
        "en": "Airport Authority Hong Kong",
        "sc": "\u9999\u6e2f\u673a\u573a\u7ba1\u7406\u5c40",
        "tc": "\u9999\u6e2f\u6a5f\u5834\u7ba1\u7406\u5c40"
      }
    },
    {
      "datasetCount": 25,
      "id": "hk-archsd",
      "name": {
        "en": "Architectural Ser...
```

