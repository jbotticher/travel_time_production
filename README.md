# **Project Team 6 DEC**
_Josh B_ - _Shruti S_ - _Alex H_

## Objective:
The use case for our project is to aggregate and serve transit times from multiple source locations to a single target destination in a major transit hub. We’ve identified Penn Station in NY as our target destination and will provide commuter transit times from x source locations across different modes of transportation (bus, train).

## Consumers:
The target consumers are daily commuters who are looking to identify the most efficient path to commute from their preferred source location to NY Penn Station. In reality they would access this data via app from either the android or apple os. 

## Questions We Want To Answer:
1) Given your preferred source location, what is the average transit time to Penn Station for typical commuting hours (6-9) historically.
2) Given your preferred source location, what is the average transit time to Penn Station for typical commuting hours (6-9) today.
3) What source locations (of the ones provided) and modes are the most efficient in travel time to Penn Station.

| `Source Name`  | `Source Type` | `Source Docs`                               | `Endpoint` |
| -------------  | ------------- | ------------                                | -----------|
|  traveltime    | rest api      | https://docs.traveltime.com/api/sdks/python | https://docs.traveltime.com/api/reference/travel-time-distance-matrix|


## Architecture:
![image](https://github.com/alheston/dec-project-1/assets/167915392/aefa75a0-9ef4-496f-a5c9-04b9838c93e5)



