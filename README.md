# **Data Engineer Camp Project 1**
# **Travel Time API ELT Project**
## Production code version

_Josh B_ - _Shruti S_ - _Alex H_

## Objective:
The use case for this project is to aggregate and serve transit times from multiple source locations to a single target destination in a major transit hub. We’ve identified Penn Station in NY as our target destination and will provide commuter transit times (driving) from 3 source locations. 

**Note: This repo is meant to be run in production using AWS. To clone code to run on your local machine, see the travel_time_local repo.

## Consumers:
The final data set of this project is limited to 4 locations and could be expanded to allow for many more source and destination locations. There is a wide variety of potential target consumers for the final data set if this project were expanded to allow more conifiguration of the API request. These coulde include commuters and tourists, transit planners and researchers, small businesses, and individuals looking to make use of transit data.


## Questions We Want To Answer:
1) Which city has the longest average travel time to New York City during rush hour?
2) How does travel time vary by day part (morning, afternoon, evening, night)?
3) What is the difference in travel times between rush hour and non-rush hour?
4) Are there trends in travel times across weekdays vs. weekends?
5) How does travel time from Stamford compare to Hackensack and Hoboken at different times of the day?
6) Does the rush hour travel time in the evening differ from the morning rush hour?
7) Are travel times improving or worsening over time?
8) How frequently are commuters from each city impacted by rush hour congestion?
9) Is there a relationship between weather conditions (if integrated with external data) and travel times?
10) What is the best time of day to avoid traffic when traveling to New York City?


| `Source Name`  | `Source Type` | `Source Docs`                               | `Endpoint` |
| -------------  | ------------- | ------------                                | -----------|
|  traveltime    | rest api      | https://docs.traveltime.com/api/sdks/python | https://docs.traveltime.com/api/reference/travel-time-distance-matrix|


## Architecture:
For commuter travel times we chose the TravelTime API (REST/HTTP requests). The endpoint we used was the travel time filter that provided travel time, distance, preferred route for any combination of source and destination locations. 

API Request:
- Penn Station as destination (coordinates hardcoded in request)
- 3 starting destinations: Hackensack, Stamford, Hoboken (coordinates hardcoded in request)
- Transit method: driving (f string passed to request)
- Each request is made for the current time (f string with current date/time passed to request)


![travel_time_prod drawio](https://github.com/user-attachments/assets/4537af3a-a083-4d85-b3c4-43a562051475)

