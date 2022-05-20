# Data-Engineering-Project

## Capturing the URL data from search results (Search engines: Google, Yahoo and Bing)


### TEST HERE!!!


30 -> pages -> stage table

15 -> link table


click_time: INT


1) User search "KEYWORD"
2) Query the links form link table with keyword
3) Show so called "history" -> frequancy + keyword -> SELECT * FROM link WHERE click_time > 0 (X)
4) Using engine to search -> send to stage table
5) Query lastest result from stage table -> Tokenlize
6) Send the result to front-end. + save data to those three tables
7) User click the link -> update the click_time