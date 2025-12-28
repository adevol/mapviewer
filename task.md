# MLE Challenge
## Context
You are analysing residential property prices in France, you have at your disposal the public recent transactions on (https://www.data.gouv.fr/datasets/demandes-de-valeurs-foncieres/) to estimate the current market price as price per squared meter (€/m²)

## Objective
You need to generate an interactive map visualization for the aggregated price data by : 
- Country
- Region
- Departament
- Neighborhood
- Postcode
- Building plots

You need to procure yourself with open data from the french government for such geometries and take care of cleaning the data to make accurate aggregates. 

You are asked to make the best estimation of the market price taking into account, transaction price volatility, transaction volume, data freshness and consistency. You can estimate a number for the price or an interval.

You can use this one as a reference https://explore.data.gouv.fr/fr/immobilier

## Deliverable 

You need to render an interactive map that shows the price aggregate with colors, as the level zooms, it needs to transition between aggregation levels. (Link to hosted app, optional but preferred)

If the volume of data is too important for the browser to support all you can subset it, but a solution for this will be appreciated. 

Produce a list of market price per square meter by property type for the top 10 biggest cities.
Submit your processing code. (Link to github repo) 

What you will be evaluated on 
- Is the colored map loading ?
- Is the map usable and not laggy ?
- Is the map refreshing the aggregation level on zoom ?
- Are all 6 aggregation levels present ?
- Are the price estimates plausible ?
- Is the data complete or was it subset ?
- The processing code is clean, clear and reusable
- The architecture is robust and logical
- App is hosted and functional
