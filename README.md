# Paris Transit Reliability

How reliable is public transport in the Paris region, line by line, stop by stop, hour by hour?

This project collects real-time passage data from Île-de-France Mobilités, refines it through a bronze / silver / gold lakehouse, and uses it for reliability analytics and delay prediction.

## Status

- Level 1 (bronze ingestion) in progress. Nothing is deployed yet.

## How it works (planned)

1. **Bronze:** poll the real-time APIs within their quotas and store every raw response, unchanged, with metadata
2. **Silver:** parse, deduplicate and validate passages, and join them to the timetable that was valid at that moment
3. **Gold:** delay and reliability tables per line, stop and hour, served through an API and a dashboard
4. **Prediction:** a delay model that has to beat a simple "same hour last week" baseline


## Data sources

| Source | Format | Use |
| --- | --- | --- |
| [PRIM](https://prim.iledefrance-mobilites.fr/) next passages (global and per stop) | SIRI Lite JSON | Real-time expected and recorded times |
| [IDFM GTFS](https://transport.data.gouv.fr/datasets/reseau-urbain-et-interurbain-dile-de-france-mobilites) | GTFS | Planned timetables |


## Setup

Real-time data needs a free PRIM API key.

    cp .env.example .env
    # then put your key in .env: PRIM_API_KEY=...

## Licences

- **Code:** MIT, see [LICENSE](LICENSE)
- **Data:** © Île-de-France Mobilités, under the [Open Database License (ODbL)](https://opendatacommons.org/licenses/odbl/). Data derived from these sources is shared under the same licence.