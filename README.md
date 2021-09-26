# railway-stations-in-japan

## How to Get TSV

```
$ curl -LO https://dumps.wikimedia.org/enwiki/20210920/enwiki-20210920-pages-articles-multistream.xml.bz2
$ curl -LO https://dumps.wikimedia.org/enwiki/20210920/enwiki-20210920-pages-articles-multistream-index.txt.bz2
$ go run . > railway-stations-in-japan.tsv
```

## References

* https://en.wikipedia.org/wiki/List_of_railway_stations_in_Japan
* https://dumps.wikimedia.org/enwiki/
