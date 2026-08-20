# factcheck-data-staging

Repository pubblico temporaneo, usato solo per far girare a costo zero (le
Actions sono gratuite/illimitate sui repo pubblici) le pipeline dati più
pesanti di [FactCheck](https://github.com/michelemarongiu79/factcheck):

- `scripts/build_carburanti_regioni.py` — storico prezzi carburanti per
  regione (dal 2015), scaricato dall'archivio MIMIT: ~46 trimestri da
  elaborare, riprende da `data/carburanti_regioni_progress.json` a ogni run.
- `scripts/build_temperature.py` — temperature e precipitazioni per regione
  (dal 1940), da Open-Meteo: lento per i rate-limit dell'API.

Una volta che i JSON generati in `site/data/` sono completi e verificati,
vengono copiati a mano nel repo privato `factcheck` (quello vero, che
alimenta il sito). Questo repo può essere archiviato o cancellato quando
non serve più.
