
### full_count

| dataset | fgb | afgba | geojson | shp | duckdb | parquet |
|------|---|---|---|---|---||
| Flood_Hazard_Areas | 5378 ms | 3977 ms | 141259 ms | No such file or di | 330 ms | 48 ms |
| Lakes | 979 ms | 454 ms | 21241 ms | 3957 ms | 47 ms | 43 ms |
| Land | 848 ms | 356 ms | 15402 ms | 1581 ms | 43 ms | 45 ms |
| US_Census_Blocks | 8204 ms | 5889 ms | 161564 ms | No such file or di | 52 ms | 45 ms |
| buildings_geojson | 5262 ms | 3661 ms | 59151 ms | 11475 ms | 52 ms | 47 ms |
| buildings_gpkg | 3330 ms | 3002 ms | 76840 ms | No such file or di | 113 ms | 57 ms |
| footways_lines | 5517 ms | 4998 ms | 105401 ms | No such file or di | 49 ms | 45 ms |
| highways_lines | 57712 ms | 47658 ms | 800701 ms | No such file or di | 116 ms | 64 ms |
| tracks_lines | 8692 ms | 7277 ms | 156991 ms | No such file or di | 51 ms | 47 ms |
| trails_lines | 4571 ms | 3891 ms | 84652 ms | No such file or di | 48 ms | 54 ms |

### geom_agg

| dataset | fgb | afgba | geojson | shp | duckdb | parquet |
|------|---|---|---|---|---||
| Flood_Hazard_Areas | 8372 ms | 8346 ms | 140840 ms | No such file or di | 7009 ms | 5356 ms |
| Lakes | 1144 ms | 1120 ms | 21130 ms | 3494 ms | 1017 ms | 1028 ms |
| Land | 968 ms | 967 ms | 15274 ms | 1264 ms | 1488 ms | 670 ms |
| US_Census_Blocks | 11090 ms | 10982 ms | 160294 ms | No such file or di | 4297 ms | 2923 ms |
| buildings_geojson | 6483 ms | 6572 ms | 58325 ms | 12032 ms | 1223 ms | 676 ms |
| buildings_gpkg | 4894 ms | 4922 ms | 76944 ms | No such file or di | 597 ms | 510 ms |
| footways_lines | 8949 ms | 8954 ms | 102564 ms | No such file or di | 1010 ms | 788 ms |
| highways_lines | 96094 ms | 96182 ms | 839751 ms | No such file or di | 25829 ms | 13556 ms |
| tracks_lines | 15990 ms | 15933 ms | 160482 ms | No such file or di | 3893 ms | 2626 ms |
| trails_lines | 8392 ms | 8417 ms | 85507 ms | No such file or di | 1692 ms | 1320 ms |

### attr_group

| dataset | fgb | afgba | geojson | shp | duckdb | parquet |
|------|---|---|---|---|---||
| Flood_Hazard_Areas | 4041 ms | 4030 ms | 140231 ms | No such file or di | 131 ms | 129 ms |
| Lakes | 678 ms | 685 ms | 21085 ms | 3285 ms | 121 ms | 159 ms |
| Land | 433 ms | 429 ms | 15179 ms | 1217 ms | 123 ms | 133 ms |
| US_Census_Blocks | 6885 ms | 6479 ms | 166234 ms | No such file or di | 120 ms | 130 ms |
| buildings_geojson | no string column | no string column | 58271 ms | no character field | no varchar column | no varchar column |
| buildings_gpkg | 12715 ms | 12601 ms | 77086 ms | No such file or di | 427 ms | 366 ms |
| footways_lines | 23089 ms | 21177 ms | 101034 ms | No such file or di | 461 ms | 525 ms |
| highways_lines | 60403 ms | 60113 ms | 810724 ms | No such file or di | 421 ms | 518 ms |
| tracks_lines | 23532 ms | 23052 ms | 155050 ms | No such file or di | 536 ms | 545 ms |
| trails_lines | 14148 ms | 14480 ms | 84644 ms | No such file or di | 386 ms | 329 ms |

### bbox_count

| dataset | fgb | afgba | geojson | shp | duckdb | parquet |
|------|---|---|---|---|---||
| Flood_Hazard_Areas | 7 ms | 0 ms | 163883 ms | No such file or di | 5132 ms | 5595 ms |
| Lakes | 4 ms | 2 ms | 23016 ms | 3447 ms | 583 ms | 724 ms |
| Land | 3 ms | 1 ms | 16465 ms | 1352 ms | 421 ms | 503 ms |
| US_Census_Blocks | 1 ms | 0 ms | 172913 ms | No such file or di | 3013 ms | 2651 ms |
| buildings_geojson | 148 ms | 122 ms | 62345 ms | 11989 ms | 2104 ms | 2152 ms |
| buildings_gpkg | 32 ms | 27 ms | 81652 ms | No such file or di | 1672 ms | 1533 ms |
| footways_lines | 21 ms | 10 ms | 104008 ms | No such file or di | 1736 ms | 1704 ms |
| highways_lines | 610 ms | 507 ms | 867581 ms | No such file or di | 26454 ms | 16042 ms |
| tracks_lines | 88 ms | 88 ms | 168586 ms | No such file or di | 3312 ms | 2162 ms |
| trails_lines | 107 ms | 99 ms | 88678 ms | No such file or di | 1703 ms | 1207 ms |

### viewport_decode

| dataset | fgb | afgba | geojson | shp | duckdb | parquet |
|------|---|---|---|---|---||
| Flood_Hazard_Areas | 0 ms | 0 ms | 160792 ms | No such file or di | 5121 ms | 5590 ms |
| Lakes | 0 ms | 0 ms | 22652 ms | 3433 ms | 586 ms | 711 ms |
| Land | 0 ms | 0 ms | 16445 ms | 1344 ms | 402 ms | 527 ms |
| US_Census_Blocks | 0 ms | 0 ms | 171805 ms | No such file or di | 2972 ms | 2431 ms |
| buildings_geojson | 6 ms | 8 ms | 63133 ms | 12020 ms | 2100 ms | 2084 ms |
| buildings_gpkg | 0 ms | 0 ms | 79181 ms | No such file or di | 1417 ms | 1378 ms |
| footways_lines | 0 ms | 0 ms | 106159 ms | No such file or di | 1806 ms | 1677 ms |
| highways_lines | 0 ms | 0 ms | 850895 ms | No such file or di | 24770 ms | 13887 ms |
| tracks_lines | 0 ms | 0 ms | 168776 ms | No such file or di | 3205 ms | 2234 ms |
| trails_lines | 0 ms | 0 ms | 88888 ms | No such file or di | 1579 ms | 1191 ms |

### zoom_agg

| dataset | fgb | afgba | geojson | shp | duckdb | parquet |
|------|---|---|---|---|---||
| Flood_Hazard_Areas | — | 3 ms | — | — | — | — |
| Lakes | — | 1 ms | — | — | — | — |
| Land | — | 1 ms | — | — | — | — |
| US_Census_Blocks | — | 2 ms | — | — | — | — |
| buildings_geojson | — | 1 ms | — | — | — | — |
| buildings_gpkg | — | 3 ms | — | — | — | — |
| footways_lines | — | 1 ms | — | — | — | — |
| highways_lines | — | 0 ms | — | — | — | — |
| tracks_lines | — | 1 ms | — | — | — | — |
| trails_lines | — | 1 ms | — | — | — | — |

### size (MB)

| dataset | fgb | afgba | geojson | shp | duckdb | parquet |
|------|---|---|---|---|---||
| Flood_Hazard_Areas | 9599 | 9600 | 26782 | 191 | 9641 | 6988 |
| Lakes | 1512 | 1518 | 3578 | 2109 | 1232 | 881 |
| Land | 1365 | 1366 | 2363 | 1507 | 1347 | 894 |
| US_Census_Blocks | 11598 | 11608 | 28659 | 2001 | 12941 | 7654 |
| buildings_geojson | 5226 | 5228 | 9918 | 5921 | 2998 | 1957 |
| buildings_gpkg | 5887 | 5888 | 12499 | 4 | 2248 | 1009 |
| footways_lines | 7057 | 7074 | 15730 | 41607 | 5068 | 2412 |
| highways_lines | 77961 | — | 122011 | 5590 | 87746 | 38214 |
| tracks_lines | 12418 | 12510 | 23235 | 12069 | 16193 | 7470 |
| trails_lines | 6245 | 6280 | 12570 | 15295 | 8085 | 3604 |
